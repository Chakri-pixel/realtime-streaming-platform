#!/usr/bin/env python3
"""
Spark Structured Streaming application: realtime clickstream analytics.

Reads clickstream events from Redpanda (Kafka protocol) and runs six
streaming queries:

  1. raw-lake      append raw events to Parquet, partitioned by (dt, hr)
  2. revenue       revenue + purchases per 1-minute event-time window
  3. top-products  per-product interactions per 5-minute window
  4. funnel        view -> cart -> purchase funnel per 5-minute window
  5. activity      throughput + active sessions per 1-minute window
  6. latency       end-to-end latency estimate per micro-batch

Windowed queries run in append mode with a 5-minute watermark, so each
window's result is emitted exactly once, when the watermark passes it.
Metric results are written with foreachBatch to JSON files under
SERVING_DIR, which the Streamlit dashboard polls.

Configuration via environment:
    KAFKA_BOOTSTRAP  (default localhost:9092)
    TOPIC            (default clickstream-events)
    SERVING_DIR      dashboard JSON output (default data/serving)
    LAKE_DIR         Parquet lake path (default data/lake)
    CHECKPOINT_DIR   streaming checkpoints (default data/checkpoints)
"""
import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transformations import (
    EVENT_SCHEMA,
    flatten_window,
    funnel_by_window,
    revenue_per_minute,
    session_activity,
    top_products,
    with_event_time,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("streaming-job")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "clickstream-events")
SERVING_DIR = os.environ.get("SERVING_DIR", "data/serving")
LAKE_DIR = os.environ.get("LAKE_DIR", "data/lake")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "data/checkpoints")
HISTORY_LIMIT = 500  # keep the last N points per metric for dashboard charts


def write_serving(rows: list, name: str, key_cols: tuple) -> None:
    """Merge rows into the metric's history file, keyed for idempotency.

    History is keyed by ``key_cols`` (e.g. window_start), so reprocessing a
    batch overwrites rather than duplicates points. The full history is
    rewritten each micro-batch; at 30s batches x 500 points this is trivially
    cheap and keeps the dashboard reader dead simple.
    """
    os.makedirs(SERVING_DIR, exist_ok=True)
    path = os.path.join(SERVING_DIR, f"{name}_history.jsonl")

    merged = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    merged[tuple(row[k] for k in key_cols)] = row
                except (json.JSONDecodeError, KeyError):
                    continue
    for row in rows:
        merged[tuple(row[k] for k in key_cols)] = row

    items = list(merged.values())[-HISTORY_LIMIT:]
    with open(path, "w") as f:
        for row in items:
            f.write(json.dumps(row, default=str) + "\n")
    with open(os.path.join(SERVING_DIR, f"{name}_latest.json"), "w") as f:
        json.dump(rows, f, default=str)


def main() -> None:
    spark = (
        SparkSession.builder.appName("realtime-clickstream")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark %s | bootstrap=%s topic=%s", spark.version, BOOTSTRAP, TOPIC)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    events = with_event_time(
        raw.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            F.col("timestamp").alias("kafka_ts"),
        ).select("e.*", "kafka_ts")
    )

    queries = []

    # 1. Raw lake sink: immutable Parquet, partitioned by event date/hour.
    queries.append(
        events.withColumn("dt", F.to_date("event_time"))
        .withColumn("hr", F.hour("event_time"))
        .writeStream.format("parquet")
        .partitionBy("dt", "hr")
        .option("path", LAKE_DIR)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw")
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .queryName("raw-lake")
        .start()
    )

    # 2-5. Windowed metric queries: append mode + watermark => each window
    # emitted exactly once, then merged into the serving JSON idempotently.
    metric_queries = [
        (revenue_per_minute, "revenue", ("window_start",), "30 seconds"),
        (top_products, "top_products", ("window_start", "product_id"), "60 seconds"),
        (funnel_by_window, "funnel", ("window_start",), "60 seconds"),
        (session_activity, "activity", ("window_start",), "30 seconds"),
    ]
    for agg_fn, name, key_cols, trigger in metric_queries:
        agg = agg_fn(events)

        def _process(batch_df, batch_id, _name=name, _keys=key_cols):
            rows = [
                r.asDict(recursive=True)
                for r in flatten_window(batch_df).collect()
            ]
            if rows:
                write_serving(rows, _name, _keys)
                log.info("query=%s batch=%d wrote %d window(s)", _name, batch_id, len(rows))

        queries.append(
            agg.writeStream.foreachBatch(_process)
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
            .trigger(processingTime=trigger)
            .queryName(name)
            .start()
        )

    # 6. End-to-end latency estimate: processing time minus event time,
    # averaged over each micro-batch.
    def _latency(batch_df, batch_id):
        row = batch_df.agg(
            F.count("*").alias("events"),
            F.avg(F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("event_time"))
            .alias("avg_latency_sec"),
            F.max(F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("event_time"))
            .alias("max_latency_sec"),
        ).collect()[0]
        if row["events"]:
            write_serving(
                [{
                    "batch_id": batch_id,
                    "measured_at": datetime.now(timezone.utc).isoformat(),
                    "events": row["events"],
                    "avg_latency_sec": row["avg_latency_sec"],
                    "max_latency_sec": row["max_latency_sec"],
                }],
                "latency",
                ("batch_id",),
            )

    queries.append(
        events.writeStream.foreachBatch(_latency)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/latency")
        .trigger(processingTime="30 seconds")
        .queryName("latency")
        .start()
    )

    log.info("Started %d streaming queries", len(queries))
    for q in queries:
        log.info("  - %s (id=%s)", q.name, q.id)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
