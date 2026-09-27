"""
Shared streaming transformations for the realtime clickstream platform.

Every metric function is pure -- DataFrame in, DataFrame out -- operating on
**event time**. They run inside Spark Structured Streaming queries (with a
watermark applied by ``with_event_time``) and are unit-testable on static
DataFrames; see ``tests/test_transformations.py``.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# How long we wait for out-of-order events before a window is considered
# complete. Bounds the aggregation state Spark must keep.
WATERMARK_DELAY = "5 minutes"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("page_url", StringType(), False),
    StructField("device", StringType(), False),
    StructField("event_time", TimestampType(), False),
])


def with_event_time(df: DataFrame) -> DataFrame:
    """Cast to timestamp and attach the watermark used by all windowed aggs."""
    return (
        df.withColumn("event_time", F.col("event_time").cast(TimestampType()))
          .withWatermark("event_time", WATERMARK_DELAY)
    )


def revenue_per_minute(events: DataFrame) -> DataFrame:
    """Revenue and purchase count per 1-minute event-time window."""
    return (
        events.filter(F.col("event_type") == "purchase")
              .groupBy(F.window(F.col("event_time"), "1 minute").alias("window"))
              .agg(
                  F.sum("price").alias("revenue"),
                  F.count("*").alias("purchases"),
              )
    )


def top_products(events: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """Cart/purchase interactions and GMV per product per event-time window."""
    return (
        events.filter(F.col("event_type").isin("add_to_cart", "purchase"))
              .groupBy(
                  F.window(F.col("event_time"), window_duration).alias("window"),
                  F.col("product_id"),
                  F.col("product_name"),
              )
              .agg(
                  F.count("*").alias("interactions"),
                  F.sum("price").alias("gross_merchandise_value"),
              )
    )


def funnel_by_window(events: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """View -> cart -> purchase funnel per event-time window.

    First collapses to one row per session per window (did the session reach
    each stage?), then counts sessions. A session that views twice still
    counts once per stage -- this is a session funnel, not an event funnel.
    """
    per_session = (
        events.groupBy(
            F.window(F.col("event_time"), window_duration).alias("window"),
            F.col("session_id"),
        )
        .agg(
            F.max(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("viewed"),
            F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carted"),
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchased"),
        )
    )
    return (
        per_session.groupBy("window")
                   .agg(
                       F.count("*").alias("sessions"),
                       F.sum("viewed").alias("viewed"),
                       F.sum("carted").alias("carted"),
                       F.sum("purchased").alias("purchased"),
                   )
    )


def session_activity(events: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """Throughput and active sessions/users per event-time window."""
    return (
        events.groupBy(F.window(F.col("event_time"), window_duration).alias("window"))
              .agg(
                  F.count("*").alias("events"),
                  F.approx_count_distinct("session_id").alias("active_sessions"),
                  F.approx_count_distinct("user_id").alias("active_users"),
              )
    )


def flatten_window(df: DataFrame) -> DataFrame:
    """Explode the ``window`` struct into ``window_start``/``window_end`` strings.

    The dashboard consumes plain JSON, so window structs are flattened before
    the serving sink writes them out.
    """
    cols = [c for c in df.columns if c != "window"]
    return df.select(
        F.col("window.start").cast("string").alias("window_start"),
        F.col("window.end").cast("string").alias("window_end"),
        *[F.col(c) for c in cols],
    )
