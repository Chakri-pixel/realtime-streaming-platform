"""Unit tests for the streaming transformation logic.

Transformations are pure (DataFrame in -> DataFrame out), so they run on
small static DataFrames with fixed event times. No Kafka or streaming
cluster needed.
"""
import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streaming"))

from transformations import (  # noqa: E402
    EVENT_SCHEMA,
    flatten_window,
    funnel_by_window,
    revenue_per_minute,
    session_activity,
    top_products,
    with_event_time,
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("transformations-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def ev(event_id, user, session, etype, product=None, price=None, ts=None):
    pid, pname, cat = product if product else (None, None, None)
    return {
        "event_id": event_id,
        "user_id": user,
        "session_id": session,
        "event_type": etype,
        "product_id": pid,
        "product_name": pname,
        "category": cat,
        "price": price,
        "page_url": "/",
        "device": "desktop",
        "event_time": ts,
    }


def P(pid, name, price):
    return (pid, name, "Electronics")


def T(hour, minute, second=0):
    return datetime(2026, 1, 1, hour, minute, second)


def make_events(spark, rows):
    return with_event_time(spark.createDataFrame(rows, schema=EVENT_SCHEMA))


def test_revenue_per_minute(spark):
    rows = [
        ev("e1", "u1", "s1", "purchase", P("p1", "Widget", 100.0), 100.0, T(12, 0, 10)),
        ev("e2", "u2", "s2", "purchase", P("p2", "Gadget", 50.0), 50.0, T(12, 0, 50)),
        ev("e3", "u3", "s3", "purchase", P("p1", "Widget", 25.0), 25.0, T(12, 1, 5)),
        ev("e4", "u4", "s4", "page_view", None, None, T(12, 0, 20)),  # must not count
    ]
    got = {
        (r["window"]["start"].strftime("%H:%M"), r["revenue"], r["purchases"])
        for r in revenue_per_minute(make_events(spark, rows)).collect()
    }
    assert got == {("12:00", 150.0, 2), ("12:01", 25.0, 1)}


def test_top_products(spark):
    rows = [
        ev("e1", "u1", "s1", "add_to_cart", P("p1", "Widget", 100.0), 100.0, T(12, 0, 10)),
        ev("e2", "u1", "s1", "add_to_cart", P("p1", "Widget", 100.0), 100.0, T(12, 1, 10)),
        ev("e3", "u2", "s2", "purchase", P("p1", "Widget", 100.0), 100.0, T(12, 2, 10)),
        ev("e4", "u3", "s3", "add_to_cart", P("p2", "Gadget", 50.0), 50.0, T(12, 0, 30)),
        ev("e5", "u4", "s4", "page_view", None, None, T(12, 0, 40)),  # must not count
    ]
    got = {
        r["product_id"]: (r["interactions"], r["gross_merchandise_value"])
        for r in top_products(make_events(spark, rows)).collect()
    }
    # all events fall in the single 5-minute window [12:00, 12:05)
    assert got == {"p1": (3, 300.0), "p2": (1, 50.0)}


def test_funnel_by_window(spark):
    rows = [
        # s1: full funnel view -> cart -> purchase
        ev("e1", "u1", "s1", "page_view", None, None, T(12, 0, 5)),
        ev("e2", "u1", "s1", "add_to_cart", P("p1", "Widget", 10.0), 10.0, T(12, 0, 30)),
        ev("e3", "u1", "s1", "purchase", P("p1", "Widget", 10.0), 10.0, T(12, 1, 10)),
        # s2: view only
        ev("e4", "u2", "s2", "page_view", None, None, T(12, 0, 15)),
        # s3: view -> cart (no purchase)
        ev("e5", "u3", "s3", "page_view", None, None, T(12, 2, 5)),
        ev("e6", "u3", "s3", "add_to_cart", P("p2", "Gadget", 20.0), 20.0, T(12, 2, 40)),
        # s1 views again: still one session in the funnel
        ev("e7", "u1", "s1", "page_view", None, None, T(12, 3, 0)),
    ]
    row = funnel_by_window(make_events(spark, rows)).collect()[0]
    assert row["sessions"] == 3
    assert row["viewed"] == 3
    assert row["carted"] == 2
    assert row["purchased"] == 1


def test_session_activity(spark):
    rows = [
        ev("e1", "u1", "s1", "page_view", None, None, T(12, 0, 5)),
        ev("e2", "u1", "s1", "page_view", None, None, T(12, 0, 15)),
        ev("e3", "u1", "s1", "add_to_cart", P("p1", "Widget", 10.0), 10.0, T(12, 0, 25)),
        ev("e4", "u2", "s2", "page_view", None, None, T(12, 0, 35)),
        ev("e5", "u2", "s2", "page_view", None, None, T(12, 0, 45)),
    ]
    row = session_activity(make_events(spark, rows)).collect()[0]
    assert row["events"] == 5
    assert row["active_sessions"] == 2
    assert row["active_users"] == 2


def test_flatten_window(spark):
    rows = [
        ev("e1", "u1", "s1", "purchase", P("p1", "Widget", 100.0), 100.0, T(12, 0, 10)),
    ]
    flat = flatten_window(revenue_per_minute(make_events(spark, rows))).collect()[0]
    assert "window" not in flat.__fields__
    assert flat["window_start"] == "2026-01-01 12:00:00"
    assert flat["window_end"] == "2026-01-01 12:01:00"
    assert flat["revenue"] == 100.0


def test_event_time_is_timestamp(spark):
    from pyspark.sql.types import TimestampType

    rows = [ev("e1", "u1", "s1", "page_view", None, None, T(12, 0, 10))]
    df = make_events(spark, rows)
    assert isinstance(df.schema["event_time"].dataType, TimestampType)
    assert df.count() == 1
