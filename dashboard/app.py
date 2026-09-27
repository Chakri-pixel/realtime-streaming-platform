"""Live Streamlit dashboard for the realtime clickstream platform.

Polls the JSON files written by the Spark streaming job (``*_history.jsonl``
for time series, ``*_latest.json`` for current snapshots) and renders
throughput, revenue, top products, funnel conversion, and latency.
"""
import json
import os
import time

import pandas as pd
import streamlit as st

SERVING_DIR = os.environ.get("SERVING_DIR", "data/serving")
REFRESH_SEC = int(os.environ.get("DASHBOARD_REFRESH_SEC", "5"))

st.set_page_config(
    page_title="Realtime Clickstream Analytics",
    page_icon="⚡",
    layout="wide",
)
st.title("⚡ Realtime Clickstream Analytics")
st.caption("Spark Structured Streaming · Redpanda · event-time windows with watermarks")


def read_history(name: str) -> pd.DataFrame:
    path = os.path.join(SERVING_DIR, f"{name}_history.jsonl")
    if not os.path.exists(path):
        return pd.DataFrame()
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return pd.DataFrame(rows)


def read_latest(name: str) -> list:
    path = os.path.join(SERVING_DIR, f"{name}_latest.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []


activity = read_history("activity")
revenue = read_history("revenue")
latency = read_history("latency")
top_products = read_latest("top_products")
funnel = read_latest("funnel")

if activity.empty:
    st.info(
        "⏳ Waiting for streaming data — start the pipeline with "
        "`docker compose up --build` and check back in ~2 minutes."
    )
    st.stop()

# ---- KPI row ---------------------------------------------------------------
latest_activity = activity.sort_values("window_start").iloc[-1]
events_per_sec = latest_activity["events"] / 60.0

latest_revenue = revenue.sort_values("window_start").iloc[-1] if not revenue.empty else None
latest_latency = latency.sort_values("batch_id").iloc[-1] if not latency.empty else None

k1, k2, k3, k4 = st.columns(4)
k1.metric("Throughput", f"{events_per_sec:,.1f} events/sec")
k2.metric("Active sessions", f"{int(latest_activity['active_sessions']):,}")
k3.metric(
    "Revenue (last min)",
    f"${latest_revenue['revenue']:,.2f}" if latest_revenue is not None else "—",
)
k4.metric(
    "Avg end-to-end latency",
    f"{latest_latency['avg_latency_sec']:.1f}s" if latest_latency is not None else "—",
)

# ---- Time series -----------------------------------------------------------
c1, c2 = st.columns(2)
with c1:
    st.subheader("Revenue per minute")
    if not revenue.empty:
        df = revenue.sort_values("window_start").set_index("window_start")["revenue"]
        st.line_chart(df)
    else:
        st.caption("waiting for purchase windows…")
with c2:
    st.subheader("Throughput (events/sec)")
    df = activity.sort_values("window_start").set_index("window_start")["events"] / 60.0
    st.area_chart(df)

# ---- Products + funnel -----------------------------------------------------
c3, c4 = st.columns(2)
with c3:
    st.subheader("Top products (5-min window)")
    if top_products:
        pdf = pd.DataFrame(top_products)
        pdf = pdf.sort_values("interactions", ascending=False).head(10)
        st.bar_chart(pdf.set_index("product_name")["interactions"])
    else:
        st.caption("waiting for product windows…")

with c4:
    st.subheader("Conversion funnel (5-min window)")
    if funnel:
        f = funnel[-1]  # most recently closed window
        viewed = f.get("viewed") or 0
        carted = f.get("carted") or 0
        purchased = f.get("purchased") or 0
        st.metric("Sessions", f"{f.get('sessions', 0):,}")
        for label, value, base in [
            ("Viewed product", viewed, None),
            ("Added to cart", carted, viewed),
            ("Purchased", purchased, carted),
        ]:
            pct = f"{value / base:.1%}" if base else "—"
            st.write(f"**{label}:** {value:,} ({pct})")
            st.progress(min(value / base, 1.0) if base else 0.0)
        if viewed:
            st.caption(f"Overall view → purchase: {purchased / viewed:.1%}")
    else:
        st.caption("waiting for funnel windows…")

# ---- Latency ----------------------------------------------------------------
with st.expander("End-to-end latency trend"):
    if not latency.empty:
        df = latency.sort_values("batch_id").set_index("batch_id")["avg_latency_sec"]
        st.line_chart(df)
        st.caption(
            "Processing time minus event time, averaged per micro-batch. "
            "Includes the generator's intentional late arrivals."
        )
    else:
        st.caption("waiting for latency samples…")

st.sidebar.header("Pipeline")
st.sidebar.checkbox("Auto-refresh", value=True, key="auto")
st.sidebar.caption(f"Refreshing every {REFRESH_SEC}s from `{SERVING_DIR}`")
st.sidebar.caption("Redpanda console: http://localhost:8080")

if st.session_state.auto:
    time.sleep(REFRESH_SEC)
    st.rerun()
