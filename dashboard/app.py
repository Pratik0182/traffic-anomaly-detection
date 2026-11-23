"""Streamlit UI for live HBase metrics and historical HDFS reports."""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hbase.database_operations import read_alerts, read_metrics

st.set_page_config(
    page_title="Website Traffic Analytics",
    page_icon="📊",
    layout="wide",
)
st.title("🌐 Real-Time Website Traffic Analytics")
st.caption("Synthetic website events · Kafka · Spark Structured Streaming · HDFS · HBase")

HDFS_REPORTS_PATH = os.getenv(
    "HDFS_REPORTS_PATH", "file:///tmp/website-traffic/reports"
).rstrip("/")

@st.cache_data(ttl=8, show_spinner=False)
def load_live_data():
    return {
        "summary": read_metrics("summary", limit=180),
        "pages": read_metrics("page", limit=600),
        "statuses": read_metrics("status", limit=180),
        "alerts": read_alerts(limit=30),
    }

@st.cache_resource
def get_spark_session():
    """Create a reusable local Spark session for reading HDFS reports."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("WebsiteTrafficDashboard")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_hdfs_json(report_name: str):
    """Read historical JSON reports directly from HDFS using Spark."""
    path = f"{HDFS_REPORTS_PATH}/{report_name}"
    try:
        dataframe = get_spark_session().read.json(path)
        return [row.asDict(recursive=True) for row in dataframe.collect()]
    except Exception as exc:
        raise RuntimeError(
            f"Could not read HDFS report: {path}. Details: {exc}"
        ) from exc


@st.fragment(run_every="10s")
def live_panel():
    st.subheader("Live metrics")
    st.caption("Refreshes automatically every 10 seconds.")
    try:
        data = load_live_data()
    except Exception as exc:
        st.warning(
            "HBase is not reachable yet. Start HBase and its Thrift service, then "
            f"refresh this page. Details: {exc}"
        )
        return

    summaries = data["summary"]
    if not summaries:
        st.info("No processed metrics yet. Start Spark streaming and then generate traffic.")
        return

    summary_df = pd.DataFrame(summaries).sort_values("window_start")
    latest = summary_df.iloc[-1]
    errors = int(latest.get("client_errors_4xx", 0)) + int(
        latest.get("server_errors_5xx", 0)
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Requests / minute", f"{int(latest['requests']):,}")
    c2.metric("Unique visitors*", f"{int(latest['unique_visitors_approx']):,}")
    c3.metric("HTTP errors", f"{errors:,}")
    c4.metric("Avg response time", f"{float(latest['avg_response_time_ms']):.1f} ms")
    st.caption("*Unique visitor count is approximate (Spark approx_count_distinct).")

    chart_df = summary_df.copy()
    chart_df["window_start"] = pd.to_datetime(chart_df["window_start"], utc=True)
    chart_df = chart_df.set_index("window_start")[["requests"]]
    st.markdown("**Traffic over time**")
    st.line_chart(chart_df, height=250)

    latest_window = str(latest["window_start"])
    page_df = pd.DataFrame(data["pages"])
    status_df = pd.DataFrame(data["statuses"])
    left, right = st.columns(2)

    with left:
        st.markdown("**Popular pages – latest window**")
        if not page_df.empty:
            page_df = page_df[page_df["window_start"] == latest_window]
            page_df = page_df.sort_values("requests", ascending=False).head(10)
            if not page_df.empty:
                st.bar_chart(page_df.set_index("page")[["requests"]], height=260)
            else:
                st.info("No page breakdown for the latest window yet.")
        else:
            st.info("Page metrics will appear after the first Spark micro-batch.")

    with right:
        st.markdown("**HTTP status distribution – latest window**")
        if not status_df.empty:
            status_df = status_df[status_df["window_start"] == latest_window]
            status_df["status_code"] = status_df["status_code"].astype(str)
            status_df = status_df.sort_values("status_code")
            if not status_df.empty:
                st.bar_chart(status_df.set_index("status_code")[["requests"]], height=260)
            else:
                st.info("No status breakdown for the latest window yet.")
        else:
            st.info("Status metrics will appear after the first Spark micro-batch.")

    st.markdown("**Traffic spike alerts**")
    alerts = data["alerts"]
    if alerts:
        st.dataframe(
            pd.DataFrame(alerts)[
                ["window_start", "requests", "threshold", "reason"]
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No traffic spike alerts recorded.")

live_panel()

st.divider()
st.subheader("Historical reports (HDFS)")
today_utc = datetime.now(timezone.utc).date()
default_range = (today_utc - timedelta(days=7), today_utc)
selected_range = st.date_input(
    "Date range",
    value=default_range,
    help="Filters all historical reports by UTC event date.",
)
if isinstance(selected_range, tuple) and len(selected_range) == 2:
    start_date, end_date = selected_range
else:
    start_date = end_date = selected_range

try:
    daily_records = load_hdfs_json("daily_summary")
    hourly_records = load_hdfs_json("hourly_summary")
    top_records = load_hdfs_json("top_pages")
    status_records = load_hdfs_json("status_distribution")
    response_records = load_hdfs_json("response_time_trend")

    daily_df = pd.DataFrame(daily_records)
    if not daily_df.empty:
        daily_df["event_date"] = pd.to_datetime(daily_df["event_date"]).dt.date
        daily_df = daily_df[
            (daily_df["event_date"] >= start_date)
            & (daily_df["event_date"] <= end_date)
        ]
        if not daily_df.empty:
            daily_df = daily_df.sort_values("event_date").set_index("event_date")
            st.markdown("**Daily requests**")
            st.line_chart(daily_df[["requests"]], height=220)
            st.dataframe(daily_df.reset_index(), use_container_width=True, hide_index=True)
        else:
            st.info("No daily records in the selected date range.")

    h1, h2 = st.columns(2)
    with h1:
        st.markdown("**Top pages (selected date range)**")
        if top_records:
            top_df = pd.DataFrame(top_records)
            top_df["event_date"] = pd.to_datetime(top_df["event_date"]).dt.date
            top_df = top_df[
                (top_df["event_date"] >= start_date)
                & (top_df["event_date"] <= end_date)
            ]
            if not top_df.empty:
                top_df = (
                    top_df.groupby("page", as_index=False)["requests"].sum()
                    .sort_values("requests", ascending=False).head(10)
                )
                st.bar_chart(top_df.set_index("page")[["requests"]])
            else:
                st.info("No page requests in the selected date range.")
        else:
            st.info("No top-page report available.")

    with h2:
        st.markdown("**HTTP status totals (selected date range)**")
        if status_records:
            status_report_df = pd.DataFrame(status_records)
            status_report_df["event_date"] = pd.to_datetime(
                status_report_df["event_date"]
            ).dt.date
            status_report_df = status_report_df[
                (status_report_df["event_date"] >= start_date)
                & (status_report_df["event_date"] <= end_date)
            ]
            if not status_report_df.empty:
                status_report_df = (
                    status_report_df.groupby("status_code", as_index=False)["requests"].sum()
                )
                status_report_df["status_code"] = status_report_df["status_code"].astype(str)
                st.bar_chart(status_report_df.set_index("status_code")[["requests"]])
            else:
                st.info("No status records in the selected date range.")
        else:
            st.info("No status report available.")

    if hourly_records:
        st.markdown("**Hourly traffic (selected date range)**")
        hourly_df = pd.DataFrame(hourly_records).sort_values("hour")
        hourly_df["hour"] = pd.to_datetime(hourly_df["hour"], utc=True)
        hourly_df = hourly_df[
            (hourly_df["hour"].dt.date >= start_date)
            & (hourly_df["hour"].dt.date <= end_date)
        ]
        if not hourly_df.empty:
            st.line_chart(hourly_df.set_index("hour")[["requests"]], height=220)
        else:
            st.info("No hourly traffic in the selected date range.")

    if response_records:
        st.markdown("**Response-time trend (average and p95, selected date range)**")
        response_df = pd.DataFrame(response_records).sort_values("hour")
        response_df["hour"] = pd.to_datetime(response_df["hour"], utc=True)
        response_df = response_df[
            (response_df["hour"].dt.date >= start_date)
            & (response_df["hour"].dt.date <= end_date)
        ]
        if not response_df.empty:
            st.line_chart(
                response_df.set_index("hour")[["avg_response_time_ms", "p95_response_time_ms"]],
                height=220,
            )
        else:
            st.info("No response-time records in the selected date range.")

except Exception as exc:
    st.info(
        "Historical HDFS reports are not available yet. Run "
        "`spark/batch_processor.py` after raw Parquet events have accumulated. "
        f"Details: {exc}"
    )
