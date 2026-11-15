"""Streamlit interactive dashboard for real-time website traffic analytics and anomaly detection.

Features 8 dedicated analytical views:
  1. Overview
  2. Live Traffic Monitoring
  3. Visitor Analytics
  4. Page Analytics
  5. HTTP Error Monitoring
  6. Anomaly Detection
  7. Historical Reports (HDFS)
  8. System / Cluster Status
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = lambda *args, **kwargs: None

try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hbase.database_operations import health_check, read_alerts, read_metrics

st.set_page_config(
    page_title="Traffic Analytics & Anomaly Detection",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

HDFS_REPORTS_PATH = os.getenv(
    "HDFS_REPORTS_PATH", "file:///tmp/website-traffic/reports"
).rstrip("/")

# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=8, show_spinner=False)
def load_live_data():
    return {
        "summary": read_metrics("summary", limit=180),
        "pages": read_metrics("page", limit=600),
        "statuses": read_metrics("status", limit=180),
        "alerts": read_alerts(limit=50),
    }

def load_hdfs_json(report_name: str) -> list[dict]:
    """Read Spark JSON report part files from HDFS or local filesystem."""
    # 1. Local filesystem path support (e.g. file:///tmp/... or local relative/absolute path)
    if HDFS_REPORTS_PATH.startswith("file://") or "://" not in HDFS_REPORTS_PATH:
        clean_path = HDFS_REPORTS_PATH.replace("file://", "")
        if os.name == "nt" and clean_path.startswith("/") and len(clean_path) > 2 and clean_path[2] == ":":
            clean_path = clean_path[1:]
        pattern = os.path.join(clean_path, report_name, "part-*.json")
        matched = glob.glob(pattern)
        if not matched:
            raise RuntimeError(f"No local JSON report parts found at {pattern}")
        records = []
        for file_path in matched:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_s = line.strip()
                    if line_s:
                        try:
                            records.append(json.loads(line_s))
                        except json.JSONDecodeError:
                            continue
        return records

    # 2. HDFS cluster query via Hadoop CLI
    path = f"{HDFS_REPORTS_PATH}/{report_name}/part-*.json"
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-cat", path],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(
            "Hadoop CLI is unavailable or timed out. Run Streamlit on a host with "
            "HADOOP_HOME/HADOOP_CONF_DIR configured and the HDFS cluster reachable."
        ) from exc
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"No HDFS report found: {path}")
    records = []
    for line in result.stdout.splitlines():
        line_s = line.strip()
        if line_s:
            try:
                records.append(json.loads(line_s))
            except json.JSONDecodeError:
                continue
    return records

# ---------------------------------------------------------------------------
# Sidebar & Header
# ---------------------------------------------------------------------------
st.sidebar.title("🚦 Traffic Analytics")
st.sidebar.caption("CSBB422 Big Data Analytics Lab Project")
st.sidebar.markdown(
    """
    **Distributed Pipeline:**
    - Traffic Simulator
    - Apache Kafka (KRaft)
    - Apache Spark Streaming
    - Hadoop HDFS (Raw Data)
    - Apache HBase (Metrics)
    - Streamlit Dashboard
    """
)
st.sidebar.divider()
refresh_button = st.sidebar.button("🔄 Refresh Data Now")
if refresh_button:
    st.cache_data.clear()

st.title("Real-Time Website Traffic Analytics & Anomaly Detection")
st.caption("Live distributed telemetry from Apache Kafka, Apache Spark Streaming, Hadoop HDFS, and Apache HBase")

# Fetch live HBase metrics
live_data = None
fetch_error = None
try:
    live_data = load_live_data()
except Exception as exc:
    fetch_error = str(exc)

summaries = live_data["summary"] if live_data else []
pages_data = live_data["pages"] if live_data else []
status_data = live_data["statuses"] if live_data else []
alerts_data = live_data["alerts"] if live_data else []

summary_df = pd.DataFrame(summaries)
if not summary_df.empty:
    summary_df["window_start"] = pd.to_datetime(summary_df["window_start"], utc=True)
    summary_df = summary_df.sort_values("window_start")

latest = summary_df.iloc[-1] if not summary_df.empty else None

# ---------------------------------------------------------------------------
# Tab Navigation (8 Required Sections)
# ---------------------------------------------------------------------------
tabs = st.tabs([
    "1. Overview",
    "2. Live Traffic Monitoring",
    "3. Visitor Analytics",
    "4. Page Analytics",
    "5. HTTP Error Monitoring",
    "6. Anomaly Detection",
    "7. Historical Reports",
    "8. System / Cluster Status",
])

# ---------------------------------------------------------------------------
# Tab 1: Overview
# ---------------------------------------------------------------------------
with tabs[0]:
    st.subheader("System Overview")
    if fetch_error:
        st.warning(f"⚠️ HBase Thrift is unreachable. Start HBase on the master node to view live metrics.\nDetails: {fetch_error}")
    elif summary_df.empty:
        st.info("ℹ️ No streaming metrics collected yet. Start the Spark Structured Streaming job and Traffic Generator.")
    else:
        req_min = int(latest["requests"])
        uniq_vis = int(latest["unique_visitors_approx"])
        err_4xx = int(latest.get("client_errors_4xx", 0))
        err_5xx = int(latest.get("server_errors_5xx", 0))
        total_errs = err_4xx + err_5xx
        err_pct = (100.0 * total_errs / req_min) if req_min > 0 else 0.0
        avg_resp = float(latest.get("avg_response_time_ms", 0.0))

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Requests / Minute", f"{req_min:,}")
        c2.metric("Unique Visitors (approx)", f"{uniq_vis:,}")
        c3.metric("Error Rate", f"{err_pct:.1f}%", f"{total_errs} errors", delta_color="inverse")
        c4.metric("Avg Latency", f"{avg_resp:.1f} ms")
        is_spike = latest.get("is_spike", False)
        c5.metric("Traffic State", "⚠️ SPIKE" if is_spike else "✅ NORMAL")

        st.caption("*Unique visitors are estimated in real-time using Spark HyperLogLog (approx_count_distinct).")

        # Visual Highlights
        col_left, col_right = st.columns(2)
        with col_left:
            st.markdown("##### Real-Time Traffic Trend")
            if HAS_PLOTLY:
                fig = px.line(
                    summary_df,
                    x="window_start",
                    y="requests",
                    labels={"window_start": "Time (UTC)", "requests": "Requests / min"},
                    title="Traffic Volume (Last Windows)",
                )
                fig.update_layout(margin=dict(l=20, r=20, t=35, b=20), height=280)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.line_chart(summary_df.set_index("window_start")[["requests"]], height=280)

        with col_right:
            st.markdown("##### Latest Status Code Breakdown")
            if status_data:
                latest_win_str = str(latest["window_start"])
                st_df = pd.DataFrame(status_data)
                st_df = st_df[st_df["window_start"] == latest_win_str]
                if not st_df.empty:
                    st_df["status_code"] = st_df["status_code"].astype(str)
                    if HAS_PLOTLY:
                        fig = px.pie(
                            st_df,
                            names="status_code",
                            values="requests",
                            hole=0.45,
                            title="HTTP Status Distribution",
                        )
                        fig.update_layout(margin=dict(l=20, r=20, t=35, b=20), height=280)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.bar_chart(st_df.set_index("status_code")[["requests"]], height=280)
                else:
                    st.info("No status breakdown for the latest window.")
            else:
                st.info("No status data available.")

# ---------------------------------------------------------------------------
# Tab 2: Live Traffic Monitoring
# ---------------------------------------------------------------------------
with tabs[1]:
    st.subheader("Live Traffic Monitoring")
    if summary_df.empty:
        st.info("No live traffic streaming yet.")
    else:
        st.markdown("#### Requests Per Minute (Event-Time 1-Minute Windows)")
        if HAS_PLOTLY:
            fig = px.area(
                summary_df,
                x="window_start",
                y="requests",
                labels={"window_start": "Window Start (UTC)", "requests": "Total Requests"},
                title="Real-Time Website Request Rate",
            )
            # Add spike threshold line
            spike_thresh = int(latest.get("spike_threshold", 600))
            fig.add_hline(
                y=spike_thresh,
                line_dash="dot",
                annotation_text=f"Spike Threshold ({spike_thresh}/min)",
                line_color="red",
            )
            fig.update_layout(margin=dict(l=20, r=20, t=35, b=20), height=380)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.line_chart(summary_df.set_index("window_start")[["requests"]], height=350)

        st.markdown("##### Recent Window Aggregates")
        display_cols = [
            "window_start", "window_end", "requests", "unique_visitors_approx",
            "client_errors_4xx", "server_errors_5xx", "avg_response_time_ms", "is_spike"
        ]
        available_cols = [c for c in display_cols if c in summary_df.columns]
        st.dataframe(
            summary_df[available_cols].iloc[::-1].head(15),
            use_container_width=True,
            hide_index=True,
        )

# ---------------------------------------------------------------------------
# Tab 3: Visitor Analytics
# ---------------------------------------------------------------------------
with tabs[2]:
    st.subheader("Visitor Analytics")
    if summary_df.empty:
        st.info("No visitor metrics available.")
    else:
        v_col1, v_col2 = st.columns(2)
        with v_col1:
            st.markdown("##### Unique Visitors vs Total Requests")
            if HAS_PLOTLY:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=summary_df["window_start"],
                    y=summary_df["requests"],
                    name="Total Requests",
                    marker_color="rgba(55, 128, 191, 0.7)",
                ))
                fig.add_trace(go.Scatter(
                    x=summary_df["window_start"],
                    y=summary_df["unique_visitors_approx"],
                    name="Unique Visitors",
                    mode="lines+markers",
                    line=dict(color="orange", width=2),
                ))
                fig.update_layout(
                    title="Traffic Volume vs Concurrent Visitors",
                    xaxis_title="Time (UTC)",
                    yaxis_title="Count",
                    margin=dict(l=20, r=20, t=35, b=20),
                    height=320,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.line_chart(summary_df.set_index("window_start")[["unique_visitors_approx", "requests"]], height=320)

        with v_col2:
            st.markdown("##### Requests Per Visitor Ratio")
            ratio_df = summary_df.copy()
            ratio_df["req_per_visitor"] = (
                ratio_df["requests"] / ratio_df["unique_visitors_approx"].replace(0, 1)
            ).round(2)
            if HAS_PLOTLY:
                fig_ratio = px.line(
                    ratio_df,
                    x="window_start",
                    y="req_per_visitor",
                    labels={"window_start": "Time (UTC)", "req_per_visitor": "Avg Requests / Visitor"},
                    title="User Session Engagement Depth",
                )
                fig_ratio.update_layout(margin=dict(l=20, r=20, t=35, b=20), height=320)
                st.plotly_chart(fig_ratio, use_container_width=True)
            else:
                st.line_chart(ratio_df.set_index("window_start")[["req_per_visitor"]], height=320)

        st.info(
            "💡 **Big Data Methodological Note:** In Spark Structured Streaming, computing exact distinct counts across continuous streaming windows is computationally infeasible without maintaining unbounded state. We use HyperLogLog (`approx_count_distinct` with relative standard deviation 0.05), which provides memory-bounded, highly accurate distinct visitor approximations."
        )

# ---------------------------------------------------------------------------
# Tab 4: Page Analytics
# ---------------------------------------------------------------------------
with tabs[3]:
    st.subheader("Page Analytics")
    if not pages_data:
        st.info("No page analytics recorded yet.")
    else:
        page_df = pd.DataFrame(pages_data)
        latest_win_str = str(latest["window_start"]) if latest is not None else ""
        current_page_df = page_df[page_df["window_start"] == latest_win_str]

        if current_page_df.empty:
            current_page_df = page_df.tail(20)

        current_page_df = current_page_df.sort_values("requests", ascending=False)

        col_p1, col_p2 = st.columns([3, 2])
        with col_p1:
            st.markdown("##### Most Visited Pages (Latest Window)")
            if HAS_PLOTLY:
                fig = px.bar(
                    current_page_df.head(10),
                    x="requests",
                    y="page",
                    orientation="h",
                    labels={"requests": "Requests", "page": "Endpoint"},
                    title="Top Requested Endpoints",
                    color="requests",
                    color_continuous_scale="Viridis",
                )
                fig.update_layout(
                    yaxis=dict(autorange="reversed"),
                    margin=dict(l=20, r=20, t=35, b=20),
                    height=360,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.bar_chart(current_page_df.head(10).set_index("page")[["requests"]], height=360)

        with col_p2:
            st.markdown("##### Page Request Breakdown")
            display_page_df = current_page_df[["page", "requests"]].copy()
            total_page_reqs = display_page_df["requests"].sum()
            display_page_df["share"] = (
                100.0 * display_page_df["requests"] / (total_page_reqs if total_page_reqs > 0 else 1)
            ).round(1).astype(str) + "%"
            st.dataframe(display_page_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Tab 5: HTTP Error Monitoring
# ---------------------------------------------------------------------------
with tabs[4]:
    st.subheader("HTTP Error Monitoring")
    if summary_df.empty:
        st.info("No error statistics available.")
    else:
        err_col1, err_col2 = st.columns(2)
        with err_col1:
            st.markdown("##### Error Rate Trend (% 4xx + 5xx)")
            err_trend_df = summary_df.copy()
            err_trend_df["error_pct"] = (
                100.0 * (err_trend_df["client_errors_4xx"] + err_trend_df["server_errors_5xx"])
                / err_trend_df["requests"].replace(0, 1)
            ).round(2)
            if HAS_PLOTLY:
                fig = px.line(
                    err_trend_df,
                    x="window_start",
                    y="error_pct",
                    labels={"window_start": "Time (UTC)", "error_pct": "Error Percentage (%)"},
                    title="HTTP Error Rate Over Time",
                )
                fig.add_hline(y=15.0, line_dash="dash", line_color="red", annotation_text="Anomaly Threshold (15%)")
                fig.update_layout(margin=dict(l=20, r=20, t=35, b=20), height=320)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.line_chart(err_trend_df.set_index("window_start")[["error_pct"]], height=320)

        with err_col2:
            st.markdown("##### Client (4xx) vs Server (5xx) Errors")
            if HAS_PLOTLY:
                fig_errs = go.Figure()
                fig_errs.add_trace(go.Bar(
                    x=summary_df["window_start"],
                    y=summary_df["client_errors_4xx"],
                    name="4xx Client Errors",
                    marker_color="orange",
                ))
                fig_errs.add_trace(go.Bar(
                    x=summary_df["window_start"],
                    y=summary_df["server_errors_5xx"],
                    name="5xx Server Errors",
                    marker_color="crimson",
                ))
                fig_errs.update_layout(
                    barmode="stack",
                    title="Error Distribution by Category",
                    xaxis_title="Time (UTC)",
                    yaxis_title="Count",
                    margin=dict(l=20, r=20, t=35, b=20),
                    height=320,
                )
                st.plotly_chart(fig_errs, use_container_width=True)
            else:
                st.bar_chart(summary_df.set_index("window_start")[["client_errors_4xx", "server_errors_5xx"]], height=320)

# ---------------------------------------------------------------------------
# Tab 6: Anomaly Detection
# ---------------------------------------------------------------------------
with tabs[5]:
    st.subheader("Traffic Anomaly Detection")
    st.caption("Multi-criteria statistical anomaly evaluation: Traffic Spikes, High Error Rates, and Latency Spikes.")

    m1, m2, m3 = st.columns(3)
    m1.info("⚡ **Rule 1: Traffic Spike**\n\nTriggers when requests/min exceed baseline threshold (default: 600/min).")
    m2.warning("🔥 **Rule 2: Error Rate Surge**\n\nTriggers when 4xx + 5xx error percentage exceeds 15% with >= 20 requests.")
    m3.error("⏱️ **Rule 3: High Latency Spike**\n\nTriggers when average server response latency exceeds 600 ms.")

    st.markdown("##### Anomaly Alert History (from Apache HBase)")
    if alerts_data:
        alert_df = pd.DataFrame(alerts_data)
        # Ensure standard columns exist
        for col in ["alert_type", "severity", "observed_value", "threshold", "reason"]:
            if col not in alert_df.columns:
                alert_df[col] = "N/A"

        # Severity filter
        severities = ["ALL"] + sorted([s for s in alert_df["severity"].unique() if s != "N/A"])
        selected_sev = st.selectbox("Filter by Severity:", severities, index=0)
        filtered_alerts = alert_df if selected_sev == "ALL" else alert_df[alert_df["severity"] == selected_sev]

        cols_to_show = ["window_start", "alert_type", "severity", "observed_value", "threshold", "reason"]
        valid_cols = [c for c in cols_to_show if c in filtered_alerts.columns]
        st.dataframe(
            filtered_alerts[valid_cols],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("✅ No anomalous traffic conditions detected in recent windows. System operating normally.")

# ---------------------------------------------------------------------------
# Tab 7: Historical Reports (HDFS)
# ---------------------------------------------------------------------------
with tabs[6]:
    st.subheader("Historical Batch Analytics (Hadoop HDFS)")
    st.caption("Reports precomputed by Apache Spark batch job from raw Parquet event logs stored in HDFS.")

    today_utc = datetime.now(timezone.utc).date()
    date_filter = st.date_input(
        "Historical Date Filter:",
        value=(today_utc - timedelta(days=7), today_utc),
    )
    if isinstance(date_filter, tuple) and len(date_filter) == 2:
        start_d, end_d = date_filter
    else:
        start_d = end_d = date_filter

    try:
        daily_records = load_hdfs_json("daily_summary")
        hourly_records = load_hdfs_json("hourly_summary")
        top_records = load_hdfs_json("top_pages")
        status_records = load_hdfs_json("status_distribution")
        response_records = load_hdfs_json("response_time_trend")

        # Daily summary
        if daily_records:
            d_df = pd.DataFrame(daily_records)
            d_df["event_date"] = pd.to_datetime(d_df["event_date"]).dt.date
            d_df = d_df[(d_df["event_date"] >= start_d) & (d_df["event_date"] <= end_d)].sort_values("event_date")
            if not d_df.empty:
                st.markdown("##### Daily Request Volume")
                if HAS_PLOTLY:
                    fig_d = px.bar(
                        d_df,
                        x="event_date",
                        y="requests",
                        title="Daily Request Aggregates",
                        labels={"event_date": "Date", "requests": "Requests"},
                    )
                    fig_d.update_layout(margin=dict(l=20, r=20, t=35, b=20), height=280)
                    st.plotly_chart(fig_d, use_container_width=True)
                else:
                    st.bar_chart(d_df.set_index("event_date")[["requests"]], height=280)
                st.dataframe(d_df, use_container_width=True, hide_index=True)

        # Response time trends (avg vs p95)
        if response_records:
            st.markdown("##### Latency Trend: Average vs P95 Percentile")
            resp_df = pd.DataFrame(response_records).sort_values("hour")
            resp_df["hour_dt"] = pd.to_datetime(resp_df["hour"], utc=True)
            resp_df = resp_df[
                (resp_df["hour_dt"].dt.date >= start_d) & (resp_df["hour_dt"].dt.date <= end_d)
            ]
            if not resp_df.empty:
                if HAS_PLOTLY:
                    fig_lat = px.line(
                        resp_df,
                        x="hour_dt",
                        y=["avg_response_time_ms", "p95_response_time_ms"],
                        labels={"hour_dt": "Hour (UTC)", "value": "Latency (ms)", "variable": "Metric"},
                        title="Hourly Latency Distribution",
                    )
                    fig_lat.update_layout(margin=dict(l=20, r=20, t=35, b=20), height=300)
                    st.plotly_chart(fig_lat, use_container_width=True)
                else:
                    st.line_chart(resp_df.set_index("hour_dt")[["avg_response_time_ms", "p95_response_time_ms"]], height=300)

    except Exception as exc:
        st.info(
            f"ℹ️ Historical HDFS reports are not available yet. "
            f"Run `spark/batch_processor.py` (or `scripts/run_batch.sh`) after raw Parquet events have accumulated.\n\n"
            f"Status note: {exc}"
        )

# ---------------------------------------------------------------------------
# Tab 8: System / Cluster Status
# ---------------------------------------------------------------------------
with tabs[7]:
    st.subheader("System & Distributed Cluster Status")
    st.caption("3-Node Physical Deployment Health Monitoring (Master + 2 Workers)")

    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown(
            """
            #### 🖥️ PC 1: Master Node
            - **Hadoop NameNode** (`:9000`, UI `:9870`)
            - **Spark Master** (`:7077`, UI `:8080`)
            - **Kafka Broker/KRaft 1** (`:9092`, `:9093`)
            - **HBase Master** (`:16000`, UI `:16010`)
            - **HBase Thrift Gateway** (`:9090`)
            - **Streamlit Host** (`:8501`)
            """
        )
    with s2:
        st.markdown(
            """
            #### 🖥️ PC 2: Worker Node 1
            - **HDFS DataNode 1** (`:9864`)
            - **Spark Worker 1** (2 cores, 2 GB)
            - **Kafka Broker/KRaft 2** (`:9092`, `:9093`)
            - **HBase RegionServer 1** (`:16020`, UI `:16030`)
            """
        )
    with s3:
        st.markdown(
            """
            #### 🖥️ PC 3: Worker Node 2
            - **HDFS DataNode 2** (`:9864`)
            - **HDFS SecondaryNameNode** (`:9868`)
            - **Spark Worker 2** (2 cores, 2 GB)
            - **Kafka Broker/KRaft 3** (`:9092`, `:9093`)
            - **HBase RegionServer 2** (`:16020`, UI `:16030`)
            - **Traffic Generator Host**
            """
        )

    st.divider()
    st.markdown("##### Live Service Probes")
    probe_col1, probe_col2 = st.columns(2)
    with probe_col1:
        st.markdown("**HBase Thrift Service Probe:**")
        try:
            h_info = health_check()
            st.success(f"Connected to HBase at `{h_info['host']}:{h_info['port']}`")
            st.json(h_info)
        except Exception as exc:
            st.error(f"HBase Thrift connection failed: {exc}")

    with probe_col2:
        st.markdown("**Diagnostic Commands for Viva Demonstration:**")
        st.code(
            """# 1. Verify HDFS status and DataNodes:
hdfs dfsadmin -report

# 2. Inspect Kafka KRaft topic partitions and replicas:
kafka-topics.sh --bootstrap-server master:9092 --describe --topic website_traffic

# 3. Inspect Spark cluster registered workers:
curl -s http://master:8080 | grep -E 'Spark Master|worker'

# 4. Scan latest HBase metric rows:
python -m hbase.database_operations scan --type summary --limit 5""",
            language="bash",
        )
