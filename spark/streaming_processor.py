#!/usr/bin/env python3
"""Kafka -> Spark Structured Streaming -> HDFS + HBase.

Four independent queries are started:
  1. raw_events: append-only Parquet archive in HDFS
  2. summary_metrics: one-minute totals, approximate unique visitors and spike alerts
  3. page_metrics: request counts by page and one-minute window
  4. status_metrics: request counts by HTTP status and one-minute window

Every query has a distinct checkpoint directory. HBase writes are absolute upserts
using deterministic row keys, so retrying a foreachBatch micro-batch overwrites rows.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = lambda *args, **kwargs: None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hbase.database_operations import upsert_alerts, upsert_metrics
from spark.anomaly_detector import AnomalyDetector

LOG = logging.getLogger("traffic-streaming")

def env(name: str, default: str) -> str:
    return os.getenv(name, default)

def timestamp_text(value: Any) -> str:
    """Normalize Spark's Python datetime value to a UTC ISO string."""
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def write_summary_batch(batch_df, batch_id: int, spike_threshold: int) -> None:
    """Write the latest absolute window values; do not increment counters on retry."""
    rows = batch_df.collect()
    metric_records = []
    alert_records = []
    detector = AnomalyDetector(spike_threshold=spike_threshold)

    for row in rows:
        start = timestamp_text(row["window_start"])
        end = timestamp_text(row["window_end"])
        requests = int(row["requests"])
        unique_visitors = int(row["unique_visitors_approx"])
        client_errs = int(row["client_errors_4xx"] or 0)
        server_errs = int(row["server_errors_5xx"] or 0)
        avg_resp = round(float(row["avg_response_time_ms"] or 0.0), 2)

        payload = {
            "window_start": start,
            "window_end": end,
            "requests": requests,
            "unique_visitors_approx": unique_visitors,
            "client_errors_4xx": client_errs,
            "server_errors_5xx": server_errs,
            "avg_response_time_ms": avg_resp,
            "spike_threshold": int(spike_threshold),
            "is_spike": requests >= int(spike_threshold),
            "last_batch_id": int(batch_id),
        }
        metric_records.append({
            "metric_type": "summary",
            "window_start": start,
            "payload": payload,
        })

        # Run multi-rule anomaly detection (spikes, error surges, high latency)
        window_alerts = detector.evaluate_window(
            window_start=start,
            window_end=end,
            requests=requests,
            client_errors_4xx=client_errs,
            server_errors_5xx=server_errs,
            avg_response_time_ms=avg_resp,
            unique_visitors=unique_visitors,
        )
        alert_records.extend(window_alerts)

    metrics_written = upsert_metrics(metric_records)
    alerts_written = upsert_alerts(alert_records)
    LOG.info(
        "summary batch=%s metric_rows=%s alert_rows=%s",
        batch_id, metrics_written, alerts_written
    )

def write_dimension_batch(batch_df, batch_id: int, metric_type: str) -> None:
    """Persist page/status counts as deterministic HBase upserts."""
    rows = batch_df.collect()
    records = []
    for row in rows:
        start = timestamp_text(row["window_start"])
        end = timestamp_text(row["window_end"])
        if metric_type == "page":
            dimension = str(row["page"])
            payload = {
                "window_start": start,
                "window_end": end,
                "page": dimension,
                "requests": int(row["requests"]),
                "last_batch_id": int(batch_id),
            }
        elif metric_type == "status":
            dimension = str(row["status_code"])
            payload = {
                "window_start": start,
                "window_end": end,
                "status_code": int(row["status_code"]),
                "requests": int(row["requests"]),
                "last_batch_id": int(batch_id),
            }
        else:
            raise ValueError(f"Unsupported metric type: {metric_type}")
        records.append({
            "metric_type": metric_type,
            "window_start": start,
            "dimension": dimension,
            "payload": payload,
        })

    written = upsert_metrics(records)
    LOG.info("%s batch=%s metric_rows=%s", metric_type, batch_id, written)

def build_spark_session():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(env("SPARK_APP_NAME", "WebsiteTrafficStreaming"))
        .config("spark.sql.shuffle.partitions",
                env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def event_schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType
    )
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("visitor_id", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("page", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("response_time", IntegerType(), True),
        StructField("request_method", StringType(), True),
        StructField("user_agent", StringType(), True),
    ])

def kafka_events(spark, schema):
    from pyspark.sql import functions as F
    source = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", env("KAFKA_TOPIC", "website_traffic"))
        .option("startingOffsets", env("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        source
        .select(F.from_json(F.col("value").cast("string"), schema).alias("event"))
        .select("event.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(
            F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("visitor_id").isNotNull()
            & F.col("page").isNotNull()
            & F.col("status_code").between(100, 599)
            & (F.col("response_time_ms") >= 0)
        )
        .withColumn("event_date", F.date_format("event_time", "yyyy-MM-dd"))
    )

def start_queries(spark):
    from pyspark.sql import functions as F

    schema = event_schema()
    raw_path = env(
        "HDFS_RAW_EVENTS_PATH",
        "file:///tmp/website-traffic/raw/events"
    )
    checkpoint_base = env(
        "STREAMING_CHECKPOINT_BASE",
        "file:///tmp/website-traffic/checkpoints"
    ).rstrip("/")
    trigger = env("SPARK_TRIGGER_INTERVAL", "10 seconds")
    window_duration = env("METRIC_WINDOW_DURATION", "1 minute")
    watermark_delay = env("METRIC_WATERMARK_DELAY", "2 minutes")
    spike_threshold = int(env("SPIKE_THRESHOLD_REQUESTS_PER_MINUTE", "600"))
    rsd = float(env("APPROX_COUNT_DISTINCT_RSD", "0.05"))

    # 1. Archive all validated events in partitioned Parquet files.
    raw_query = (
        kafka_events(spark, schema)
        .select(
            "event_id", "timestamp", "event_time", "visitor_id", "ip",
            "page", "status_code", "response_time_ms", "event_date"
        )
        .writeStream
        .queryName("raw_events_to_hdfs")
        .format("parquet")
        .outputMode("append")
        .option("path", raw_path)
        .option("checkpointLocation", f"{checkpoint_base}/raw_events")
        .partitionBy("event_date")
        .trigger(processingTime=trigger)
        .start()
    )

    # 2. Summary: approx_count_distinct is used because exact distinct aggregation
    # is not supported on streaming DataFrames. The result is explicitly labelled
    # approximate in HBase and the dashboard.
    summary = (
        kafka_events(spark, schema)
        .withWatermark("event_time", watermark_delay)
        .groupBy(F.window("event_time", window_duration))
        .agg(
            F.count("*").alias("requests"),
            F.approx_count_distinct("visitor_id", rsd)
             .alias("unique_visitors_approx"),
            F.sum(F.when(F.col("status_code").between(400, 499), 1).otherwise(0))
             .alias("client_errors_4xx"),
            F.sum(F.when(F.col("status_code").between(500, 599), 1).otherwise(0))
             .alias("server_errors_5xx"),
            F.avg("response_time_ms").alias("avg_response_time_ms"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "requests", "unique_visitors_approx", "client_errors_4xx",
            "server_errors_5xx", "avg_response_time_ms"
        )
    )
    summary_query = (
        summary.writeStream
        .queryName("summary_metrics_to_hbase")
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/summary_metrics")
        .trigger(processingTime=trigger)
        .foreachBatch(
            lambda df, batch_id: write_summary_batch(df, batch_id, spike_threshold)
        )
        .start()
    )

    # 3. Popular pages by one-minute event-time window.
    page_counts = (
        kafka_events(spark, schema)
        .withWatermark("event_time", watermark_delay)
        .groupBy(F.window("event_time", window_duration), F.col("page"))
        .agg(F.count("*").alias("requests"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "page", "requests"
        )
    )
    page_query = (
        page_counts.writeStream
        .queryName("page_metrics_to_hbase")
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/page_metrics")
        .trigger(processingTime=trigger)
        .foreachBatch(
            lambda df, batch_id: write_dimension_batch(df, batch_id, "page")
        )
        .start()
    )

    # 4. HTTP status distribution by one-minute event-time window.
    status_counts = (
        kafka_events(spark, schema)
        .withWatermark("event_time", watermark_delay)
        .groupBy(F.window("event_time", window_duration), F.col("status_code"))
        .agg(F.count("*").alias("requests"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "status_code", "requests"
        )
    )
    status_query = (
        status_counts.writeStream
        .queryName("status_metrics_to_hbase")
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/status_metrics")
        .trigger(processingTime=trigger)
        .foreachBatch(
            lambda df, batch_id: write_dimension_batch(df, batch_id, "status")
        )
        .start()
    )

    return [raw_query, summary_query, page_query, status_query]

def main():
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(
        description="Run Kafka -> Spark Structured Streaming -> HDFS/HBase."
    )
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args()
    logging.basicConfig(
        level=(args.log_level or env("SPARK_LOG_LEVEL", "INFO")).upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    spark = build_spark_session()
    queries = []
    try:
        queries = start_queries(spark)
        LOG.info("Started streaming queries: %s", [query.name for query in queries])
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        LOG.info("Shutdown requested.")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()

if __name__ == "__main__":
    main()
