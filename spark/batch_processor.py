#!/usr/bin/env python3
"""Historical batch analytics over raw Parquet events stored in HDFS."""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = lambda *args, **kwargs: None

LOG = logging.getLogger("traffic-batch")
ROOT = Path(__file__).resolve().parents[1]

def env(name: str, default: str) -> str:
    return os.getenv(name, default)

def main():
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(
        description="Generate historical website traffic reports from HDFS Parquet data."
    )
    parser.add_argument("--reports-path", default=None,
                        help="Override HDFS_REPORTS_PATH.")
    args = parser.parse_args()
    logging.basicConfig(
        level=env("SPARK_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    from pyspark.sql import SparkSession, functions as F

    raw_path = env("HDFS_RAW_EVENTS_PATH", "file:///tmp/website-traffic/raw/events")
    reports_path = args.reports_path or env(
        "HDFS_REPORTS_PATH", "file:///tmp/website-traffic/reports"
    ).rstrip("/")

    spark = (
        SparkSession.builder
        .appName(env("SPARK_BATCH_APP_NAME", "WebsiteTrafficBatchAnalytics"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
    try:
        LOG.info("Reading raw events from %s", raw_path)
        try:
            events = spark.read.parquet(raw_path)
        except Exception as exc:
            raise RuntimeError(
                f"Could not read raw Parquet events at {raw_path}. "
                "Start the streaming job and generate traffic first."
            ) from exc

        events = events.filter(
            F.col("event_time").isNotNull()
            & F.col("page").isNotNull()
            & F.col("status_code").isNotNull()
        )

        daily = (
            events.groupBy("event_date")
            .agg(
                F.count("*").alias("requests"),
                F.approx_count_distinct("visitor_id").alias("unique_visitors_approx"),
                F.sum(F.when(F.col("status_code").between(400, 499), 1).otherwise(0))
                 .alias("client_errors_4xx"),
                F.sum(F.when(F.col("status_code").between(500, 599), 1).otherwise(0))
                 .alias("server_errors_5xx"),
                F.round(F.avg("response_time_ms"), 2).alias("avg_response_time_ms"),
            )
            .withColumn(
                "error_rate_percent",
                F.round(
                    100.0 * (F.col("client_errors_4xx") + F.col("server_errors_5xx"))
                    / F.col("requests"), 2
                )
            )
            .orderBy("event_date")
        )

        hourly = (
            events.withColumn(
                "hour",
                F.date_format("event_time", "yyyy-MM-dd HH:00:00")
            )
            .groupBy("hour")
            .agg(
                F.count("*").alias("requests"),
                F.round(F.avg("response_time_ms"), 2).alias("avg_response_time_ms"),
            )
            .orderBy("hour")
        )

        # Keep event_date in these reports so the dashboard's date filter applies
        # consistently to page and status breakdowns as well as daily/hourly charts.
        top_pages = (
            events.groupBy("event_date", "page")
            .agg(F.count("*").alias("requests"))
            .orderBy("event_date", F.desc("requests"), F.asc("page"))
        )

        status_distribution = (
            events.groupBy("event_date", "status_code")
            .agg(F.count("*").alias("requests"))
            .orderBy("event_date", "status_code")
        )

        response_time_trend = (
            events.withColumn(
                "hour",
                F.date_format("event_time", "yyyy-MM-dd HH:00:00")
            )
            .groupBy("hour")
            .agg(
                F.round(F.avg("response_time_ms"), 2).alias("avg_response_time_ms"),
                F.expr("percentile_approx(response_time_ms, 0.95)").alias("p95_response_time_ms"),
            )
            .orderBy("hour")
        )

        reports = {
            "daily_summary": daily,
            "hourly_summary": hourly,
            "top_pages": top_pages,
            "status_distribution": status_distribution,
            "response_time_trend": response_time_trend,
        }
        for name, dataframe in reports.items():
            destination = f"{reports_path}/{name}"
            LOG.info("Writing %s to %s", name, destination)
            dataframe.coalesce(1).write.mode("overwrite").json(destination)

        run_info = spark.createDataFrame([{
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_path": raw_path,
            "reports_path": reports_path,
            "report_names": ",".join(reports.keys()),
        }])
        run_info.coalesce(1).write.mode("overwrite").json(
            f"{reports_path}/run_info"
        )
        LOG.info("Batch reports completed successfully.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
