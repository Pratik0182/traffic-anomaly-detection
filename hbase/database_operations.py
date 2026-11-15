"""HBase Thrift operations for the traffic dashboard.

Spark calls these helpers from foreachBatch. HappyBase is imported lazily so the
pure row-key helpers can be tested without a running HBase cluster.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

def metrics_table_name() -> str:
    return os.getenv("HBASE_METRICS_TABLE", "traffic_metrics")

def alerts_table_name() -> str:
    return os.getenv("HBASE_ALERTS_TABLE", "traffic_spike_alerts")

def _parse_datetime(value: Any) -> datetime:
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def window_key(value: Any) -> str:
    """Stable UTC timestamp component used in HBase row keys."""
    return _parse_datetime(value).strftime("%Y%m%dT%H%M%S")

def metric_row_key(metric_type: str, window_start: Any,
                    dimension: str | None = None) -> bytes:
    """Deterministic key: a retried Spark batch overwrites, rather than duplicates."""
    if metric_type not in {"summary", "page", "status"}:
        raise ValueError("metric_type must be summary, page, or status")
    key = f"{metric_type}#{window_key(window_start)}"
    if dimension is not None:
        key += f"#{quote(str(dimension), safe='')}"
    return key.encode("utf-8")

def alert_row_key(window_start: Any, alert_type: str = "spike") -> bytes:
    prefix = "spike" if alert_type.lower() in ("spike", "traffic_spike") else f"alert_{alert_type.lower()}"
    return f"{prefix}#{window_key(window_start)}".encode("utf-8")

def _connection():
    try:
        import happybase
    except ImportError as exc:
        raise RuntimeError(
            "HappyBase is missing. Install project dependencies with pip install -r requirements.txt"
        ) from exc
    return happybase.Connection(
        host=os.getenv("HBASE_THRIFT_HOST", "localhost"),
        port=int(os.getenv("HBASE_THRIFT_PORT", "9090")),
        timeout=int(os.getenv("HBASE_THRIFT_TIMEOUT_MS", "10000")),
        autoconnect=True,
    )

def ensure_tables(connection=None) -> None:
    owns_connection = connection is None
    conn = connection or _connection()
    try:
        existing = {
            item.decode() if isinstance(item, bytes) else str(item)
            for item in conn.tables()
        }
        if metrics_table_name() not in existing:
            conn.create_table(metrics_table_name(), {"m": dict()})
        if alerts_table_name() not in existing:
            conn.create_table(alerts_table_name(), {"a": dict()})
    finally:
        if owns_connection:
            conn.close()

def _encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      default=str).encode("utf-8")

def upsert_metrics(records: Iterable[dict[str, Any]]) -> int:
    """Write absolute metric values with PUT semantics; safe for batch retries."""
    rows = list(records)
    if not rows:
        return 0
    conn = _connection()
    try:
        ensure_tables(conn)
        table = conn.table(metrics_table_name())
        with table.batch(batch_size=200) as batch:
            for item in rows:
                key = metric_row_key(
                    item["metric_type"], item["window_start"], item.get("dimension")
                )
                batch.put(key, {b"m:payload": _encode(item["payload"])})
        return len(rows)
    finally:
        conn.close()

def upsert_alerts(alerts: Iterable[dict[str, Any]]) -> int:
    rows = list(alerts)
    if not rows:
        return 0
    conn = _connection()
    try:
        ensure_tables(conn)
        table = conn.table(alerts_table_name())
        with table.batch(batch_size=200) as batch:
            for alert in rows:
                key = alert_row_key(alert["window_start"], alert.get("alert_type", "spike"))
                batch.put(key, {b"a:payload": _encode(alert)})
        return len(rows)
    finally:
        conn.close()

def _payload(row, family: bytes):
    raw = row[1].get(family + b":payload")
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

def read_metrics(metric_type: str | None = None, limit: int = 300):
    conn = _connection()
    try:
        ensure_tables(conn)
        table = conn.table(metrics_table_name())
        prefix = f"{metric_type}#".encode() if metric_type else None
        values = []
        for row in table.scan(row_prefix=prefix):
            value = _payload(row, b"m")
            if value is not None:
                value["_row_key"] = row[0].decode("utf-8", errors="replace")
                values.append(value)
        values.sort(key=lambda item: item.get("window_start", ""))
        return values[-max(1, int(limit)):]
    finally:
        conn.close()

def read_alerts(limit: int = 100):
    conn = _connection()
    try:
        ensure_tables(conn)
        table = conn.table(alerts_table_name())
        values = []
        for row in table.scan():
            value = _payload(row, b"a")
            if value is not None:
                value["_row_key"] = row[0].decode("utf-8", errors="replace")
                values.append(value)
        values.sort(key=lambda item: item.get("window_start", ""))
        return values[-max(1, int(limit)):][::-1]
    finally:
        conn.close()

def health_check():
    conn = _connection()
    try:
        ensure_tables(conn)
        tables = sorted(
            item.decode() if isinstance(item, bytes) else str(item)
            for item in conn.tables()
        )
        return {
            "connected": True,
            "host": os.getenv("HBASE_THRIFT_HOST", "localhost"),
            "port": int(os.getenv("HBASE_THRIFT_PORT", "9090")),
            "tables": tables,
        }
    finally:
        conn.close()

def main():
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    parser = argparse.ArgumentParser(description="HBase helpers for website traffic analytics.")
    parser.add_argument("action", choices=["init", "check", "scan"])
    parser.add_argument("--type", choices=["summary", "page", "status"], default="summary")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    if args.action == "init":
        ensure_tables()
        print(f"Created/verified {metrics_table_name()} and {alerts_table_name()}")
    elif args.action == "check":
        print(json.dumps(health_check(), indent=2))
    else:
        print(json.dumps(read_metrics(args.type, args.limit), indent=2, default=str))

if __name__ == "__main__":
    main()
