"""Shared website traffic event schema and validation helpers."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import uuid

PAGES = [
    "/", "/login", "/register", "/products", "/products/item-101",
    "/products/item-202", "/search", "/cart", "/checkout",
    "/about", "/contact", "/api/products", "/api/orders",
]

def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp ending in Z."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def make_event(
    *,
    visitor_id: str,
    ip: str,
    page: str,
    status_code: int,
    response_time_ms: int,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Build one event using the stable project event contract."""
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": timestamp or utc_now_iso(),
        "visitor_id": visitor_id,
        "ip": ip,
        "page": page,
        "status_code": int(status_code),
        "response_time_ms": int(response_time_ms),
    }
    validate_event(event)
    return event

def validate_event(event: dict[str, Any]) -> None:
    required = {
        "event_id", "timestamp", "visitor_id", "ip", "page",
        "status_code", "response_time_ms",
    }
    missing = required.difference(event)
    if missing:
        raise ValueError(f"Missing event fields: {sorted(missing)}")
    if not isinstance(event["status_code"], int) or not 100 <= event["status_code"] <= 599:
        raise ValueError("status_code must be an HTTP status integer (100-599)")
    if not isinstance(event["response_time_ms"], int) or event["response_time_ms"] < 0:
        raise ValueError("response_time_ms must be a non-negative integer")
    if not str(event["visitor_id"]).strip() or not str(event["page"]).startswith("/"):
        raise ValueError("visitor_id must be non-empty and page must start with '/'")
    try:
        datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be ISO-8601") from exc
