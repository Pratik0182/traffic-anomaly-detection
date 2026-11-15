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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
]

REQUEST_METHODS = ["GET", "POST", "PUT", "DELETE", "HEAD"]

def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp ending in Z."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def make_event(
    *,
    visitor_id: str,
    ip: str,
    page: str,
    status_code: int,
    response_time_ms: int | None = None,
    response_time: int | None = None,
    request_method: str = "GET",
    user_agent: str | None = None,
    timestamp: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    """Build one event using the stable project event contract."""
    rt = response_time_ms if response_time_ms is not None else (response_time if response_time is not None else 100)
    event = {
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": timestamp or utc_now_iso(),
        "visitor_id": visitor_id,
        "ip": ip,
        "page": page,
        "status_code": int(status_code),
        "response_time_ms": int(rt),
        "response_time": int(rt),
        "request_method": request_method.upper(),
        "user_agent": user_agent or USER_AGENTS[0],
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
    if "request_method" in event and event["request_method"] not in REQUEST_METHODS:
        raise ValueError(f"Invalid request_method: {event['request_method']}")
    try:
        datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be ISO-8601") from exc
