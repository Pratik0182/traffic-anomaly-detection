#!/usr/bin/env python3
"""Generate realistic synthetic website traffic events and publish to Kafka."""
from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
import time

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = lambda *args, **kwargs: None

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from common.event_schema import PAGES, USER_AGENTS, make_event
from kafka_client.producer import TrafficKafkaProducer

LOG = logging.getLogger("traffic-generator")
RUNNING = True

# Track active sessions to simulate realistic user browsing behavior
ACTIVE_SESSIONS: dict[str, dict] = {}

def stop(_signum, _frame):
    global RUNNING
    RUNNING = False

def get_or_create_session(rng: random.Random) -> dict:
    """Simulate realistic user sessions where visitors browse multiple pages."""
    # Keep active session pool around 30 concurrent users
    if len(ACTIVE_SESSIONS) > 40:
        oldest = min(ACTIVE_SESSIONS.keys(), key=lambda k: ACTIVE_SESSIONS[k]["last_active"])
        del ACTIVE_SESSIONS[oldest]

    # 70% chance to reuse an existing visitor session (repeated page views)
    if ACTIVE_SESSIONS and rng.random() < 0.70:
        vid = rng.choice(list(ACTIVE_SESSIONS.keys()))
        sess = ACTIVE_SESSIONS[vid]
        sess["last_active"] = time.time()
        sess["page_views"] += 1
        return sess

    # Otherwise create a new visitor
    vid_num = rng.randint(1, 9999)
    vid = f"visitor_{vid_num:04d}"
    ip = f"192.168.{rng.randint(1, 10)}.{rng.randint(2, 254)}"
    ua = rng.choice(USER_AGENTS)
    sess = {
        "visitor_id": vid,
        "ip": ip,
        "user_agent": ua,
        "last_active": time.time(),
        "page_views": 1,
    }
    ACTIVE_SESSIONS[vid] = sess
    return sess

def build_event(
    rng: random.Random,
    spike: bool = False,
    error_boost: bool = False,
    slow_boost: bool = False,
) -> dict:
    """Create one realistic synthetic web request event."""
    session = get_or_create_session(rng)
    page = rng.choice(PAGES)

    # Determine request method based on page
    if page in {"/login", "/register", "/checkout", "/api/orders"}:
        method = "POST" if rng.random() < 0.75 else "GET"
    elif page == "/cart":
        method = "POST" if rng.random() < 0.40 else "GET"
    else:
        method = "GET"

    # Status code distribution
    status_roll = rng.random()
    if error_boost:
        # High error scenario for anomaly demonstration (30% errors)
        status = 500 if status_roll < 0.20 else 404 if status_roll < 0.40 else 200
    elif spike:
        # Spikes slightly elevate server load / errors
        status = 500 if status_roll < 0.08 else 404 if status_roll < 0.16 else 200
    else:
        # Normal operations: ~95% success, 3% 404, 2% 500
        status = 500 if status_roll < 0.02 else 404 if status_roll < 0.05 else 200

    # Response time in milliseconds
    if slow_boost:
        # Slow response anomaly injection
        response_time = rng.randint(800, 2500)
    elif status == 500:
        response_time = rng.randint(300, 1500)
    elif spike:
        response_time = rng.randint(80, 950)
    else:
        response_time = rng.randint(25, 380)

    return make_event(
        visitor_id=session["visitor_id"],
        ip=session["ip"],
        page=page,
        status_code=status,
        response_time_ms=response_time,
        request_method=method,
        user_agent=session["user_agent"],
    )

def parse_args():
    parser = argparse.ArgumentParser(description="Publish synthetic website traffic to Kafka.")
    parser.add_argument("--rate", type=float, default=float(os.getenv("GENERATOR_RATE", "5")),
                        help="Normal events per second (default: 5).")
    parser.add_argument("--spike-after-seconds", type=float, default=0,
                        help="Start one traffic spike this many seconds after launch; 0 disables it.")
    parser.add_argument("--spike-duration-seconds", type=float, default=20,
                        help="Duration of the spike in seconds (default: 20).")
    parser.add_argument("--spike-rate", type=float, default=40,
                        help="Events per second during spike (default: 40).")
    parser.add_argument("--inject-errors", action="store_true",
                        help="Simulate an error rate surge (for anomaly testing).")
    parser.add_argument("--slow-responses", action="store_true",
                        help="Simulate high server latency (for latency anomaly testing).")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Stop after this many events; 0 means run continuously.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducible traffic generation.")
    parser.add_argument("--bootstrap-servers", default=None,
                        help="Override KAFKA_BOOTSTRAP_SERVERS.")
    parser.add_argument("--topic", default=None, help="Override KAFKA_TOPIC.")
    return parser.parse_args()

def main():
    load_dotenv(os.path.join(ROOT, ".env"))
    args = parse_args()
    if args.rate <= 0 or args.spike_rate <= 0 or args.spike_duration_seconds < 0:
        raise SystemExit("Rates must be positive and spike duration cannot be negative.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    rng = random.Random(args.seed)
    producer = TrafficKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )
    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)

    started = time.monotonic()
    sent = 0
    failed = 0
    last_report = started
    LOG.info("Publishing to Kafka topic '%s' at %s (base rate: %.1f events/s)",
             producer.topic, producer.bootstrap_servers, args.rate)
    try:
        while RUNNING and (args.max_events == 0 or sent + failed < args.max_events):
            elapsed = time.monotonic() - started
            in_spike = (
                args.spike_after_seconds > 0
                and args.spike_after_seconds <= elapsed
                < args.spike_after_seconds + args.spike_duration_seconds
            )
            current_rate = args.spike_rate if in_spike else args.rate
            event = build_event(
                rng,
                spike=in_spike,
                error_boost=args.inject_errors,
                slow_boost=args.slow_responses,
            )
            try:
                producer.publish(event)
                sent += 1
            except Exception as exc:
                failed += 1
                LOG.error("Failed to publish event to Kafka: %s", exc)

            now = time.monotonic()
            if now - last_report >= 5:
                mode_desc = "SPIKE" if in_spike else ("ERR_BOOST" if args.inject_errors else "NORMAL")
                LOG.info("Status: sent=%d failed=%d mode=%s rate=%.1f/s",
                         sent, failed, mode_desc, current_rate)
                last_report = now
            time.sleep(max(0.0, 1.0 / current_rate))
    finally:
        producer.close()
        LOG.info("Generator stopped. Total sent=%d failed=%d", sent, failed)

if __name__ == "__main__":
    main()
