#!/usr/bin/env python3
"""Generate repeatable synthetic website traffic and publish it to Kafka."""
from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
import time

from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from common.event_schema import PAGES, make_event
from kafka_client.producer import TrafficKafkaProducer

LOG = logging.getLogger("traffic-generator")
RUNNING = True

def stop(_signum, _frame):
    global RUNNING
    RUNNING = False

def build_event(rng: random.Random, spike: bool = False) -> dict:
    """Create one plausible synthetic web request event."""
    status_roll = rng.random()
    if spike:
        # Spikes contain a slightly larger proportion of server errors for demo visibility.
        status = 500 if status_roll < 0.08 else 404 if status_roll < 0.18 else 200
    else:
        status = 500 if status_roll < 0.02 else 404 if status_roll < 0.08 else 200

    response_time = (
        rng.randint(250, 1800) if status == 500
        else rng.randint(80, 900) if spike
        else rng.randint(20, 500)
    )
    visitor_number = rng.randint(1, 250)
    return make_event(
        visitor_id=f"visitor_{visitor_number:04d}",
        ip=f"192.168.{rng.randint(0, 3)}.{rng.randint(2, 254)}",
        page=rng.choice(PAGES),
        status_code=status,
        response_time_ms=response_time,
    )

def parse_args():
    parser = argparse.ArgumentParser(description="Publish synthetic website traffic to Kafka.")
    parser.add_argument("--rate", type=float, default=float(os.getenv("GENERATOR_RATE", "5")),
                        help="Normal events per second (default: 5).")
    parser.add_argument("--spike-after-seconds", type=float, default=0,
                        help="Start one traffic spike this many seconds after launch; 0 disables it.")
    parser.add_argument("--spike-duration-seconds", type=float, default=20,
                        help="Duration of the spike (default: 20).")
    parser.add_argument("--spike-rate", type=float, default=40,
                        help="Events per second during the spike (default: 40).")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Stop after this many events; 0 means run until Ctrl+C.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Optional random seed for repeatable page/status selection.")
    parser.add_argument("--bootstrap-servers", default=None,
                        help="Override KAFKA_BOOTSTRAP_SERVERS.")
    parser.add_argument("--topic", default=None, help="Override KAFKA_TOPIC.")
    return parser.parse_args()

def main():
    load_dotenv(os.path.join(ROOT, ".env"))
    args = parse_args()
    if args.rate <= 0 or args.spike_rate <= 0 or args.spike_duration_seconds < 0:
        raise SystemExit("Rates must be positive and spike duration cannot be negative.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
    LOG.info("Publishing to %s at %s; normal rate %.1f events/s",
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
            event = build_event(rng, spike=in_spike)
            try:
                producer.publish(event)
                sent += 1
            except Exception as exc:
                failed += 1
                LOG.error("Publish failed (%s); continuing", exc)

            now = time.monotonic()
            if now - last_report >= 5:
                LOG.info("sent=%d failed=%d mode=%s rate=%.1f/s",
                         sent, failed, "SPIKE" if in_spike else "normal", current_rate)
                last_report = now
            time.sleep(max(0.0, 1.0 / current_rate))
    finally:
        producer.close()
        LOG.info("Stopped. acknowledged=%d failed=%d", sent, failed)

if __name__ == "__main__":
    main()
