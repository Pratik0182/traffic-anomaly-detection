"""Kafka producer wrapper for website traffic JSON events."""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

LOG = logging.getLogger(__name__)

class TrafficKafkaProducer:
    """Small producer wrapper; Kafka's own retry/reconnect settings do most retry work."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        client_id: str | None = None,
        connect_attempts: int = 5,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("KAFKA_TOPIC", "website_traffic")
        self.client_id = client_id or os.getenv(
            "KAFKA_CLIENT_ID", "website-traffic-generator"
        )
        self.producer = self._connect(connect_attempts)

    def _connect(self, attempts: int) -> KafkaProducer:
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return KafkaProducer(
                    bootstrap_servers=[s.strip() for s in self.bootstrap_servers.split(",")],
                    client_id=self.client_id,
                    acks="all",
                    retries=10,
                    max_in_flight_requests_per_connection=1,
                    retry_backoff_ms=500,
                    request_timeout_ms=30000,
                    delivery_timeout_ms=120000,
                    value_serializer=lambda value: json.dumps(
                        value, separators=(",", ":"), ensure_ascii=False
                    ).encode("utf-8"),
                )
            except NoBrokersAvailable as exc:
                last_error = exc
                LOG.warning("Kafka unavailable (attempt %s/%s)", attempt, attempts)
                if attempt < attempts:
                    time.sleep(min(2 ** (attempt - 1), 8))
        raise ConnectionError(
            f"Could not connect to Kafka at {self.bootstrap_servers}"
        ) from last_error

    def publish(self, event: dict[str, Any], timeout: float = 15.0) -> None:
        """Publish and wait for broker acknowledgement; raises on failure."""
        try:
            self.producer.send(self.topic, value=event).get(timeout=timeout)
        except KafkaError:
            LOG.exception("Kafka publish failed")
            raise

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
        self.producer.close(timeout=10)
