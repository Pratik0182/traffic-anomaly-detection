"""Statistical and threshold-based anomaly detection for website traffic metrics."""
from __future__ import annotations

import logging
import os
from typing import Any

LOG = logging.getLogger("traffic-anomaly-detector")

# Default thresholds (can be overridden via environment variables)
DEFAULT_SPIKE_THRESHOLD = 600  # requests / minute
DEFAULT_ERROR_RATE_THRESHOLD = 15.0  # percentage of 4xx + 5xx
DEFAULT_LATENCY_THRESHOLD_MS = 600.0  # average response time in ms

class AnomalyDetector:
    """Evaluates windowed traffic aggregates against statistical and threshold rules."""

    def __init__(
        self,
        spike_threshold: int | None = None,
        error_rate_threshold: float | None = None,
        latency_threshold_ms: float | None = None,
    ):
        self.spike_threshold = (
            spike_threshold
            if spike_threshold is not None
            else int(os.getenv("SPIKE_THRESHOLD_REQUESTS_PER_MINUTE", str(DEFAULT_SPIKE_THRESHOLD)))
        )
        self.error_rate_threshold = (
            error_rate_threshold
            if error_rate_threshold is not None
            else float(os.getenv("ERROR_RATE_THRESHOLD_PERCENT", str(DEFAULT_ERROR_RATE_THRESHOLD)))
        )
        self.latency_threshold_ms = (
            latency_threshold_ms
            if latency_threshold_ms is not None
            else float(os.getenv("LATENCY_THRESHOLD_MS", str(DEFAULT_LATENCY_THRESHOLD_MS)))
        )

    def evaluate_window(
        self,
        *,
        window_start: str,
        window_end: str,
        requests: int,
        client_errors_4xx: int = 0,
        server_errors_5xx: int = 0,
        avg_response_time_ms: float = 0.0,
        unique_visitors: int = 0,
    ) -> list[dict[str, Any]]:
        """Evaluate a single 1-minute aggregation window and return generated alerts."""
        alerts: list[dict[str, Any]] = []
        total_errors = client_errors_4xx + server_errors_5xx
        error_rate = (100.0 * total_errors / requests) if requests > 0 else 0.0

        # 1. Traffic Spike Detection
        if requests >= self.spike_threshold:
            severity = "CRITICAL" if requests >= (self.spike_threshold * 2) else "WARNING"
            alerts.append({
                "window_start": window_start,
                "window_end": window_end,
                "alert_type": "TRAFFIC_SPIKE",
                "severity": severity,
                "observed_value": float(requests),
                "expected_threshold": float(self.spike_threshold),
                "requests": requests,
                "threshold": self.spike_threshold,
                "unique_visitors_approx": unique_visitors,
                "reason": (
                    f"Surge of {requests} requests/min exceeds threshold of {self.spike_threshold} "
                    f"({round(requests / self.spike_threshold, 1)}x baseline)"
                ),
            })

        # 2. Abnormal Error Rate Surge (4xx + 5xx)
        if requests >= 20 and error_rate >= self.error_rate_threshold:
            severity = "CRITICAL" if error_rate >= 30.0 or server_errors_5xx > client_errors_4xx else "WARNING"
            alerts.append({
                "window_start": window_start,
                "window_end": window_end,
                "alert_type": "HIGH_ERROR_RATE",
                "severity": severity,
                "observed_value": round(error_rate, 2),
                "expected_threshold": self.error_rate_threshold,
                "requests": requests,
                "threshold": int(self.error_rate_threshold),
                "unique_visitors_approx": unique_visitors,
                "reason": (
                    f"Elevated error rate of {error_rate:.1f}% ({total_errors}/{requests} requests; "
                    f"4xx: {client_errors_4xx}, 5xx: {server_errors_5xx}) exceeds threshold {self.error_rate_threshold}%"
                ),
            })

        # 3. High Latency Anomaly
        if requests >= 10 and avg_response_time_ms >= self.latency_threshold_ms:
            severity = "CRITICAL" if avg_response_time_ms >= (self.latency_threshold_ms * 1.5) else "WARNING"
            alerts.append({
                "window_start": window_start,
                "window_end": window_end,
                "alert_type": "HIGH_LATENCY",
                "severity": severity,
                "observed_value": round(avg_response_time_ms, 2),
                "expected_threshold": self.latency_threshold_ms,
                "requests": requests,
                "threshold": int(self.latency_threshold_ms),
                "unique_visitors_approx": unique_visitors,
                "reason": (
                    f"Average response time {avg_response_time_ms:.1f} ms exceeds latency threshold "
                    f"of {self.latency_threshold_ms:.1f} ms"
                ),
            })

        return alerts
