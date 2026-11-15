from spark.anomaly_detector import AnomalyDetector

def test_anomaly_detector_traffic_spike():
    detector = AnomalyDetector(spike_threshold=600)
    alerts = detector.evaluate_window(
        window_start="2026-11-15T10:00:00Z",
        window_end="2026-11-15T10:01:00Z",
        requests=800,
        client_errors_4xx=5,
        server_errors_5xx=2,
        avg_response_time_ms=120.0,
        unique_visitors=250,
    )
    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "TRAFFIC_SPIKE"
    assert alerts[0]["severity"] == "WARNING"
    assert alerts[0]["observed_value"] == 800.0

def test_anomaly_detector_traffic_spike_critical():
    detector = AnomalyDetector(spike_threshold=600)
    alerts = detector.evaluate_window(
        window_start="2026-11-15T10:00:00Z",
        window_end="2026-11-15T10:01:00Z",
        requests=1500,
        client_errors_4xx=10,
        server_errors_5xx=5,
        avg_response_time_ms=150.0,
        unique_visitors=500,
    )
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "CRITICAL"

def test_anomaly_detector_error_rate_surge():
    detector = AnomalyDetector(error_rate_threshold=15.0)
    alerts = detector.evaluate_window(
        window_start="2026-11-15T10:00:00Z",
        window_end="2026-11-15T10:01:00Z",
        requests=100,
        client_errors_4xx=18,
        server_errors_5xx=2,
        avg_response_time_ms=110.0,
        unique_visitors=50,
    )
    assert any(a["alert_type"] == "HIGH_ERROR_RATE" for a in alerts)
    err_alert = next(a for a in alerts if a["alert_type"] == "HIGH_ERROR_RATE")
    assert err_alert["observed_value"] == 20.0

def test_anomaly_detector_latency_spike():
    detector = AnomalyDetector(latency_threshold_ms=500.0)
    alerts = detector.evaluate_window(
        window_start="2026-11-15T10:00:00Z",
        window_end="2026-11-15T10:01:00Z",
        requests=50,
        client_errors_4xx=1,
        server_errors_5xx=0,
        avg_response_time_ms=750.0,
        unique_visitors=25,
    )
    assert any(a["alert_type"] == "HIGH_LATENCY" for a in alerts)
    lat_alert = next(a for a in alerts if a["alert_type"] == "HIGH_LATENCY")
    assert lat_alert["observed_value"] == 750.0

def test_anomaly_detector_normal_traffic_no_alerts():
    detector = AnomalyDetector(spike_threshold=600, error_rate_threshold=15.0, latency_threshold_ms=500.0)
    alerts = detector.evaluate_window(
        window_start="2026-11-15T10:00:00Z",
        window_end="2026-11-15T10:01:00Z",
        requests=300,
        client_errors_4xx=3,
        server_errors_5xx=1,
        avg_response_time_ms=120.0,
        unique_visitors=150,
    )
    assert len(alerts) == 0
