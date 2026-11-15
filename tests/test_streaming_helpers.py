from datetime import datetime

import spark.streaming_processor as processor

class FakeBatch:
    def __init__(self, rows):
        self._rows = rows
    def collect(self):
        return self._rows

def sample_summary(requests=750):
    return {
        "window_start": datetime(2026, 11, 14, 12, 0, 0),
        "window_end": datetime(2026, 11, 14, 12, 1, 0),
        "requests": requests,
        "unique_visitors_approx": 123,
        "client_errors_4xx": 20,
        "server_errors_5xx": 5,
        "avg_response_time_ms": 145.75,
    }

def test_timestamp_text_normalizes_naive_datetime_to_utc():
    assert processor.timestamp_text(datetime(2026, 11, 14, 12, 0, 0)) == (
        "2026-11-14T12:00:00Z"
    )

def test_summary_batch_writes_absolute_metric_and_spike_alert(monkeypatch):
    captured = {}
    def fake_metrics(records):
        captured["metrics"] = list(records)
        return len(captured["metrics"])

    def fake_alerts(records):
        captured["alerts"] = list(records)
        return len(captured["alerts"])

    monkeypatch.setattr(processor, "upsert_metrics", fake_metrics)
    monkeypatch.setattr(processor, "upsert_alerts", fake_alerts)

    processor.write_summary_batch(FakeBatch([sample_summary(750)]), 17, 600)

    metric = captured["metrics"][0]
    alert = captured["alerts"][0]
    assert metric["metric_type"] == "summary"
    assert metric["payload"]["requests"] == 750
    assert metric["payload"]["is_spike"] is True
    assert metric["payload"]["last_batch_id"] == 17
    assert alert["alert_type"] == "TRAFFIC_SPIKE"
    assert alert["threshold"] == 600

def test_summary_batch_does_not_alert_below_threshold(monkeypatch):
    captured = {}
    def fake_metrics(records):
        captured["metrics"] = list(records)
        return len(captured["metrics"])

    def fake_alerts(records):
        captured["alerts"] = list(records)
        return len(captured["alerts"])

    monkeypatch.setattr(processor, "upsert_metrics", fake_metrics)
    monkeypatch.setattr(processor, "upsert_alerts", fake_alerts)

    processor.write_summary_batch(FakeBatch([sample_summary(250)]), 18, 600)

    assert captured["metrics"][0]["payload"]["is_spike"] is False
    assert captured["alerts"] == []
