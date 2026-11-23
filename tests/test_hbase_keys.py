from hbase.database_operations import metric_row_key, alert_row_key, window_key

def test_window_key_normalizes_utc():
    assert window_key("2026-11-14T12:00:00Z") == "20261114T120000"

def test_metric_keys_are_deterministic_and_dimension_safe():
    a = metric_row_key("page", "2026-11-14T12:00:00Z", "/products/item-101")
    b = metric_row_key("page", "2026-11-14T12:00:00+00:00", "/products/item-101")
    assert a == b
    assert a.startswith(b"page#20261114T120000#")
    assert b"/" not in a.split(b"#")[-1]

def test_alert_key_is_deterministic():
    assert alert_row_key("2026-11-14T12:00:00Z") == b"spike#20261114T120000"

def test_invalid_metric_type_rejected():
    try:
        metric_row_key("unknown", "2026-11-14T12:00:00Z")
        assert False, "Expected ValueError"
    except ValueError:
        pass
