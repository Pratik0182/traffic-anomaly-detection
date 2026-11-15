from common.event_schema import make_event, validate_event
from tests.data_generator_test_helper import build_sample_event

def test_make_event_has_required_fields():
    event = make_event(
        visitor_id="visitor_0001", ip="192.168.1.10", page="/home",
        status_code=200, response_time_ms=120,
        request_method="POST",
        timestamp="2026-11-14T00:00:00Z",
    )
    assert event["page"] == "/home"
    assert event["status_code"] == 200
    assert event["request_method"] == "POST"
    assert "user_agent" in event
    assert event["response_time"] == 120
    validate_event(event)

def test_make_event_with_response_time_alias():
    event = make_event(
        visitor_id="visitor_0002", ip="192.168.1.11", page="/login",
        status_code=200, response_time=150,
    )
    assert event["response_time_ms"] == 150
    assert event["response_time"] == 150
    validate_event(event)

def test_invalid_request_method_rejected():
    event = build_sample_event()
    event["request_method"] = "INVALID_METHOD"
    try:
        validate_event(event)
        assert False, "Expected ValueError"
    except ValueError:
        pass

def test_invalid_status_is_rejected():
    event = build_sample_event()
    event["status_code"] = 99
    try:
        validate_event(event)
        assert False, "Expected ValueError"
    except ValueError:
        pass

def test_negative_response_time_is_rejected():
    event = build_sample_event()
    event["response_time_ms"] = -1
    try:
        validate_event(event)
        assert False, "Expected ValueError"
    except ValueError:
        pass
