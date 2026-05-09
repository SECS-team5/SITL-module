import pathlib
import sys
import json
from uuid import UUID
import pytest
from shared import contracts  # noqa: E402

def test_parse_json_payload_invalid_bytes_and_string():
    assert contracts.parse_json_payload(b"\xff\xfe") is None  # UnicodeDecodeError
    assert contracts.parse_json_payload("not json") is None   # JSONDecodeError
    assert contracts.parse_json_payload("[1,2]") is None      # not a dict

def test_classify_input_topic_unsupported():
    ok, msg_type, schema = contracts.classify_input_topic("unknown", "cmd", "home")
    assert ok is False
    assert msg_type == ""
    assert "unsupported topic" in schema

def test_resolve_verified_topic_raises_on_unknown():
    with pytest.raises(ValueError, match="unsupported message_type"):
        contracts.resolve_verified_topic("UNKNOWN", "v_cmd", "v_home")

def test_build_verified_message_structure():
    payload = {"drone_id": "d1"}
    msg = contracts.build_verified_message("in.topic", "COMMAND", payload)
    assert msg["data"] == payload
    assert msg["message_type"] == "COMMAND"
    assert msg["verifier_stage"] == contracts.VERIFIER_STAGE
    assert msg["input_topic"] == "in.topic"
    assert msg["verified_at"].endswith("Z")

def test_decode_headers_and_build_request_headers():
    raw = [("cid", b"123"), ("reply", b"topic")]
    decoded = contracts.decode_headers(raw)
    assert decoded == {"cid": "123", "reply": "topic"}

    headers = contracts.build_request_headers("cid", "reply")
    assert headers[0][1] == b"cid"
    assert headers[1][1] == b"reply"

def test_generate_correlation_id_is_valid_uuid():
    cid = contracts.generate_correlation_id()
    UUID(cid, version=4)  # не упадёт, если валидный hex uuid

def test_get_transport_value_priority():
    payload = {"field": "from_payload"}
    headers = {"field": "from_header"}
    assert contracts.get_transport_value(payload, headers, "field") == "from_header"
    assert contracts.get_transport_value(payload, {}, "field") == "from_payload"
    assert contracts.get_transport_value({}, {}, "field") == ""

def test_is_iso_timestamp():
    assert contracts.is_iso_timestamp("2023-01-01T12:00:00Z") is True
    assert contracts.is_iso_timestamp("2023-01-01T12:00:00") is True
    assert contracts.is_iso_timestamp("") is False
    assert contracts.is_iso_timestamp("not-date") is False
    assert contracts.is_iso_timestamp(None) is False

@pytest.mark.parametrize("missing_field", ["data", "input_topic", "message_type", "verified_at", "verifier_stage"])
def test_validate_verified_message_missing_field(missing_field):
    base = {
        "data": {"id": "1"}, "input_topic": "cmd", "message_type": "COMMAND",
        "verified_at": "2023-01-01T12:00:00Z", "verifier_stage": contracts.VERIFIER_STAGE
    }
    del base[missing_field]
    ok, reason = contracts.validate_verified_message(base, "cmd", "home")
    assert ok is False
    assert f"missing field '{missing_field}'" in reason

def test_validate_verified_message_wrong_stage_and_bad_data():
    msg = {
        "data": "not_dict", "input_topic": "cmd", "message_type": "COMMAND",
        "verified_at": "2023-01-01T12:00:00Z", "verifier_stage": "WRONG"
    }
    ok, reason = contracts.validate_verified_message(msg, "cmd", "home")
    assert ok is False
    assert "unexpected verifier_stage" in reason or "must be an object" in reason

def test_validate_verified_message_timestamp_and_type_mismatch(monkeypatch):
    monkeypatch.setattr(contracts, "is_iso_timestamp", lambda x: False)
    msg = {
        "data": {"id": "1"}, "input_topic": "cmd", "message_type": "COMMAND",
        "verified_at": "bad", "verifier_stage": contracts.VERIFIER_STAGE
    }
    ok, reason = contracts.validate_verified_message(msg, "cmd", "home")
    assert ok is False
    assert "ISO-8601" in reason

    # Проверка несоответствия типа
    monkeypatch.setattr(contracts, "is_iso_timestamp", lambda x: True)
    monkeypatch.setattr(contracts, "validate_schema", lambda p, s: (True, ""))
    msg["message_type"] = "HOME"
    ok, reason = contracts.validate_verified_message(msg, "cmd", "home")
    assert ok is False
    assert "does not match" in reason