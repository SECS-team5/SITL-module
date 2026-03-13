import json
import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import contracts  # type: ignore  # noqa: E402
import verifier  # type: ignore  # noqa: E402


COMMAND_TOPIC = "sitl/commands"
HOME_TOPIC = "sitl-drone-home"


def test_parse_json_payload_accepts_dict_bytes_and_string() -> None:
    payload = {"drone_id": "drone_001"}
    assert contracts.parse_json_payload(payload) is payload
    assert contracts.parse_json_payload(json.dumps(payload).encode()) == payload
    assert contracts.parse_json_payload(json.dumps(payload)) == payload


def test_process_input_message_accepts_valid_command_message() -> None:
    payload = {
        "drone_id": "drone_001",
        "vx": 3.5,
        "vy": -1.0,
        "vz": 0.5,
        "mag_heading": 90.0,
    }
    ok, verified_message, reason = verifier.process_input_message(
        COMMAND_TOPIC,
        json.dumps(payload).encode(),
        COMMAND_TOPIC,
        HOME_TOPIC,
    )

    assert ok is True
    assert reason == ""
    assert verified_message is not None
    assert verified_message["message_type"] == "COMMAND"
    assert verified_message["data"] == payload
    assert verified_message["input_topic"] == COMMAND_TOPIC
    assert contracts.is_iso_timestamp(verified_message["verified_at"]) is True


def test_process_input_message_accepts_valid_home_message() -> None:
    payload = {
        "drone_id": "drone_010",
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
    }
    ok, verified_message, reason = verifier.process_input_message(
        HOME_TOPIC,
        payload,
        COMMAND_TOPIC,
        HOME_TOPIC,
    )

    assert ok is True
    assert reason == ""
    assert verified_message is not None
    assert verified_message["message_type"] == "HOME"
    assert verified_message["data"] == payload


def test_process_input_message_rejects_missing_required_command_field() -> None:
    payload = {
        "drone_id": "drone_001",
        "vx": 3.5,
        "vy": -1.0,
        "mag_heading": 90.0,
    }
    ok, verified_message, reason = verifier.process_input_message(
        COMMAND_TOPIC,
        payload,
        COMMAND_TOPIC,
        HOME_TOPIC,
    )

    assert ok is False
    assert verified_message is None
    assert "required property" in reason
    assert "vz" in reason


def test_process_input_message_rejects_additional_fields() -> None:
    payload = {
        "drone_id": "drone_001",
        "home_lat": 59.0,
        "home_lon": 30.0,
        "home_alt": 100.0,
        "unexpected": "value",
    }
    ok, verified_message, reason = verifier.process_input_message(
        HOME_TOPIC,
        payload,
        COMMAND_TOPIC,
        HOME_TOPIC,
    )

    assert ok is False
    assert verified_message is None
    assert "Additional properties are not allowed" in reason


def test_process_input_message_rejects_out_of_range_heading() -> None:
    payload = {
        "drone_id": "drone_001",
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "mag_heading": 400.0,
    }
    ok, verified_message, reason = verifier.process_input_message(
        COMMAND_TOPIC,
        payload,
        COMMAND_TOPIC,
        HOME_TOPIC,
    )

    assert ok is False
    assert verified_message is None
    assert "359.9" in reason


def test_process_input_message_rejects_unsupported_topic() -> None:
    ok, verified_message, reason = verifier.process_input_message(
        "sitl/unknown",
        {"drone_id": "drone_001"},
        COMMAND_TOPIC,
        HOME_TOPIC,
    )

    assert ok is False
    assert verified_message is None
    assert "unsupported topic" in reason


def test_validate_verified_message_rejects_message_type_topic_mismatch() -> None:
    verified_message = contracts.build_verified_message(
        COMMAND_TOPIC,
        "HOME",
        {
            "drone_id": "drone_001",
            "vx": 0.0,
            "vy": 0.0,
            "vz": 0.0,
            "mag_heading": 0.0,
        },
    )
    ok, reason = contracts.validate_verified_message(
        verified_message,
        COMMAND_TOPIC,
        HOME_TOPIC,
    )

    assert ok is False
    assert "does not match input topic" in reason
