import json
import pathlib
import sys
from unittest.mock import MagicMock


ROOT = pathlib.Path(__file__).resolve().parents[2]  # new-SITL/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared import contracts  # noqa: E402


COMMAND_TOPIC = "sitl.commands"
HOME_TOPIC = "sitl-drone-home"
VERIFIED_COMMAND_TOPIC = "sitl.verified-commands"
VERIFIED_HOME_TOPIC = "sitl.verified-home"


def test_parse_json_payload_accepts_dict_bytes_and_string() -> None:
    payload = {"drone_id": "drone_001"}
    assert contracts.parse_json_payload(payload) is payload
    assert contracts.parse_json_payload(json.dumps(payload).encode()) == payload
    assert contracts.parse_json_payload(json.dumps(payload)) == payload


def test_process_input_message_accepts_valid_command_message() -> None:
    from components.sitl_verifier.src.sitl_verifier import SitlVerifierComponent

    payload = {
        "drone_id": "drone_001",
        "vx": 3.5,
        "vy": -1.0,
        "vz": 0.5,
        "mag_heading": 90.0,
    }

    component = _make_component()
    ok, message_type, validated_payload, reason = component._process_input_message(
        COMMAND_TOPIC, payload
    )

    assert ok is True
    assert reason == ""
    assert message_type == "COMMAND"
    assert validated_payload == payload
    assert contracts.resolve_verified_topic(
        message_type,
        VERIFIED_COMMAND_TOPIC,
        VERIFIED_HOME_TOPIC,
    ) == VERIFIED_COMMAND_TOPIC


def test_process_input_message_accepts_valid_home_message() -> None:
    payload = {
        "drone_id": "drone_010",
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
    }

    component = _make_component()
    ok, message_type, validated_payload, reason = component._process_input_message(
        HOME_TOPIC, payload
    )

    assert ok is True
    assert reason == ""
    assert message_type == "HOME"
    assert validated_payload == payload
    assert contracts.resolve_verified_topic(
        message_type,
        VERIFIED_COMMAND_TOPIC,
        VERIFIED_HOME_TOPIC,
    ) == VERIFIED_HOME_TOPIC


def test_process_input_message_rejects_missing_required_command_field() -> None:
    payload = {
        "drone_id": "drone_001",
        "vx": 3.5,
        "vy": -1.0,
        "mag_heading": 90.0,
    }

    component = _make_component()
    ok, message_type, validated_payload, reason = component._process_input_message(
        COMMAND_TOPIC, payload
    )

    assert ok is False
    assert message_type is None
    assert validated_payload is None
    assert "required property" in reason.lower() or "vz" in reason


def test_process_input_message_rejects_additional_fields() -> None:
    payload = {
        "drone_id": "drone_001",
        "home_lat": 59.0,
        "home_lon": 30.0,
        "home_alt": 100.0,
        "unexpected": "value",
    }

    component = _make_component()
    ok, message_type, validated_payload, reason = component._process_input_message(
        HOME_TOPIC, payload
    )

    assert ok is False
    assert message_type is None
    assert validated_payload is None
    assert "Additional properties are not allowed" in reason


def test_process_input_message_rejects_out_of_range_heading() -> None:
    payload = {
        "drone_id": "drone_001",
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "mag_heading": 400.0,
    }

    component = _make_component()
    ok, message_type, validated_payload, reason = component._process_input_message(
        COMMAND_TOPIC, payload
    )

    assert ok is False
    assert message_type is None
    assert validated_payload is None
    assert "359.9" in reason


def test_process_input_message_rejects_unsupported_topic() -> None:
    component = _make_component()
    ok, message_type, validated_payload, reason = component._process_input_message(
        "sitl-unknown", {"drone_id": "drone_001"}
    )

    assert ok is False
    assert message_type is None
    assert validated_payload is None
    assert "unsupported topic" in reason


def test_start_starts_bus_before_raw_topic_subscriptions() -> None:
    import asyncio

    from components.sitl_verifier.src.sitl_verifier import SitlVerifierComponent

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        mock_bus = MagicMock()
        component = SitlVerifierComponent(
            component_id="test-verifier",
            bus=mock_bus,
            topic="components.sitl_verifier",
        )

        component.start()

        assert mock_bus.method_calls[0][0] == "start"
        subscribe_topics = [call.args[0] for call in mock_bus.subscribe.call_args_list]
        assert subscribe_topics == [
            "components.sitl_verifier",
            COMMAND_TOPIC,
            HOME_TOPIC,
        ]
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def _make_component():
    """Создаёт компонент с мокнутым брокером для тестирования."""
    from unittest.mock import MagicMock
    from components.sitl_verifier.src.sitl_verifier import SitlVerifierComponent

    mock_bus = MagicMock()
    return SitlVerifierComponent(
        component_id="test-verifier",
        bus=mock_bus,
        topic="components.sitl_verifier",
    )
