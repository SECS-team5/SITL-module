# tests/unit/test_verifier_unit.py
import json
import pathlib
import sys
import asyncio
import pytest
from unittest.mock import MagicMock, patch

# 🛡 ГЛОБАЛЬНЫЙ МОК ИНФОПАНЕЛИ
# Должен быть ДО импорта любых компонентов
_mock_infopanel = MagicMock()
patch(
    "shared.infopanel_client.create_infopanel_client_from_env",
    return_value=_mock_infopanel,
).start()

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared import contracts  # noqa: E402
from components.sitl_verifier.src.sitl_verifier import SitlVerifierComponent  # noqa: E402

COMMAND_TOPIC = "sitl.commands"
HOME_TOPIC = "sitl-drone-home"
VERIFIED_COMMAND_TOPIC = "sitl.verified-commands"
VERIFIED_HOME_TOPIC = "sitl.verified-home"


def _make_component():
    """Создаёт компонент с мокнутым брокером для тестирования."""
    mock_bus = MagicMock()
    return SitlVerifierComponent(
        component_id="test-verifier",
        bus=mock_bus,
        topic="components.sitl_verifier",
    )


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


def test_verifier_start_subscribes_and_loop():
    mock_bus = MagicMock()
    with patch.object(SitlVerifierComponent, '_register_handlers', return_value=None):
        comp = SitlVerifierComponent("v1", mock_bus, topic="test")
        comp._loop = asyncio.get_event_loop()
        comp.start()
        assert mock_bus.subscribe.call_count >= 1


def test_verifier_log_callback_error_success_and_fail():
    mock_bus = MagicMock()
    comp = SitlVerifierComponent("v1", mock_bus)
    fut_ok = asyncio.Future()
    fut_ok.set_result("ok")
    comp._log_callback_error(fut_ok, "cmd")

    fut_err = asyncio.Future()
    fut_err.set_exception(RuntimeError("fail"))
    comp._log_callback_error(fut_err, "cmd")


@pytest.mark.asyncio
async def test_handle_raw_command_reject_and_publish():
    mock_bus = MagicMock()
    with patch.object(SitlVerifierComponent, '_process_input_message', return_value=(False, None, None, "bad")):
        comp = SitlVerifierComponent("v1", mock_bus)
        res = await comp._handle_raw_command({"payload": "x"})
        assert res["status"] == "rejected"

    with patch.object(SitlVerifierComponent, '_process_input_message', return_value=(True, "COMMAND", {"drone_id": "d"}, " ")):
        with patch.object(contracts, "resolve_verified_topic", return_value="out"):
            comp = SitlVerifierComponent("v1", mock_bus)
            comp._verified_commands_topic = "cmd"
            comp._verified_home_topic = "home"
            res = await comp._handle_raw_command({"payload": {}})
            assert res["status"] == "verified"
            assert mock_bus.publish.called


@pytest.mark.asyncio
async def test_handle_raw_home_reject_and_publish():
    mock_bus = MagicMock()
    with patch.object(SitlVerifierComponent, '_process_input_message', return_value=(True, "HOME", {"drone_id": "d"}, " ")):
        with patch.object(contracts, "resolve_verified_topic", return_value="out"):
            comp = SitlVerifierComponent("v1", mock_bus)
            comp._verified_commands_topic = "cmd"
            comp._verified_home_topic = "home"
            res = await comp._handle_raw_home({"payload": {}})
            assert res["status"] == "verified"
            assert mock_bus.publish.called