import json
import pathlib
import sys
import asyncio
import pytest
from unittest.mock import MagicMock, patch

# 🛡 ГЛОБАЛЬНЫЙ МОК ИНФОПАНЕЛИ — ДОЛЖЕН БЫТЬ ДО ИМПОРТА КОМПОНЕНТОВ
_mock_infopanel = MagicMock()
patch(
    "shared.infopanel_client.create_infopanel_client_from_env",
    return_value=_mock_infopanel,
).start()

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared import contracts  # noqa: E402
from components.sitl_verifier.src.sitl_verifier import (  # noqa: E402
    SitlVerifierComponent,
    ValidationChain,
    ValidationResult,
)

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
    """Тестируем ValidationChain напрямую, т.к. _process_input_message удалён."""
    payload = {
        "drone_id": "drone_001",
        "vx": 3.5,
        "vy": -1.0,
        "vz": 0.5,
        "mag_heading": 90.0,
    }
    chain = ValidationChain(
        commands_topic=COMMAND_TOPIC,
        home_topic=HOME_TOPIC,
        infopanel_client=_mock_infopanel,
    )
    result = chain.validate(COMMAND_TOPIC, payload)
    
    assert result.success is True
    assert result.message_type == "COMMAND"
    assert result.validated_payload is not None
    assert result.validated_payload["drone_id"] == "drone_001"
    assert "verified_at" not in result.validated_payload
    assert "verified_by" not in result.validated_payload
    assert "verified_at" in result.verification_metadata
    assert result.verification_metadata["verified_by"] == "sitl_verifier"
    assert contracts.resolve_verified_topic(
        result.message_type,
        VERIFIED_COMMAND_TOPIC,
        VERIFIED_HOME_TOPIC,
    ) == VERIFIED_COMMAND_TOPIC


def test_process_input_message_accepts_valid_home_message() -> None:
    """Тестируем ValidationChain напрямую."""
    payload = {
        "drone_id": "drone_010",
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
    }
    chain = ValidationChain(
        commands_topic=COMMAND_TOPIC,
        home_topic=HOME_TOPIC,
        infopanel_client=_mock_infopanel,
    )
    result = chain.validate(HOME_TOPIC, payload)
    
    assert result.success is True
    assert result.message_type == "HOME"
    assert result.validated_payload is not None
    assert contracts.resolve_verified_topic(
        result.message_type,
        VERIFIED_COMMAND_TOPIC,
        VERIFIED_HOME_TOPIC,
    ) == VERIFIED_HOME_TOPIC


def test_process_input_message_rejects_missing_required_command_field() -> None:
    """Проверяем отклонение при недостающем поле."""
    payload = {
        "drone_id": "drone_001",
        "vx": 3.5,
        "vy": -1.0,
        "mag_heading": 90.0,  # missing vz
    }
    chain = ValidationChain(
        commands_topic=COMMAND_TOPIC,
        home_topic=HOME_TOPIC,
        infopanel_client=_mock_infopanel,
    )
    result = chain.validate(COMMAND_TOPIC, payload)
    
    assert result.success is False
    assert result.message_type is None or result.message_type == "COMMAND"
    assert result.validated_payload is None
    assert "required" in result.reason.lower() or "vz" in result.reason


def test_process_input_message_rejects_additional_fields() -> None:
    """Проверяем отклонение при лишних полях."""
    payload = {
        "drone_id": "drone_001",
        "home_lat": 59.0,
        "home_lon": 30.0,
        "home_alt": 100.0,
        "unexpected": "value",
    }
    chain = ValidationChain(
        commands_topic=COMMAND_TOPIC,
        home_topic=HOME_TOPIC,
        infopanel_client=_mock_infopanel,
    )
    result = chain.validate(HOME_TOPIC, payload)
    
    assert result.success is False
    assert "Additional properties" in result.reason


def test_process_input_message_rejects_out_of_range_heading() -> None:
    """Проверяем отклонение при некорректном heading."""
    payload = {
        "drone_id": "drone_001",
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "mag_heading": 400.0,
    }
    chain = ValidationChain(
        commands_topic=COMMAND_TOPIC,
        home_topic=HOME_TOPIC,
        infopanel_client=_mock_infopanel,
    )
    result = chain.validate(COMMAND_TOPIC, payload)
    
    assert result.success is False
    assert "359.9" in result.reason or "maximum" in result.reason.lower()


def test_process_input_message_rejects_unsupported_topic() -> None:
    """Проверяем отклонение неподдерживаемого топика."""
    chain = ValidationChain(
        commands_topic=COMMAND_TOPIC,
        home_topic=HOME_TOPIC,
        infopanel_client=_mock_infopanel,
    )
    result = chain.validate("sitl-unknown", {"drone_id": "drone_001"})
    
    assert result.success is False
    assert "unsupported topic" in result.reason


def test_verifier_start_subscribes_and_loop():
    mock_bus = MagicMock()
    with patch.object(SitlVerifierComponent, '_register_handlers', return_value=None):
        comp = SitlVerifierComponent("v1", mock_bus, topic="test")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            comp._loop = loop
            comp.start()
            assert mock_bus.subscribe.call_count >= 1
        finally:
            loop.close()
            asyncio.set_event_loop(None)


def test_verifier_log_callback_error_success_and_fail():
    mock_bus = MagicMock()
    comp = SitlVerifierComponent("v1", mock_bus)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        fut_ok = loop.create_future()
        fut_ok.set_result("ok")
        comp._log_callback_error(fut_ok, "cmd")
        fut_err = loop.create_future()
        fut_err.set_exception(RuntimeError("fail"))
        comp._log_callback_error(fut_err, "cmd")
    finally:
        loop.close()
        asyncio.set_event_loop(None)


@pytest.mark.asyncio
async def test_handle_raw_command_reject_and_publish():
    mock_bus = MagicMock()
    component = SitlVerifierComponent("v1", mock_bus)
    component._verified_commands_topic = VERIFIED_COMMAND_TOPIC
    component._verified_home_topic = VERIFIED_HOME_TOPIC
    
    # Мокаем ValidationChain.validate вместо удалённого _process_input_message
    with patch.object(component._validation_chain, 'validate',
                     return_value=ValidationResult(False, None, None, "bad schema")):
        res = await component._handle_raw_command({"payload": "x"})
        assert res["status"] == "rejected"
        assert res["reason"] == "bad schema"
        mock_bus.publish.assert_not_called()


@pytest.mark.asyncio
async def test_handle_raw_command_success_and_publish():
    mock_bus = MagicMock()
    component = SitlVerifierComponent("v1", mock_bus)
    component._verified_commands_topic = VERIFIED_COMMAND_TOPIC
    component._verified_home_topic = VERIFIED_HOME_TOPIC
    
    validated = {
        "drone_id": "d", "vx": 1.0, "vy": 1.0, "vz": 0.0,
        "mag_heading": 90.0,
    }
    
    with patch.object(component._validation_chain, 'validate',
                     return_value=ValidationResult(
                         True,
                         "COMMAND",
                         validated,
                         "",
                         {"verified_at": "now", "verified_by": "sitl_verifier"},
                     )):
        with patch("components.sitl_verifier.src.sitl_verifier.resolve_verified_topic",
                  return_value="out_topic"):
            res = await component._handle_raw_command({"payload": {}})
            assert res["status"] == "verified"
            assert res["output_topic"] == "out_topic"
            mock_bus.publish.assert_called_once()
            pub_topic, pub_msg = mock_bus.publish.call_args.args
            assert pub_topic == "out_topic"
            assert pub_msg["message_type"] == "COMMAND"
            assert pub_msg["payload"] == validated
            assert "verified_at" not in pub_msg["payload"]
            assert pub_msg["verified_at"] == "now"
            assert pub_msg["verified_by"] == "sitl_verifier"


@pytest.mark.asyncio
async def test_handle_raw_home_reject_and_publish():
    mock_bus = MagicMock()
    component = SitlVerifierComponent("v1", mock_bus)
    component._verified_commands_topic = VERIFIED_COMMAND_TOPIC
    component._verified_home_topic = VERIFIED_HOME_TOPIC
    
    validated = {
        "drone_id": "d", "home_lat": 1.0, "home_lon": 1.0, "home_alt": 1.0,
    }
    
    with patch.object(component._validation_chain, 'validate',
                     return_value=ValidationResult(
                         True,
                         "HOME",
                         validated,
                         "",
                         {"verified_at": "now", "verified_by": "sitl_verifier"},
                     )):
        with patch("components.sitl_verifier.src.sitl_verifier.resolve_verified_topic",
                  return_value="home_out"):
            res = await component._handle_raw_home({"payload": {}})
            assert res["status"] == "verified"
            mock_bus.publish.assert_called_once()
            assert mock_bus.publish.call_args.args[0] == "home_out"
            pub_msg = mock_bus.publish.call_args.args[1]
            assert pub_msg["payload"] == validated
            assert "verified_at" not in pub_msg["payload"]
            assert pub_msg["verified_at"] == "now"
