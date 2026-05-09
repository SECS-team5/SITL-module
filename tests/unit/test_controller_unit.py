# tests/unit/test_controller_unit.py
import pathlib
import sys
import asyncio
import pytest
from unittest.mock import MagicMock, patch

# 🛡 ГЛОБАЛЬНЫЙ МОК ИНФОПАНЕЛИ
# Должен быть ДО импорта любых компонентов, чтобы переопределить фабрику на этапе инициализации
_mock_infopanel = MagicMock()
patch(
    "shared.infopanel_client.create_infopanel_client_from_env",
    return_value=_mock_infopanel,
).start()

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared import state, contracts  # noqa: E402
from components.sitl_controller.src.sitl_controller import SitlControllerComponent  # noqa: E402


@pytest.fixture
def fake_redis():
    """Fake Redis для тестов."""
    from tests.unit.fakes import FakeRedis
    return FakeRedis()


def _make_component(fake_redis):
    """Создаёт компонент с fake Redis."""
    mock_bus = MagicMock()
    component = SitlControllerComponent(
        component_id="test-controller",
        bus=mock_bus,
        topic="components.sitl_controller",
    )
    component._redis = fake_redis
    return component


@pytest.mark.asyncio
async def test_handle_verified_home_message(fake_redis):
    component = _make_component(fake_redis)
    message = {
        "payload": {
            "drone_id": "drone_001",
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 100.0,
        },
        "message_type": "HOME",
    }
    result = await component._handle_verified_message(message)
    assert result["status"] == "home_stored"
    assert result["drone_id"] == "drone_001"


@pytest.mark.asyncio
async def test_handle_verified_command_without_home_fails(fake_redis):
    component = _make_component(fake_redis)
    state_data = await fake_redis.hgetall("drone:drone_001:state")
    assert state_data == {}, f"Expected empty state, got: {state_data}"

    message = {
        "payload": {
            "drone_id": "drone_001",
            "vx": 3.5,
            "vy": -1.0,
            "vz": 0.5,
            "mag_heading": 90.0,
        },
        "message_type": "COMMAND",
    }
    result = await component._handle_verified_message(message)
    assert result["status"] == "ignored", f"Expected 'ignored', got: {result}"
    assert "HOME state missing" in result["reason"]


@pytest.mark.asyncio
async def test_handle_verified_command_after_home(fake_redis):
    component = _make_component(fake_redis)
    home_message = {
        "payload": {
            "drone_id": "drone_001",
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 100.0,
        },
        "message_type": "HOME",
    }
    await component._handle_verified_message(home_message)

    command_message = {
        "payload": {
            "drone_id": "drone_001",
            "vx": 3.5,
            "vy": -1.0,
            "vz": 0.5,
            "mag_heading": 90.0,
        },
        "message_type": "COMMAND",
    }
    result = await component._handle_verified_message(command_message)
    assert result["status"] == "command_applied"
    assert result["drone_id"] == "drone_001"


def test_controller_log_callback_error():
    mock_bus = MagicMock()
    comp = SitlControllerComponent("c1", mock_bus)
    fut = asyncio.Future()
    fut.set_exception(ValueError("err"))
    comp._log_callback_error(fut, "cmd")  # печатает traceback, не падает


@pytest.mark.asyncio
async def test_handle_verified_message_type_none_and_fail(fake_redis):
    mock_bus = MagicMock()
    comp = SitlControllerComponent("c1", mock_bus)
    comp._redis = fake_redis
    msg = {"payload": {}, "message_type": None, "source_topic": "unknown"}
    res = await comp._handle_verified_message(msg)
    assert res["status"] == "rejected"


@pytest.mark.asyncio
async def test_handle_verified_message_unknown_type(fake_redis):
    mock_bus = MagicMock()
    comp = SitlControllerComponent("c1", mock_bus)
    comp._redis = fake_redis
    msg = {"payload": {}, "message_type": "UNKNOWN"}
    res = await comp._handle_verified_message(msg)
    assert "unknown message_type" in res["reason"]


@pytest.mark.asyncio
async def test_handle_verified_message_schema_fail(fake_redis):
    mock_bus = MagicMock()
    with patch("components.sitl_controller.src.sitl_controller.validate_schema", return_value=(False, "drone_id is required")):
        comp = SitlControllerComponent("c1", mock_bus)
        comp._redis = fake_redis
        res = await comp._handle_verified_message({"payload": {}, "message_type": "COMMAND"})
        assert res["status"] == "rejected"
        assert "drone_id" in res["reason"] or "sitl-commands.json" in res["reason"]


@pytest.mark.asyncio
async def test_persist_state_ttl_zero(fake_redis):
    mock_bus = MagicMock()
    comp = SitlControllerComponent("c1", mock_bus)
    comp._redis = fake_redis
    comp._state_ttl_sec = 0  # ветка TTL <= 0
    await comp._persist_state(fake_redis, "d1", {"k": "v"})
    assert fake_redis.hashes["drone:d1:state"] == {"k": "v"}