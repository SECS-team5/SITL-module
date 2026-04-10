import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]  # new-SITL/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
import pytest_asyncio

from shared import state  # noqa: E402


@pytest.fixture
def fake_redis():
    """Fake Redis для тестов."""
    from tests.unit.fakes import FakeRedis
    return FakeRedis()


def _make_component(fake_redis):
    """Создаёт компонент с fake Redis."""
    from unittest.mock import MagicMock
    from components.sitl_controller.src.sitl_controller import SitlControllerComponent

    mock_bus = MagicMock()
    component = SitlControllerComponent(
        component_id="test-controller",
        bus=mock_bus,
        topic="components.sitl_controller",
    )
    # Устанавливаем fake Redis напрямую
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

    # Убедимся что Redis пустой — нет HOME
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

    # Сначала HOME
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

    # Потом COMMAND
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
