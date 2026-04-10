import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]  # new-SITL/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from shared import state  # noqa: E402


@pytest.fixture
def fake_redis():
    """Fake Redis для тестов."""
    from tests.unit.fakes import FakeRedis
    return FakeRedis()


def test_normalize_state_converts_bytes_to_str():
    raw = {
        b"lat": b"59.9386",
        b"lon": b"30.3141",
        b"alt": b"100.0",
        b"status": b"ARMED",
    }
    normalized = state.normalize_state(raw)
    assert normalized["lat"] == 59.9386
    assert normalized["lon"] == 30.3141
    assert normalized["alt"] == 100.0
    assert normalized["status"] == "ARMED"


def test_build_home_state_creates_valid_state():
    payload = {
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
    }
    home_state = state.build_home_state(payload)
    assert home_state["status"] == "ARMED"
    assert home_state["lat"] == 59.9386
    assert home_state["home_lat"] == 59.9386
    assert home_state["vx"] == 0.0
    assert home_state["vy"] == 0.0
    assert home_state["vz"] == 0.0


def test_apply_command_update_changes_state():
    existing = {
        "status": "ARMED",
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 100.0,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "mag_heading": 90.0,
    }
    command = {
        "vx": 3.5,
        "vy": -1.0,
        "vz": 0.5,
        "mag_heading": 95.0,
    }
    next_state = state.apply_command_update(existing, command)
    assert next_state["status"] == "MOVING"
    assert next_state["vx"] == 3.5
    assert next_state["speed_h_ms"] > 0.0


def test_advance_drone_state_updates_position():
    moving_state = {
        "status": "MOVING",
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 100.0,
        "vx": 3.5,
        "vy": -1.0,
        "vz": 0.5,
    }
    original_lat = moving_state["lat"]
    original_lon = moving_state["lon"]
    
    next_state = state.advance_drone_state(moving_state, 1.0)
    assert next_state["lat"] != original_lat or next_state["lon"] != original_lon


@pytest.mark.asyncio
async def test_core_position_updater(fake_redis):
    from unittest.mock import MagicMock, AsyncMock, patch
    from components.sitl_core.src.sitl_core import SitlCoreComponent

    # Заполняем Redis тестовыми данными
    drone_state = {
        "status": "MOVING",
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 100.0,
        "vx": 3.5,
        "vy": -1.0,
        "vz": 0.5,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
    }
    # Сохраняем исходные значения
    original_lat = drone_state["lat"]
    original_lon = drone_state["lon"]
    
    fake_redis.hashes["drone:test:state"] = drone_state.copy()

    mock_bus = MagicMock()
    with patch.object(SitlCoreComponent, '_get_redis', new=AsyncMock(return_value=fake_redis)):
        component = SitlCoreComponent(
            component_id="test-core",
            bus=mock_bus,
            topic="components.sitl_core",
        )
        component._update_hz = 1.0  # 1 обновление в секунду

        # Выполняем одно обновление
        await component._update_drone_position(fake_redis, "drone:test:state", 1.0)

        # Проверяем что позиция обновилась
        updated = await fake_redis.hgetall("drone:test:state")
        assert updated["lat"] != original_lat or updated["lon"] != original_lon
