"""
ЦПБ-2, ЦПБ-3, ЦПБ-6: Граничные случаи и отказоустойчивость
Покрывает: Boundary values, TTL=0, geofence precision, state consistency
"""
import pathlib
import sys
import asyncio
import pytest
from unittest.mock import MagicMock, patch

# 🛡 ГЛОБАЛЬНЫЙ МОК ИНФОПАНЕЛИ (ОБЯЗАТЕЛЬНО ДО ИМПОРТА КОМПОНЕНТОВ)
_mock_infopanel = MagicMock()
patch(
    "shared.infopanel_client.create_infopanel_client_from_env",
    return_value=_mock_infopanel,
).start()

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared import state
from components.sitl_controller.src.sitl_controller import SitlControllerComponent
from components.sitl_core.src.sitl_core import SitlCoreComponent

@pytest.fixture
def fake_redis():
    from tests.unit.fakes import FakeRedis
    return FakeRedis()

@pytest.mark.asyncio
async def test_cpbb2_cpbb3_boundary_command_accepted(fake_redis):
    """Команда с пограничными значениями (vx=±50, vz=±10) должна применяться."""
    comp = SitlControllerComponent("c1", MagicMock())
    comp._redis = fake_redis
    
    # Задаём HOME
    await comp._handle_verified_message({
        "payload": {"drone_id": "drone_001", "home_lat": 59.0, "home_lon": 30.0, "home_alt": 100.0},
        "message_type": "HOME"
    })
    
    # Команда на границе допустимых значений
    res = await comp._handle_verified_message({
        "payload": {"drone_id": "drone_001", "vx": 50.0, "vy": 0.0, "vz": 10.0, "mag_heading": 0.0},
        "message_type": "COMMAND"
    })
    assert res["status"] == "command_applied"
    stored = await fake_redis.hgetall("drone:drone_001:state")
    assert stored["status"] == "MOVING"

@pytest.mark.asyncio
async def test_cpbb3_ttl_zero_does_not_expire(fake_redis):
    """TTL=0 должен сохранять состояние, но не ставить expire."""
    comp = SitlControllerComponent("c1", MagicMock())
    comp._redis = fake_redis
    comp._state_ttl_sec = 0
    
    await comp._handle_verified_message({
        "payload": {"drone_id": "drone_t1", "home_lat": 59.0, "home_lon": 30.0, "home_alt": 100.0},
        "message_type": "HOME"
    })
    # TTL=0 означает, что expire не вызывается
    assert "drone:drone_t1:state" not in fake_redis.expirations or fake_redis.expirations.get("drone:drone_t1:state") == 0

def test_cpbb6_geofence_haversine_precision():
    """Проверка точности расчёта дистанции для геозоны."""
    s = {"lat": 59.9386, "lon": 30.3141, "home_lat": 59.9386, "home_lon": 30.3141}
    assert state.distance_from_home_meters(s) == pytest.approx(0.0, abs=0.1)
    
    s2 = {**s, "lon": 30.3151}
    assert state.is_within_home_geofence(s2, 150.0) is True
    assert state.is_within_home_geofence(s2, 50.0) is False

def test_cpbb6_advance_state_non_moving_returns_unchanged():
    next_s = state.advance_drone_state({"status": "ARMED", "vx": 5.0}, 1.0)
    assert next_s["status"] == "ARMED"
    assert "lat" not in next_s  # Координаты не меняются без статуса MOVING