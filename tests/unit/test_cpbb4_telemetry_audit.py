"""
ЦПБ-4: Центр безопасной телеметрии
Покрывает: Трассировка запросов, audit через correlation_id, READ-ONLY policy
"""
import pytest
from unittest.mock import MagicMock
from components.sitl_messaging.src.sitl_messaging import TelemetryProxy

@pytest.fixture
def fake_redis():
    from tests.unit.fakes import FakeRedis
    r = FakeRedis()
    # ✅ drone_002 соответствует паттерну ^drone_[0-9]{3,4}$ из схемы
    r.hashes["drone:drone_002:state"] = {
        "status": "MOVING", "lat": "59.9", "lon": "30.3", "alt": "120.0",
        "home_lat": "59.9", "home_lon": "30.3", "home_alt": "120.0",
        "vx": "1.0", "vy": "0.0", "vz": "0.0"
    }
    return r

@pytest.fixture
def mock_infopanel():
    return MagicMock()

@pytest.fixture
def proxy(fake_redis, mock_infopanel):
    return TelemetryProxy(
        redis_client=fake_redis,
        response_topic="default.topic",
        infopanel_client=mock_infopanel,
        component_id="test-messaging"
    )

@pytest.mark.asyncio
async def test_cpbb4_correlation_id_traced(proxy, mock_infopanel):
    cid = "audit-123-xyz"
    # ✅ transport-поля вынесены на верхний уровень, в payload только drone_id
    msg = {"payload": {"drone_id": "drone_002"}, "correlation_id": cid}
    await proxy.process_request(msg)
    
    calls = [str(c) for c in mock_infopanel.log_event.call_args_list]
    assert any(cid in call for call in calls), f"Correlation ID {cid} не найден в логах"

@pytest.mark.asyncio
async def test_cpbb4_readonly_response_structure(proxy):
    msg = {"payload": {"drone_id": "drone_002"}}
    resp = await proxy.process_request(msg)
    assert resp.success is True
    pos = resp.position
    # ✅ READ-ONLY policy: возвращаются только координаты
    assert set(pos.keys()) == {"lat", "lon", "alt"}
    assert pos["lat"] == 59.9 and pos["lon"] == 30.3 and pos["alt"] == 120.0