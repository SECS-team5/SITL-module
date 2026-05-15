"""
ЦПБ-5: Центр изоляции сообщений
Покрывает: Маршрутизация ответов, контроль reply_to, предотвращение утечек
"""
import pytest
from unittest.mock import MagicMock
from components.sitl_messaging.src.sitl_messaging import TelemetryProxy

@pytest.fixture
def fake_redis():
    from tests.unit.fakes import FakeRedis
    r = FakeRedis()
    # ✅ drone_003 соответствует паттерну схемы
    r.hashes["drone:drone_003:state"] = {
        "status": "ARMED", "lat": "59.0", "lon": "30.0", "alt": "100.0",
        "home_lat": "59.0", "home_lon": "30.0", "home_alt": "100.0"
    }
    return r

@pytest.fixture
def mock_infopanel():
    return MagicMock()

@pytest.fixture
def proxy(fake_redis, mock_infopanel):
    return TelemetryProxy(
        redis_client=fake_redis,
        response_topic="sitl.telemetry.response",
        infopanel_client=mock_infopanel,
        component_id="test-messaging"
    )

@pytest.mark.asyncio
async def test_cpbb5_custom_reply_to_used(proxy):
    # ✅ reply_to на верхнем уровне, схема валидирует только payload
    msg = {"payload": {"drone_id": "drone_003"}, "reply_to": "custom.reply.topic"}
    resp = await proxy.process_request(msg)
    assert resp.success is True
    assert resp.reply_topic == "custom.reply.topic"

@pytest.mark.asyncio
async def test_cpbb5_fallback_to_default_topic(proxy):
    msg = {"payload": {"drone_id": "drone_003"}}
    resp = await proxy.process_request(msg)
    assert resp.success is True
    assert resp.reply_topic == "sitl.telemetry.response"

@pytest.mark.asyncio
async def test_cpbb5_unauthorized_drone_rejected(proxy, mock_infopanel):
    # ✅ drone_999 проходит схему, но отсутствует в Redis → отклоняется на шаге авторизации
    msg = {"payload": {"drone_id": "drone_999"}}
    resp = await proxy.process_request(msg)
    assert resp.success is False
    assert "not found" in resp.error
    # Проверяем, что попытка доступа залогирована
    calls = [str(c).lower() for c in mock_infopanel.log_event.call_args_list]
    assert any("unauthorized" in c or "not found" in c for c in calls)