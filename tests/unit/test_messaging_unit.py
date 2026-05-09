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


@pytest.fixture
def fake_redis():
    """Fake Redis для тестов."""
    from tests.unit.fakes import FakeRedis
    return FakeRedis()


def _seed_drone_state(fake_redis, drone_id="drone_001"):
    fake_redis.hashes[f"drone:{drone_id}:state"] = {
        "status": "MOVING", "lat": 59.9386, "lon": 30.3141, "alt": 100.0,
        "vx": 0.0, "vy": 0.0, "vz": 0.0,
        "home_lat": 59.9386, "home_lon": 30.3141, "home_alt": 100.0,
    }


def _make_messaging_component(fake_redis):
    """Создаёт компонент messaging с fake Redis."""
    from components.sitl_messaging.src.sitl_messaging import SitlMessagingComponent
    mock_bus = MagicMock()
    component = SitlMessagingComponent(
        component_id="test-messaging",
        bus=mock_bus,
        topic="components.sitl_messaging",
    )
    component._redis = fake_redis
    return component


@pytest.mark.asyncio
async def test_handle_request_position_returns_coordinates(fake_redis):
    drone_state = {
        "status": "MOVING", "lat": 59.9386, "lon": 30.3141, "alt": 100.0,
        "vx": 0.0, "vy": 0.0, "vz": 0.0,
        "home_lat": 59.9386, "home_lon": 30.3141, "home_alt": 100.0,
    }
    fake_redis.hashes["drone:drone_001:state"] = drone_state
    component = _make_messaging_component(fake_redis)

    message = {"payload": {"drone_id": "drone_001"}, "correlation_id": "test-123"}
    result = await component._handle_request_position(message)

    assert result is not None
    assert "lat" in result
    assert "lon" in result
    assert "alt" in result
    assert result["lat"] == 59.9386
    
    component.bus.publish.assert_called_once()
    published_topic, response_message = component.bus.publish.call_args.args
    assert published_topic == component._response_topic
    assert response_message["correlation_id"] == "test-123"
    assert response_message["payload"] == result
    assert response_message["drone_id"] == "drone_001"


@pytest.mark.asyncio
async def test_handle_request_position_uses_reply_to_from_payload(fake_redis):
    _seed_drone_state(fake_redis)
    component = _make_messaging_component(fake_redis)
    message = {
        "payload": {
            "drone_id": "drone_001",
            "reply_to": "sitl.telemetry.response.demo",
            "correlation_id": "payload-123",
        }
    }
    result = await component._handle_request_position(message)

    assert result is not None
    component.bus.publish.assert_called_once()
    published_topic, response_message = component.bus.publish.call_args.args
    assert published_topic == "sitl.telemetry.response.demo"
    assert response_message["correlation_id"] == "payload-123"
    assert response_message["payload"] == result
    assert response_message["drone_id"] == "drone_001"


@pytest.mark.asyncio
async def test_handle_request_position_drone_not_found(fake_redis):
    component = _make_messaging_component(fake_redis)
    message = {"payload": {"drone_id": "drone_999"}, "correlation_id": "test-123"}
    result = await component._handle_request_position(message)

    assert result is not None
    assert "error" in result
    assert "not found" in result["error"]


def test_messaging_start():
    from components.sitl_messaging.src.sitl_messaging import SitlMessagingComponent
    mock_bus = MagicMock()
    comp = SitlMessagingComponent("m1", mock_bus)
    comp.start()
    mock_bus.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_handle_request_schema_fail(fake_redis):
    mock_bus = MagicMock()
    with patch.object(contracts, "validate_schema", return_value=(False, "bad")):
        comp = _make_messaging_component(fake_redis)
        res = await comp._handle_request_position({"payload": {}})
        assert "error" in res
