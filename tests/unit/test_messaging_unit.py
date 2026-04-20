import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]  # new-SITL/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from shared import state, contracts  # noqa: E402


@pytest.fixture(autouse=True)
def infopanel_env(monkeypatch):
    monkeypatch.setenv("INFOPANEL_API_KEY", "test-api-key")


@pytest.fixture
def fake_redis():
    """Fake Redis для тестов."""
    from tests.unit.fakes import FakeRedis
    return FakeRedis()


def _seed_drone_state(fake_redis, drone_id="drone_001"):
    fake_redis.hashes[f"drone:{drone_id}:state"] = {
        "status": "MOVING",
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 100.0,
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
    }


def _make_messaging_component(fake_redis):
    """Создаёт компонент messaging с fake Redis."""
    from unittest.mock import MagicMock
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
    # Заполняем Redis тестовыми данными
    drone_state = {
        "status": "MOVING",
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 100.0,
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
    }
    fake_redis.hashes["drone:drone_001:state"] = drone_state

    component = _make_messaging_component(fake_redis)

    message = {
        "payload": {"drone_id": "drone_001"},
        "correlation_id": "test-123",
    }
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

    message = {
        "payload": {"drone_id": "drone_999"},
        "correlation_id": "test-123",
    }
    result = await component._handle_request_position(message)

    assert result is not None
    assert "error" in result
    assert "not found" in result["error"]


@pytest.mark.asyncio
async def test_handle_message_with_reply_to_publishes_single_response(fake_redis):
    _seed_drone_state(fake_redis)
    component = _make_messaging_component(fake_redis)

    message = {
        "action": "request_position",
        "drone_id": "drone_001",
        "reply_to": "replies.test.single",
        "correlation_id": "single-123",
    }

    await component._handle_message(message)

    component.bus.publish.assert_called_once()
    published_topic, response_message = component.bus.publish.call_args.args
    assert published_topic == "replies.test.single"
    assert response_message["correlation_id"] == "single-123"
    assert response_message["payload"] == {
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 100.0,
    }
    assert response_message["drone_id"] == "drone_001"
