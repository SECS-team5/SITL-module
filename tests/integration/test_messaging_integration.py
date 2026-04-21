"""
Интеграционный тест: запрос позиции через Messaging.

Поток:
  sitl.telemetry.request → Messaging → (чтение Redis) → sitl.telemetry.response

Тест взаимодействует:
  ВХОД:  sitl.telemetry.request  (топик запроса)
  ВЫХОД: sitl.telemetry.response  (топик ответа)

Предусловие: состояние дрона задаётся через sitl-drone-home (как в реальной системе).
"""
import asyncio
import os
import pathlib
import sys
from uuid import uuid4

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis.asyncio as redis

from broker.src.bus_factory import create_system_bus
from shared import state
from shared.contracts import HOME_SCHEMA_NAME, POSITION_REQUEST_SCHEMA_NAME, validate_schema

# ── Брокер из .env ──────────────────────────────────────────────
BROKER_BACKEND = os.environ.get("BROKER_BACKEND", "mqtt")
os.environ.setdefault("BROKER_TYPE", BROKER_BACKEND)
os.environ.setdefault("MQTT_BROKER", os.environ.get("MQTT_BROKER", "mosquitto"))
os.environ.setdefault("MQTT_PORT", os.environ.get("MQTT_PORT", "1883"))
os.environ.setdefault("MQTT_QOS", os.environ.get("MQTT_QOS", "1"))
os.environ.setdefault("KAFKA_SERVERS", os.environ.get("KAFKA_SERVERS", "kafka:29092"))
os.environ.setdefault("SYSTEM_ID", "integration-test-messaging")
os.environ.setdefault("REDIS_URL", os.environ.get("REDIS_URL", "redis://redis:6379"))

# ── Топики ──────────────────────────────────────────────────────
INPUT_HOME_TOPIC = os.getenv("HOME_TOPIC", "sitl-drone-home")
REQUEST_TOPIC = os.getenv("POSITION_REQUEST_TOPIC", "sitl.telemetry.request")
RESPONSE_TOPIC = os.getenv("POSITION_RESPONSE_TOPIC", "sitl.telemetry.response")

DRONE_ID = "drone_001"


def _make_bus() -> "SystemBus":  # type: ignore[name-defined]
    bus = create_system_bus()
    bus.start()
    return bus


async def _wait_for_home_in_redis(redis_client: redis.Redis, drone_id: str, timeout: float = 10.0) -> dict:
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and "home_lat" in stored:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"HOME не появился в Redis за {timeout}с")


async def test_position_request_via_topics() -> None:
    """Тест: запрос позиции через Messaging возвращает координаты."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    bus = _make_bus()
    correlation_id = uuid4().hex
    response_future: asyncio.Future = asyncio.get_event_loop().create_future()

    def _on_response(msg):
        if msg.get("correlation_id") == correlation_id and not response_future.done():
            response_future.set_result(msg)

    bus.subscribe(RESPONSE_TOPIC, _on_response)

    try:
        # 1. Создаём состояние через HOME (как в реальной системе)
        print("1. Задаю HOME через входной топик...")
        home_payload = {
            "drone_id": DRONE_ID,
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 150.0,
        }
        ok, reason = validate_schema(home_payload, HOME_SCHEMA_NAME)
        assert ok, f"HOME не валиден: {reason}"
        bus.publish(INPUT_HOME_TOPIC, home_payload)
        await _wait_for_home_in_redis(redis_client, DRONE_ID, timeout=10.0)
        print("   ✅ HOME создан")

        # 2. Запрос позиции через топик Messaging
        print("2. Запрос позиции через sitl.telemetry.request...")
        request = {
            "drone_id": DRONE_ID,
            "action": "request_position",
            "correlation_id": correlation_id,
        }
        ok, reason = validate_schema(request, POSITION_REQUEST_SCHEMA_NAME)
        assert ok, f"Запрос не валиден: {reason}"

        bus.publish(REQUEST_TOPIC, request)

        # 3. Ждём ответ
        response = await asyncio.wait_for(response_future, timeout=5.0)
        payload = response.get("payload", response)

        assert payload.get("lat") == 59.9386, f"lat={payload.get('lat')}"
        assert payload.get("lon") == 30.3141, f"lon={payload.get('lon')}"
        assert payload.get("alt") == 150.0, f"alt={payload.get('alt')}"

        print(f"   ✅ Координаты получены: lat={payload['lat']}, lon={payload['lon']}, alt={payload['alt']}")
        print("✅ Тест пройден!")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass
        bus.stop()


if __name__ == "__main__":
    print(f"Тест: запрос позиции через топики (BROKER_BACKEND={BROKER_BACKEND})")
    print("-" * 60)
    try:
        asyncio.run(test_position_request_via_topics())
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
