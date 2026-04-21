"""
Интеграционный тест: HOME-позиция через входной топик Verifier.

Поток:
  sitl-drone-home → Verifier → sitl.verified-home → Controller → Redis

Тест взаимодействует ТОЛЬКО через входной топик Verifier.
Проверка результата — через Redis.
"""
import asyncio
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis.asyncio as redis

from broker.src.bus_factory import create_system_bus
from shared import state
from shared.contracts import HOME_SCHEMA_NAME, validate_schema

# ── Брокер из .env ──────────────────────────────────────────────
BROKER_BACKEND = os.environ.get("BROKER_BACKEND", "mqtt")
os.environ.setdefault("BROKER_TYPE", BROKER_BACKEND)
os.environ.setdefault("MQTT_BROKER", os.environ.get("MQTT_BROKER", "mosquitto"))
os.environ.setdefault("MQTT_PORT", os.environ.get("MQTT_PORT", "1883"))
os.environ.setdefault("MQTT_QOS", os.environ.get("MQTT_QOS", "1"))
os.environ.setdefault("KAFKA_SERVERS", os.environ.get("KAFKA_SERVERS", "kafka:29092"))
os.environ.setdefault("SYSTEM_ID", "integration-test-home")
os.environ.setdefault("REDIS_URL", os.environ.get("REDIS_URL", "redis://redis:6379"))

# ── Топики ──────────────────────────────────────────────────────
INPUT_HOME_TOPIC = os.getenv("HOME_TOPIC", "sitl-drone-home")

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


async def test_home_position_via_topics() -> None:
    """Тест: HOME через входной топик сохраняется в Redis."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    home_payload = {
        "drone_id": DRONE_ID,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 120.0,
    }

    bus = _make_bus()

    try:
        # Валидация
        ok, reason = validate_schema(home_payload, HOME_SCHEMA_NAME)
        assert ok, f"HOME не валиден: {reason}"

        # Публикация через входной топик Verifier
        print(f"Публикую HOME в {INPUT_HOME_TOPIC}...")
        bus.publish(INPUT_HOME_TOPIC, home_payload)

        # Ждём появления в Redis (Verifier → Controller → Redis)
        stored = await _wait_for_home_in_redis(redis_client, DRONE_ID, timeout=10.0)

        assert stored["status"] == "ARMED"
        assert float(stored["home_lat"]) == 59.9386
        assert float(stored["home_lon"]) == 30.3141
        assert float(stored["home_alt"]) == 120.0

        print(f"   ✅ HOME сохранён: status={stored['status']}, "
              f"lat={stored['home_lat']}, lon={stored['home_lon']}, alt={stored['home_alt']}")
        print("✅ Тест пройден!")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass
        bus.stop()


if __name__ == "__main__":
    print(f"Тест: HOME-позиция через топики (BROKER_BACKEND={BROKER_BACKEND})")
    print("-" * 60)
    try:
        asyncio.run(test_home_position_via_topics())
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
