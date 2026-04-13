"""
Интеграционный тест: валидация и обработка команд через входные топики.

Проверяет полный пайплайн:
  sitl.commands → Verifier → sitl.verified-commands → Controller → Redis

Тест взаимодействует ТОЛЬКО через входной топик Verifier.
Проверка результата — через Redis.
"""
import asyncio
import os
import pathlib
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis.asyncio as redis

from broker.src.bus_factory import create_system_bus
from shared import state
from shared.contracts import (
    COMMAND_SCHEMA_NAME,
    HOME_SCHEMA_NAME,
    validate_schema,
)

# ── Брокер из .env ──────────────────────────────────────────────
BROKER_BACKEND = os.environ.get("BROKER_BACKEND", "mqtt")
os.environ.setdefault("BROKER_TYPE", BROKER_BACKEND)
os.environ.setdefault("MQTT_BROKER", os.environ.get("MQTT_BROKER", "mosquitto"))
os.environ.setdefault("MQTT_PORT", os.environ.get("MQTT_PORT", "1883"))
os.environ.setdefault("MQTT_QOS", os.environ.get("MQTT_QOS", "1"))
os.environ.setdefault("KAFKA_SERVERS", os.environ.get("KAFKA_SERVERS", "kafka:29092"))
os.environ.setdefault("SYSTEM_ID", "integration-test-command")
os.environ.setdefault("REDIS_URL", os.environ.get("REDIS_URL", "redis://redis:6379"))

# ── Топики ──────────────────────────────────────────────────────
INPUT_HOME_TOPIC = os.getenv("HOME_TOPIC", "sitl-drone-home")
INPUT_COMMAND_TOPIC = os.getenv("COMMAND_TOPIC", "sitl.commands")

STATE_TTL_SEC = 7200
DRONE_ID = "drone_001"


# ── Хелперы ─────────────────────────────────────────────────────

def _make_bus() -> "SystemBus":  # type: ignore[name-defined]
    bus = create_system_bus()
    bus.start()
    return bus


async def _send_home_via_verifier(bus, drone_id: str, lat: float, lon: float, alt: float) -> None:
    """Отправить HOME через входной топик Verifier."""
    payload = {
        "drone_id": drone_id,
        "home_lat": lat,
        "home_lon": lon,
        "home_alt": alt,
    }
    ok, reason = validate_schema(payload, HOME_SCHEMA_NAME)
    assert ok, f"HOME не валиден: {reason}"
    print(f"   📤 Публикую HOME в {INPUT_HOME_TOPIC}")
    bus.publish(INPUT_HOME_TOPIC, payload)


async def _send_command_via_verifier(bus, payload: dict[str, Any]) -> None:
    """Отправить команду через входной топик Verifier."""
    print(f"   📤 Публикую COMMAND в {INPUT_COMMAND_TOPIC}")
    bus.publish(INPUT_COMMAND_TOPIC, payload)


async def _wait_for_home_in_redis(redis_client: redis.Redis, drone_id: str, timeout: float = 10.0) -> dict:
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and "home_lat" in stored:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"HOME не появился в Redis за {timeout}с")


async def _wait_for_status(redis_client: redis.Redis, drone_id: str, expected: str, timeout: float = 10.0) -> dict:
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and state.normalize_state(stored).get("status") == expected:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"Статус '{expected}' не получен за {timeout}с")


# ── Тесты ───────────────────────────────────────────────────────

async def test_command_updates_drone_state_via_topics() -> None:
    """Тест: валидная команда через Verifier обновляет состояние в Redis."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    bus = _make_bus()

    command_payload = {
        "drone_id": DRONE_ID,
        "vx": 5.0,
        "vy": 3.0,
        "vz": 1.5,
        "mag_heading": 45.0,
    }

    try:
        # 1. HOME через входной топик
        print("1. Задаю HOME через входной топик...")
        await _send_home_via_verifier(bus, DRONE_ID, 59.9386, 30.3141, 120.0)
        await _wait_for_home_in_redis(redis_client, DRONE_ID, timeout=10.0)
        print("   ✅ HOME обработан")

        # 2. COMMAND через входной топик
        print("2. Отправляю команду через входной топик...")
        ok, reason = validate_schema(command_payload, COMMAND_SCHEMA_NAME)
        assert ok, f"Команда не валидна: {reason}"
        await _send_command_via_verifier(bus, command_payload)

        # 3. Ждём результат в Redis
        print("3. Ожидаю обработку команды...")
        stored = await _wait_for_status(redis_client, DRONE_ID, "MOVING", timeout=10.0)
        assert float(stored["vx"]) == 5.0
        assert float(stored["vy"]) == 3.0
        assert float(stored["vz"]) == 1.5
        assert float(stored["mag_heading"]) == 45.0
        print(f"   ✅ Команда обработана: status={stored['status']}, vx={stored['vx']}, vy={stored['vy']}")

        print("\n✅ Тест пройден!")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass
        bus.stop()


if __name__ == "__main__":
    print(f"Тесты команд (BROKER_BACKEND={BROKER_BACKEND})")
    print("-" * 60)
    try:
        asyncio.run(test_command_updates_drone_state_via_topics())
        print("\nВсе тесты команд пройдены!")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
