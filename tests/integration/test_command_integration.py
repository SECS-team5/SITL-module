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
DRONE_ID = "drone_001"

# ── Хелперы ─────────────────────────────────────────────────────
def _make_bus():
    bus = create_system_bus()
    bus.start()
    return bus

async def _wait_for_home_in_redis(redis_client: redis.Redis, drone_id: str, timeout: float = 10.0) -> dict:
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and "home_lat" in stored:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"HOME не появился в Redis за {timeout}с")

async def _wait_for_status(redis_client: redis.Redis, drone_id: str, expected: str, timeout: float = 10.0) -> dict:
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and state.normalize_state(stored).get("status") == expected:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"Статус '{expected}' не получен за {timeout}с")

async def _safe_stop_components(*components):
    """Безопасная остановка: ловит TypeError из BaseAsyncComponent.stop()"""
    for comp in components:
        try:
            res = comp.stop()
            if asyncio.iscoroutine(res):
                await res
        except TypeError:
            pass

# ── Тест ────────────────────────────────────────────────────────
async def test_command_updates_drone_state_via_topics() -> None:
    """Тест: валидная команда через Verifier обновляет состояние в Redis."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)
    bus = _make_bus()

    command_payload = {
        "drone_id": DRONE_ID,
        "vx": 5.0, "vy": 3.0, "vz": 1.5, "mag_heading": 45.0,
    }

    from components.sitl_verifier.src.sitl_verifier import SitlVerifierComponent
    from components.sitl_controller.src.sitl_controller import SitlControllerComponent

    # 🚀 Запускаем компоненты
    verifier = SitlVerifierComponent("test-verifier", bus)
    controller = SitlControllerComponent("test-controller", bus)
    verifier.start()
    controller.start()
    await asyncio.sleep(0.5)  # Даём время на регистрацию MQTT-подписок

    try:
        print("1. Задаю HOME через входной топик...")
        home_payload = {
            "drone_id": DRONE_ID,
            "home_lat": 59.9386, "home_lon": 30.3141, "home_alt": 120.0
        }
        bus.publish(INPUT_HOME_TOPIC, home_payload)

        await _wait_for_home_in_redis(redis_client, DRONE_ID, timeout=10.0)
        print("   ✅ HOME обработан")

        print("2. Отправляю команду через входной топик...")
        bus.publish(INPUT_COMMAND_TOPIC, command_payload)

        print("3. Ожидаю обработку команды...")
        stored = await _wait_for_status(redis_client, DRONE_ID, "MOVING", timeout=10.0)
        assert float(stored["vx"]) == 5.0
        assert float(stored["vy"]) == 3.0
        assert float(stored["vz"]) == 1.5
        assert float(stored["mag_heading"]) == 45.0
        print(f"   ✅ Команда обработана: status={stored['status']}, vx={stored['vx']}")
        print("\n✅ Тест пройден!")

    finally:
        await _safe_stop_components(verifier, controller)
        bus.stop()
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass

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
