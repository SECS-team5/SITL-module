"""
Интеграционный тест: валидация и обработка команд.

Проверяет полный пайплайн:
  Verifier → Controller → Redis
"""
import asyncio
import os
import pathlib
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]  # new-SITL/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis.asyncio as redis

from broker.src.bus_factory import create_system_bus
from shared import state
from shared.contracts import (
    COMMAND_SCHEMA_NAME,
    VERIFIED_COMMAND_TOPIC_DEFAULT,
    validate_schema,
)

BROKER_BACKEND = os.environ.get("BROKER_BACKEND", "kafka")
os.environ.setdefault("BROKER_TYPE", BROKER_BACKEND)
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", os.environ.get("KAFKA_SERVERS", "kafka:29092"))
os.environ.setdefault("SYSTEM_ID", "integration-test")

# Топик компонента Controller
CONTROLLER_TOPIC = "components.sitl_controller"
VERIFIED_COMMAND_TOPIC = os.getenv("VERIFIED_COMMAND_TOPIC", VERIFIED_COMMAND_TOPIC_DEFAULT)
STATE_TTL_SEC = 7200
DRONE_ID = "drone_001"


async def send_command_to_controller(payload: dict[str, Any]) -> None:
    """Отправляет команду напрямую в Controller через его топик."""
    bus = create_system_bus()
    bus.start()

    message = {
        "action": "verified_message",
        "payload": payload,
        "message_type": "COMMAND",
        "source_topic": VERIFIED_COMMAND_TOPIC,
    }

    print(f"Публикую сообщение в топик: {CONTROLLER_TOPIC}")
    bus.publish(CONTROLLER_TOPIC, message)

    # Даём время на доставку и обработку
    await asyncio.sleep(3.0)

    bus.stop()


async def _setup_drone_home_state(redis_client: redis.Redis, drone_id: str) -> None:
    """Предусловие: дрон должен иметь HOME состояние."""
    drone_key = state.get_drone_state_key(drone_id)
    initial_state = {
        "status": "ARMED",
        "lat": "59.9386",
        "lon": "30.3141",
        "alt": "120.0",
        "home_lat": "59.9386",
        "home_lon": "30.3141",
        "home_alt": "120.0",
        "vx": "0.0",
        "vy": "0.0",
        "vz": "0.0",
        "speed_h_ms": "0.0",
        "speed_v_ms": "0.0",
        "mag_heading": "0.0",
        "last_update": "2025-01-01T00:00:00+00:00",
    }
    await redis_client.hset(drone_key, mapping=initial_state)
    await redis_client.expire(drone_key, STATE_TTL_SEC)


async def test_command_updates_drone_state_in_redis() -> None:
    """Тест: валидная команда обновляет состояние дрона в Redis."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)

    drone_key = state.get_drone_state_key(DRONE_ID)

    command_payload = {
        "drone_id": DRONE_ID,
        "vx": 5.0,
        "vy": 3.0,
        "vz": 1.5,
        "mag_heading": 45.0,
    }

    try:
        print(f"Проверяю валидацию команды...")
        ok, reason = validate_schema(command_payload, COMMAND_SCHEMA_NAME)
        assert ok, f"Команда не валидна: {reason}"

        print(f"Создаю HOME состояние для {DRONE_ID}")
        await _setup_drone_home_state(redis_client, DRONE_ID)
        await asyncio.sleep(0.5)

        print(f"Отправляю команду в Controller")
        await send_command_to_controller(command_payload)

        print("Ожидаю обработку команды...")
        max_wait = 15
        poll_interval = 0.5
        stored_data = None

        for attempt in range(int(max_wait / poll_interval)):
            try:
                stored_data = await redis_client.hgetall(drone_key)
                if stored_data:
                    status = stored_data.get("status")
                    if status == "MOVING":
                        print(f"Статус MOVING получен через {attempt * poll_interval:.1f}с")
                        break
            except Exception:
                pass
            await asyncio.sleep(poll_interval)
        else:
            print(f"Данные не обновились за {max_wait}с")

        assert stored_data, f"Ключ {drone_key} не найден"

        stored_state = state.normalize_state(stored_data)
        print(f"Состояние: status={stored_state.get('status')}, vx={stored_state.get('vx')}")

        assert stored_state["status"] == "MOVING"
        assert float(stored_state["vx"]) == 5.0
        assert float(stored_state["vy"]) == 3.0
        assert float(stored_state["vz"]) == 1.5
        assert float(stored_state["mag_heading"]) == 45.0

        print("Тест пройден! Команда обработана")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    print(f"Тесты команд (BROKER_BACKEND={BROKER_BACKEND})")
    print("-" * 60)
    try:
        asyncio.run(test_command_updates_drone_state_in_redis())
        print("\nВсе тесты команд пройдены!")
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
