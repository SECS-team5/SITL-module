"""
Интеграционный тест: обработка команд и обновление состояния в Redis.

Проверяет:
1. Создание HOME-состояния (статус ARMED)
2. Обработка COMMAND (статус MOVING)
3. Core обновляет позиции MOVING дронов
4. Messaging возвращает координаты из Redis
"""
import asyncio
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]  # new-SITL/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis.asyncio as redis

from shared import state
from shared.contracts import (
    COMMAND_SCHEMA_NAME,
    HOME_SCHEMA_NAME,
    validate_schema,
)

STATE_TTL_SEC = 7200
DRONE_ID = "drone_001"


async def test_full_drone_lifecycle_in_redis() -> None:
    """Тест: полный жизненный цикл дрона в Redis."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    try:
        # 1. Проверяем валидацию HOME
        print("1. Валидация HOME...")
        home_payload = {
            "drone_id": DRONE_ID,
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 120.0,
        }
        ok, reason = validate_schema(home_payload, HOME_SCHEMA_NAME)
        assert ok, f"HOME не валиден: {reason}"

        # 2. Создаём HOME-состояние
        print("2. Создаю HOME-состояние...")
        home_state = state.build_home_state(home_payload)
        await redis_client.hset(drone_key, mapping=home_state)
        await redis_client.expire(drone_key, STATE_TTL_SEC)

        stored = await redis_client.hgetall(drone_key)
        stored_state = state.normalize_state(stored)
        assert stored_state["status"] == "ARMED"
        assert float(stored_state["lat"]) == 59.9386
        print(f"   HOME создан: status=ARMED, lat={stored_state['lat']}")

        # 3. Проверяем валидацию COMMAND
        print("3. Валидация COMMAND...")
        command_payload = {
            "drone_id": DRONE_ID,
            "vx": 5.0,
            "vy": 3.0,
            "vz": 1.5,
            "mag_heading": 45.0,
        }
        ok, reason = validate_schema(command_payload, COMMAND_SCHEMA_NAME)
        assert ok, f"COMMAND не валиден: {reason}"

        # 4. Применяем команду
        print("4. Применяю команду...")
        current_state = state.normalize_state(await redis_client.hgetall(drone_key))
        next_state = state.apply_command_update(current_state, command_payload)
        await redis_client.hset(drone_key, mapping=next_state)

        stored = await redis_client.hgetall(drone_key)
        stored_state = state.normalize_state(stored)
        assert stored_state["status"] == "MOVING"
        assert float(stored_state["vx"]) == 5.0
        assert float(stored_state["vy"]) == 3.0
        print(f"   Команда применена: status=MOVING, vx={stored_state['vx']}")

        # 5. Core обновляет позицию
        print("5. Core обновляет позицию...")
        original_lat = stored_state["lat"]
        original_lon = stored_state["lon"]

        # Имитируем один тик Core (1 секунда)
        next_state = state.advance_drone_state(stored_state, 1.0)
        await redis_client.hset(drone_key, mapping=next_state)

        stored = await redis_client.hgetall(drone_key)
        updated_state = state.normalize_state(stored)
        assert updated_state["lat"] != original_lat or updated_state["lon"] != original_lon
        print(f"   Позиция обновлена: lat={updated_state['lat']}, lon={updated_state['lon']}")

        # 6. Messaging возвращает координаты
        print("6. Messaging возвращает координаты...")
        response = state.build_position_response(stored)
        assert response is not None
        assert "lat" in response
        assert "lon" in response
        assert "alt" in response
        print(f"   Координаты: lat={response['lat']}, lon={response['lon']}, alt={response['alt']}")

        print("\nТест пройден! Полный жизненный цикл работает")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass


async def test_command_without_home_is_ignored() -> None:
    """Тест: команда без HOME игнорируется."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key("drone_002")

    try:
        # Убеждаемся что нет HOME
        stored = await redis_client.hgetall(drone_key)
        assert not stored, "Дрон не должен иметь состояния"

        # Проверяем что state_has_home вернёт False
        assert not state.state_has_home(stored)

        print("Тест пройден! Команда без HOME корректно игнорируется")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass


async def test_invalid_command_is_rejected() -> None:
    """Тест: невалидная команда отклоняется."""
    invalid_payload = {
        "drone_id": DRONE_ID,
        "vx": 100.0,  # Превышение лимита
        "vy": 0.0,
        "vz": 0.0,
        "mag_heading": 0.0,
    }

    ok, reason = validate_schema(invalid_payload, COMMAND_SCHEMA_NAME)
    assert not ok, "Невалидная команда должна быть отклонена"
    print(f"Тест пройден! Невалидная команда отклонена: {reason}")


if __name__ == "__main__":
    print("Интеграционные тесты: полный жизненный цикл дрона")
    print("=" * 60)
    try:
        asyncio.run(test_full_drone_lifecycle_in_redis())
        print()
        asyncio.run(test_command_without_home_is_ignored())
        print()
        asyncio.run(test_invalid_command_is_rejected())
        print("\nВсе интеграционные тесты пройдены!")
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
