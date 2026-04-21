"""
Интеграционный тест: полный жизненный цикл дрона через топики.

Проверяет реальный поток сообщений:
  1. sitl-drone-home → Verifier → sitl.verified-home → Controller → Redis
  2. sitl.commands → Verifier → sitl.verified-commands → Controller → Redis
  3. sitl_core обновляет позицию (MOVING дрон)
  4. sitl.telemetry.request → Messaging → sitl.telemetry.response

Тест взаимодействует ТОЛЬКО через входные/выходные топики.
Проверка результата — через Redis (общее состояние).
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
from shared.contracts import (
    COMMAND_SCHEMA_NAME,
    HOME_SCHEMA_NAME,
    POSITION_REQUEST_SCHEMA_NAME,
    validate_schema,
)

# ── Брокер из .env ──────────────────────────────────────────────
BROKER_BACKEND = os.environ.get("BROKER_BACKEND", "mqtt")
os.environ.setdefault("BROKER_TYPE", BROKER_BACKEND)
os.environ.setdefault("MQTT_BROKER", os.environ.get("MQTT_BROKER", "mosquitto"))
os.environ.setdefault("MQTT_PORT", os.environ.get("MQTT_PORT", "1883"))
os.environ.setdefault("MQTT_QOS", os.environ.get("MQTT_QOS", "1"))
os.environ.setdefault("KAFKA_SERVERS", os.environ.get("KAFKA_SERVERS", "kafka:29092"))
os.environ.setdefault("SYSTEM_ID", "integration-test-lifecycle")
os.environ.setdefault("REDIS_URL", os.environ.get("REDIS_URL", "redis://redis:6379"))

# ── Топики (входные / выходные) ─────────────────────────────────
INPUT_HOME_TOPIC = os.getenv("HOME_TOPIC", "sitl-drone-home")
INPUT_COMMAND_TOPIC = os.getenv("COMMAND_TOPIC", "sitl.commands")
REQUEST_TOPIC = os.getenv("POSITION_REQUEST_TOPIC", "sitl.telemetry.request")
RESPONSE_TOPIC = os.getenv("POSITION_RESPONSE_TOPIC", "sitl.telemetry.response")

STATE_TTL_SEC = 7200
DRONE_ID = "drone_001"


# ── Хелперы ─────────────────────────────────────────────────────

def _make_bus() -> "SystemBus":  # type: ignore[name-defined]
    bus = create_system_bus()
    bus.start()
    return bus


async def _wait_for_home_in_redis(redis_client: redis.Redis, drone_id: str, timeout: float = 10.0) -> dict:
    """Ждём появления home_lat/home_lon в Redis (значит Controller обработал HOME)."""
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and "home_lat" in stored:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"HOME не появился в Redis за {timeout}с")


async def _wait_for_status(redis_client: redis.Redis, drone_id: str, expected: str, timeout: float = 10.0) -> dict:
    """Ждём нужный статус в Redis."""
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and state.normalize_state(stored).get("status") == expected:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"Статус '{expected}' не получен за {timeout}с")


# ── Тесты ───────────────────────────────────────────────────────

async def test_full_drone_lifecycle_via_topics() -> None:
    """Полный цикл через входные топики → Verifier → Controller → Redis → Messaging."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    bus = _make_bus()
    response_future: asyncio.Future = asyncio.get_event_loop().create_future()

    def _on_position_response(msg):
        if not response_future.done():
            response_future.set_result(msg)

    bus.subscribe(RESPONSE_TOPIC, _on_position_response)

    try:
        # ── Шаг 1: HOME через входной топик ──
        print("1. Отправляю HOME через входной топик...")
        home_payload = {
            "drone_id": DRONE_ID,
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 120.0,
        }
        ok, reason = validate_schema(home_payload, HOME_SCHEMA_NAME)
        assert ok, f"HOME не валиден: {reason}"

        bus.publish(INPUT_HOME_TOPIC, home_payload)

        stored = await _wait_for_home_in_redis(redis_client, DRONE_ID, timeout=10.0)
        assert stored["status"] == "ARMED"
        assert float(stored["home_lat"]) == 59.9386
        print(f"   ✅ HOME обработан: status={stored['status']}")

        # ── Шаг 2: COMMAND через входной топик ──
        print("2. Отправляю COMMAND через входной топик...")
        command_payload = {
            "drone_id": DRONE_ID,
            "vx": 5.0,
            "vy": 3.0,
            "vz": 1.5,
            "mag_heading": 45.0,
        }
        ok, reason = validate_schema(command_payload, COMMAND_SCHEMA_NAME)
        assert ok, f"COMMAND не валиден: {reason}"

        bus.publish(INPUT_COMMAND_TOPIC, command_payload)

        stored = await _wait_for_status(redis_client, DRONE_ID, "MOVING", timeout=10.0)
        assert float(stored["vx"]) == 5.0
        assert float(stored["vy"]) == 3.0
        print(f"   ✅ COMMAND обработан: status={stored['status']}, vx={stored['vx']}")

        # ── Шаг 3: Ждём пока Core обновит позицию ──
        print("3. Жду обновления позиции от Core...")
        original_lat = float(stored["lat"])
        original_lon = float(stored["lon"])

        # Core работает на 10Hz (каждые 0.1с), ждём 2 секунды для надёжного обновления
        await asyncio.sleep(2.0)

        key = state.get_drone_state_key(DRONE_ID)
        for i in range(10):
            stored = await redis_client.hgetall(key)
            if stored:
                s = state.normalize_state(stored)
                new_lat = float(s.get("lat", original_lat))
                new_lon = float(s.get("lon", original_lon))
                if new_lat != original_lat or new_lon != original_lon:
                    print(f"   ✅ Позиция обновлена: lat={new_lat:.6f}, lon={new_lon:.6f}")
                    break
            await asyncio.sleep(0.3)
        else:
            raise AssertionError(f"Позиция не изменилась за 3с (lat={original_lat}, lon={original_lon})")

        # ── Шаг 4: Запрос позиции через Messaging ──
        print("4. Запрос позиции через sitl.telemetry.request...")
        request = {
            "drone_id": DRONE_ID,
            "action": "request_position",
        }
        ok, reason = validate_schema(request, POSITION_REQUEST_SCHEMA_NAME)
        assert ok, f"Запрос не валиден: {reason}"

        bus.publish(REQUEST_TOPIC, request)

        response = await asyncio.wait_for(response_future, timeout=5.0)
        payload = response.get("payload", response)
        assert "lat" in payload
        assert "lon" in payload
        assert "alt" in payload
        print(f"   ✅ Координаты получены: lat={payload['lat']}, lon={payload['lon']}, alt={payload['alt']}")

        print("\n✅ Тест пройден! Полный жизненный цикл через топики работает")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass
        bus.stop()


async def test_command_without_home_is_rejected_via_topics() -> None:
    """Тест: команда без HOME отвергается Controller (проверка через Redis)."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key("drone_002")

    bus = _make_bus()

    try:
        # Убеждаемся что нет HOME
        stored = await redis_client.hgetall(drone_key)
        assert not stored, "Дрон не должен иметь состояния"

        # Отправляем команду через входной топик
        print("Отправляю COMMAND без HOME...")
        command_payload = {
            "drone_id": "drone_002",
            "vx": 5.0,
            "vy": 0.0,
            "vz": 0.0,
            "mag_heading": 0.0,
        }
        bus.publish(INPUT_COMMAND_TOPIC, command_payload)

        # Ждём — состояние НЕ должно появиться (или остаться без MOVING)
        await asyncio.sleep(3.0)
        stored = await redis_client.hgetall(drone_key)
        # Controller должен проигнорировать — состояния нет или status != MOVING
        if stored:
            s = state.normalize_state(stored)
            assert s.get("status") != "MOVING", f"Команда без HOME не должна изменить статус на MOVING, получено: {s}"
        print("✅ Тест пройден! Команда без HOME отвергнута")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass
        bus.stop()


async def test_invalid_command_rejected_by_verifier_via_topics() -> None:
    """Тест: невалидная команда отклоняется Verifier (не доходит до Controller)."""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    bus = _make_bus()

    try:
        # Сначала задаём HOME (иначе Controller всё равно отклонит)
        home_payload = {
            "drone_id": DRONE_ID,
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 120.0,
        }
        bus.publish(INPUT_HOME_TOPIC, home_payload)
        await _wait_for_home_in_redis(redis_client, DRONE_ID, timeout=10.0)

        # Отправляем невалидную команду
        print("Отправляю невалидную команду (vx=100)...")
        invalid_payload = {
            "drone_id": DRONE_ID,
            "vx": 100.0,  # Превышение лимита
            "vy": 0.0,
            "vz": 0.0,
            "mag_heading": 0.0,
        }
        bus.publish(INPUT_COMMAND_TOPIC, invalid_payload)

        # Ждём — состояние НЕ должно измениться на MOVING
        await asyncio.sleep(3.0)
        stored = await redis_client.hgetall(drone_key)
        s = state.normalize_state(stored)
        assert s.get("status") != "MOVING", "Невалидная команда не должна привести к MOVING"
        print("✅ Тест пройден! Невалидная команда отклонена Verifier")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass
        bus.stop()


if __name__ == "__main__":
    print(f"Интеграционные тесты: полный жизненный цикл (BROKER_BACKEND={BROKER_BACKEND})")
    print("=" * 70)
    try:
        asyncio.run(test_full_drone_lifecycle_via_topics())
        print()
        asyncio.run(test_command_without_home_is_rejected_via_topics())
        print()
        asyncio.run(test_invalid_command_rejected_by_verifier_via_topics())
        print("\n✅ Все интеграционные тесты пройдены!")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
