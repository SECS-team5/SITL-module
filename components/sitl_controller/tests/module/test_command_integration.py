# tests/integration/test_command_integration.py
import asyncio
import json
import os
import pathlib
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT.parent / "src"
TESTS = ROOT / "tests"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

import redis.asyncio as redis
import state

BROKER_BACKEND = os.environ.get("BROKER_BACKEND", "kafka")
if BROKER_BACKEND == "mqtt":
    import aiomqtt
else:
    from aiokafka import AIOKafkaProducer

INPUT_COMMAND_TOPIC = os.getenv("COMMAND_TOPIC", "sitl.commands")
VERIFIED_COMMAND_TOPIC = os.getenv("VERIFIED_COMMAND_TOPIC", "sitl.verified-commands")
STATE_TTL_SEC = 7200
DRONE_ID = "drone_001"


async def send_message_via_kafka(payload: dict[str, Any], topic: str) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_SERVERS", "kafka:29092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        await producer.send_and_wait(topic, payload)
    finally:
        await producer.stop()


async def send_message_via_mqtt(payload: dict[str, Any], topic: str) -> None:
    host = os.environ.get("MQTT_HOST", "mosquitto")
    port = int(os.environ.get("MQTT_PORT", "1883"))
    async with aiomqtt.Client(hostname=host, port=port) as client:
        await client.publish(topic, json.dumps(payload).encode("utf-8"))


async def send_message(payload: dict[str, Any], topic: str) -> None:
    if BROKER_BACKEND == "mqtt":
        await send_message_via_mqtt(payload, topic)
    else:
        await send_message_via_kafka(payload, topic)


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
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url)
    
    drone_key = state.get_drone_state_key(DRONE_ID)
    
    command_payload = {
        "drone_id": DRONE_ID,
        "vx": 5.0,
        "vy": 3.0,
        "vz": 1.5,
        "mag_heading": 45.0,
    }
    
    try:
        print(f"🏠 Создаю HOME состояние для {DRONE_ID}")
        await _setup_drone_home_state(redis_client, DRONE_ID)
        await asyncio.sleep(0.5)
        
        print(f"📤 Отправляю команду в {INPUT_COMMAND_TOPIC}")
        await send_message(command_payload, INPUT_COMMAND_TOPIC)
        
        print("⏳ Ожидаю обработку команды...")
        max_wait = 15
        poll_interval = 0.5
        stored_data = None
        
        for attempt in range(int(max_wait / poll_interval)):
            try:
                stored_data = await redis_client.hgetall(drone_key)
                if stored_data:
                    raw_status = stored_data.get(b"status") or stored_data.get("status")
                    status = raw_status.decode() if isinstance(raw_status, bytes) else raw_status
                    if status == "MOVING":
                        print(f"✅ Статус MOVING получен через {attempt * poll_interval:.1f}с")
                        break
            except Exception:
                pass
            await asyncio.sleep(poll_interval)
        else:
            print(f"❌ Данные не обновились за {max_wait}с")
        
        assert stored_data, f"❌ Ключ {drone_key} не найден"
        
        stored_state = state.normalize_state(stored_data)
        print(f"🔍 Состояние: status={stored_state.get('status')}, vx={stored_state.get('vx')}")
        
        assert stored_state["status"] == "MOVING"
        assert float(stored_state["vx"]) == 5.0
        assert float(stored_state["vy"]) == 3.0
        assert float(stored_state["vz"]) == 1.5
        assert float(stored_state["mag_heading"]) == 45.0
        
        print("✅ Тест пройден! Команда обработана")
        
    finally:
        # Гарантированная очистка
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass


async def test_invalid_command_is_rejected() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url)
    drone_key = state.get_drone_state_key(DRONE_ID)
    
    invalid_payload = {
        "drone_id": DRONE_ID,
        "vx": 100.0,  # Превышение лимита
        "vy": 0.0,
        "vz": 0.0,
        "mag_heading": 0.0,
    }
    
    try:
        await _setup_drone_home_state(redis_client, DRONE_ID)
        await asyncio.sleep(0.5)
        
        initial_data = await redis_client.hgetall(drone_key)
        initial_vx = initial_data.get(b"vx") or initial_data.get("vx")
        
        print(f"📤 Отправляю НЕВАЛИДНУЮ команду")
        await send_message(invalid_payload, INPUT_COMMAND_TOPIC)
        
        await asyncio.sleep(3.0)
        
        final_data = await redis_client.hgetall(drone_key)
        final_vx = final_data.get(b"vx") or final_data.get("vx")
        
        assert initial_vx == final_vx, "Состояние изменилось при невалидной команде!"
        
        print("✅ Тест пройден! Невалидная команда отклонена")
        
    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    print(f"🚀 Тесты команд (BROKER_BACKEND={BROKER_BACKEND})")
    print("-" * 60)
    try:
        asyncio.run(test_command_updates_drone_state_in_redis())
        print()
        asyncio.run(test_invalid_command_is_rejected())
        print("\n🎉 Все тесты команд пройдены! ✅")
    except Exception as e:
        print(f"\n💥 Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)