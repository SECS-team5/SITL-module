# tests/integration/test_home_position_integration.py
import asyncio
import json
import os
import pathlib
import sys
from typing import Any

# Настройка путей для импортов
ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT.parent / "src"
TESTS = ROOT / "tests"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

import redis.asyncio as redis  # type: ignore
import state  # type: ignore

# Импорт брокера в зависимости от переменной окружения
BROKER_BACKEND = os.environ.get("BROKER_BACKEND", "kafka")

if BROKER_BACKEND == "mqtt":
    import aiomqtt  # type: ignore
else:
    from aiokafka import AIOKafkaProducer  # type: ignore


# 🎯 Топики: тест отправляет во ВХОДНОЙ, verifier публикует в ВЫХОДНОЙ, controller читает выходной
INPUT_HOME_TOPIC = "sitl-drone-home"           # ← Сюда отправляем мы
VERIFIED_HOME_TOPIC = os.getenv("VERIFIED_HOME_TOPIC", "sitl.verified-home")  # ← Сюда пишет verifier, читает controller

STATE_TTL_SEC = 7200
DRONE_ID = "drone_001"  # ✅ Соответствует паттерну ^drone_[0-9]{3,4}$


async def send_message_via_kafka(payload: dict[str, Any], topic: str) -> None:
    """Отправляет сообщение в Kafka."""
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
    """Отправляет сообщение в MQTT."""
    host = os.environ.get("MQTT_HOST", "mosquitto")
    port = int(os.environ.get("MQTT_PORT", "1883"))
    async with aiomqtt.Client(hostname=host, port=port) as client:
        await client.publish(topic, json.dumps(payload).encode("utf-8"))


async def send_message(payload: dict[str, Any], topic: str) -> None:
    """Отправляет сообщение через правильный брокер."""
    if BROKER_BACKEND == "mqtt":
        await send_message_via_mqtt(payload, topic)
    else:
        await send_message_via_kafka(payload, topic)


async def test_home_position_sets_drone_state_in_redis() -> None:
    """
    Интеграционный тест: входное сообщение → verifier → controller → Redis.
    
    Поток данных:
    1. Тест → INPUT_HOME_TOPIC ("sitl-drone-home")
    2. Verifier: валидация → VERIFIED_HOME_TOPIC ("sitl.verified-home")  
    3. Controller: обработка → запись в Redis
    4. Тест: проверка Redis
    """
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url)
    
    drone_key = state.get_drone_state_key(DRONE_ID)
    
    # ✅ Валидный payload по схеме schemas/sitl-drone-home.json
    home_payload = {
        "drone_id": DRONE_ID,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 120.0,
    }
    
    try:
        # 1. Очистка перед тестом
        print(f"🧹 Очищаю ключ: {drone_key}")
        await redis_client.delete(drone_key)
        await asyncio.sleep(0.3)
        
        # 2. Отправка сообщения во ВХОДНОЙ топик (для verifier)
        print(f"📤 Отправляю в INPUT_TOPIC={INPUT_HOME_TOPIC}: {home_payload}")
        await send_message(home_payload, INPUT_HOME_TOPIC)
        
        # 3. Ожидание полной обработки: verifier → controller → Redis
        # Используем активный поллинг вместо фиксированного sleep
        print("⏳ Ожидаю обработку (verifier → controller → Redis)...")
        max_wait = 20  # секунд (две цепочки обработки)
        poll_interval = 0.5
        stored_data = None
        
        for attempt in range(int(max_wait / poll_interval)):
            stored_data = await redis_client.hgetall(drone_key)
            if stored_data:
                print(f"✅ Данные появились после {attempt * poll_interval:.1f}с")
                break
            await asyncio.sleep(poll_interval)
        else:
            # Если не появилось — отладочная информация
            print(f"❌ Данные не появились за {max_wait}с")
            all_keys = await redis_client.keys("drone:*")
            print(f"🔍 Все ключи 'drone:*' в Redis: {all_keys}")
            
            # Проверка, живы ли сервисы
            print(f"🔍 VERIFIED_HOME_TOPIC={VERIFIED_HOME_TOPIC}")
            print(f"🔍 INPUT_HOME_TOPIC={INPUT_HOME_TOPIC}")
        
        # 4. Проверка: состояние сохранено в Redis
        assert stored_data, (
            f"❌ Ключ {drone_key} не найден в Redis. "
            "Возможные причины:\n"
            "  • Verifier не запущен или не читает INPUT_HOME_TOPIC\n"
            "  • Controller не читает VERIFIED_HOME_TOPIC\n"
            "  • Ошибка валидации схемы (проверьте schemas/sitl-drone-home.json)\n"
            "  • Разные экземпляры Redis у теста и сервисов"
        )
        
        # 🔍 Отладка: покажем сырые данные из Redis
        print(f"🔍 Raw keys: {list(stored_data.keys())}")
        print(f"🔍 Sample values: {dict(list(stored_data.items())[:5])}")
        
        # 5. Нормализация и проверка полей
        stored_state = state.normalize_state(stored_data)
        print(f"🔍 Normalized keys: {list(stored_state.keys())}")
        
        # ✅ Проверяем ТОЛЬКО поля, которые controller действительно сохраняет:
        # ❌ drone_id НЕ проверяем — controller использует его только для ключа, не сохраняет в хеше
        
        assert stored_state["status"] == "ARMED", f"Статус: ожидался ARMED, получен {stored_state.get('status')}"
        assert float(stored_state["lat"]) == 59.9386, f"lat: {stored_state.get('lat')}"
        assert float(stored_state["lon"]) == 30.3141, f"lon: {stored_state.get('lon')}"
        assert float(stored_state["alt"]) == 120.0, f"alt: {stored_state.get('alt')}"
        assert float(stored_state["home_lat"]) == 59.9386
        assert float(stored_state["home_lon"]) == 30.3141
        assert float(stored_state["home_alt"]) == 120.0
        assert "last_update" in stored_state, "Поле last_update отсутствует"
        assert stored_state["vx"] == 0.0
        assert stored_state["vy"] == 0.0
        assert stored_state["vz"] == 0.0
        
        # 6. Проверка TTL
        ttl = await redis_client.ttl(drone_key)
        print(f"⏱️ TTL: {ttl}с")
        assert 0 < ttl <= STATE_TTL_SEC, f"TTL {ttl} вне диапазона (0, {STATE_TTL_SEC}]"
        
        print(f"✅ Тест пройден! Дрон {DRONE_ID} в Redis, TTL={ttl}с")
        return True
        
    except AssertionError as e:
        print(f"❌ Тест не пройден: {e}")
        raise
    finally:
        await redis_client.delete(drone_key)
        await redis_client.aclose()
        print("🧹 Ключ очищен")


async def test_invalid_home_message_is_rejected() -> None:
    """
    Тест: невалидное сообщение отклоняется verifier'ом и не попадает в Redis.
    """
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url)
    drone_key = state.get_drone_state_key(DRONE_ID)
    
    # ❌ Невалидный payload: отсутствует home_alt
    invalid_payload = {
        "drone_id": DRONE_ID,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        # home_alt отсутствует → валидация не пройдёт
    }
    
    try:
        await redis_client.delete(drone_key)
        await asyncio.sleep(0.3)
        
        print(f"📤 Отправляю НЕВАЛИДНОЕ сообщение: {invalid_payload}")
        await send_message(invalid_payload, INPUT_HOME_TOPIC)
        
        # Ждём чуть дольше — verifier должен отклонить, controller не получит
        await asyncio.sleep(5.0)
        
        stored_data = await redis_client.hgetall(drone_key)
        assert not stored_data, "❌ Невалидное сообщение не должно было сохраниться"
        
        print("✅ Тест пройден! Невалидное сообщение отклонено")
        return True
        
    finally:
        await redis_client.delete(drone_key)
        await redis_client.aclose()


if __name__ == "__main__":
    print(f"🚀 Интеграционные тесты (BROKER_BACKEND={BROKER_BACKEND})")
    print(f"📡 INPUT_HOME_TOPIC={INPUT_HOME_TOPIC}")
    print(f"📡 VERIFIED_HOME_TOPIC={VERIFIED_HOME_TOPIC}")
    print(f"🔑 DRONE_ID={DRONE_ID}")
    print("-" * 70)
    
    try:
        asyncio.run(test_home_position_sets_drone_state_in_redis())
        print()
        asyncio.run(test_invalid_home_message_is_rejected())
        print("\n🎉 Все тесты пройдены! ✅")
    except Exception as e:
        print(f"\n💥 Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)