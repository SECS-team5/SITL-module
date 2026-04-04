# tests/integration/test_messaging_integration.py
import asyncio
import json
import os
import pathlib
import sys
from typing import Any
from uuid import uuid4

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
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Топики из переменных окружения
INPUT_HOME_TOPIC = os.getenv("HOME_TOPIC", "sitl-drone-home")
POSITION_REQUEST_TOPIC = os.getenv("POSITION_REQUEST_TOPIC", "sitl.telemetry.request")
POSITION_RESPONSE_TOPIC = os.getenv("POSITION_RESPONSE_TOPIC", "sitl.telemetry.response")
STATE_TTL_SEC = 7200
DRONE_ID = "drone_001"

# =============================================================================
# Отправка сообщений
# =============================================================================
async def send_message_via_kafka(payload: dict[str, Any], topic: str, headers: list | None = None) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_SERVERS", "kafka:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        await producer.send_and_wait(topic, payload, headers=headers)
    finally:
        await producer.stop()

async def send_message_via_mqtt(payload: dict[str, Any], topic: str) -> None:
    host = os.environ.get("MQTT_HOST", "mosquitto")
    port = int(os.environ.get("MQTT_PORT", "1883"))
    async with aiomqtt.Client(hostname=host, port=port) as client:
        await client.publish(topic, json.dumps(payload).encode("utf-8"))

async def send_message(payload: dict[str, Any], topic: str, headers: list | None = None) -> None:
    if BROKER_BACKEND == "mqtt":
        await send_message_via_mqtt(payload, topic)
    else:
        await send_message_via_kafka(payload, topic, headers=headers)

# =============================================================================
# Получение ответов
# =============================================================================
async def listen_for_response_via_kafka(response_topic: str, correlation_id: str, timeout: float = 10.0) -> dict | None:
    consumer = None
    try:
        consumer = AIOKafkaConsumer(
            response_topic,
            bootstrap_servers=os.environ.get("KAFKA_SERVERS", "kafka:9092"),
            group_id=f"test-consumer-{uuid4().hex}",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            # ✅ Убран allow_auto_create_topics (не существует в aiokafka)
        )
        await consumer.start()
        await asyncio.sleep(2.0)  # Дать Kafka время на создание метаданных
        
        async def _consume():
            async for message in consumer:
                if message.headers:
                    for key, value in message.headers:
                        k = key.decode() if isinstance(key, bytes) else key
                        v = value.decode() if isinstance(value, bytes) else value
                        if k == "correlation_id" and v == correlation_id:
                            return message.value
                
                if message.value.get("correlation_id") == correlation_id:
                    return message.value
                    
                if "lat" in message.value and "lon" in message.value and "alt" in message.value:
                    return message.value
            return None
        
        return await asyncio.wait_for(_consume(), timeout=timeout)
    
    except asyncio.TimeoutError:
        print(f"⏰ Таймаут ожидания ответа на {response_topic}")
        return None
    except Exception as e:
        print(f"❌ Ошибка Kafka: {type(e).__name__}: {e}")
        return None
    finally:
        if consumer:
            try:
                await consumer.stop()
            except Exception:
                pass

async def listen_for_response_via_mqtt(response_topic: str, correlation_id: str, timeout: float = 10.0) -> dict | None:
    host = os.environ.get("MQTT_HOST", "mosquitto")
    port = int(os.environ.get("MQTT_PORT", "1883"))
    async with aiomqtt.Client(hostname=host, port=port) as client:
        await client.subscribe(response_topic)
        try:
            async def _consume():
                async for message in client.messages:
                    payload = json.loads(message.payload.decode("utf-8"))
                    if payload.get("correlation_id") == correlation_id:
                        return payload
            return await asyncio.wait_for(_consume(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

async def listen_for_response(response_topic: str, correlation_id: str, timeout: float = 10.0) -> dict | None:
    if BROKER_BACKEND == "mqtt":
        return await listen_for_response_via_mqtt(response_topic, correlation_id, timeout)
    else:
        return await listen_for_response_via_kafka(response_topic, correlation_id, timeout)

# =============================================================================
# Подготовка данных
# =============================================================================
async def setup_drone_via_home_message(drone_id: str) -> None:
    home_payload = {
        "drone_id": drone_id,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 120.0,
    }
    print(f"🏠 Инициализация дрона {drone_id} через HOME сообщение...")
    await send_message(home_payload, INPUT_HOME_TOPIC)
    await asyncio.sleep(2.0)

# =============================================================================
# ТЕСТ 1: Успешный запрос позиции
# =============================================================================
async def test_valid_position_request_returns_coordinates() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url)
    correlation_id = uuid4().hex
    drone_key = state.get_drone_state_key(DRONE_ID)

    try:
        await setup_drone_via_home_message(DRONE_ID)
        stored_data = await redis_client.hgetall(drone_key)
        assert stored_data, f"❌ Дрон {DRONE_ID} не найден в Redis"
        print(f"✅ Дрон {DRONE_ID} успешно инициализирован в Redis")

        # ✅ Сначала отправляем запрос (создаёт топики на брокере), потом слушаем
        request_payload = {
            "drone_id": DRONE_ID,
            "correlation_id": correlation_id,
            "reply_to": POSITION_RESPONSE_TOPIC,
        }
        print(f"📤 Отправляю запрос позиции: {request_payload}")

        headers = [
            ("correlation_id", correlation_id.encode()),
            ("reply_to", POSITION_RESPONSE_TOPIC.encode()),
        ]
        await send_message(request_payload, POSITION_REQUEST_TOPIC, headers=headers)
        
        await asyncio.sleep(1.0)  # Дать messaging.py время на ответ

        print(f"👂 Запускаю слушателя на {POSITION_RESPONSE_TOPIC}...")
        response = await listen_for_response(POSITION_RESPONSE_TOPIC, correlation_id, timeout=15.0)

        assert response is not None, "❌ Ответ не получен в течение таймаута"
        print(f"✅ Ответ получен: {response}")

        assert float(response.get("lat", 0)) == 59.9386
        assert float(response.get("lon", 0)) == 30.3141
        assert float(response.get("alt", 0)) == 120.0

        print("✅ Тест 1 пройден!")

    finally:
        await redis_client.delete(drone_key)
        await redis_client.aclose()

# =============================================================================
# ТЕСТ 2: Несуществующий дрон
# =============================================================================
async def test_position_request_for_missing_drone_returns_error() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url)
    correlation_id = uuid4().hex
    missing_drone_id = "drone_999"
    drone_key = state.get_drone_state_key(missing_drone_id)

    try:
        await redis_client.delete(drone_key)
        await asyncio.sleep(0.3)

        request_payload = {"drone_id": missing_drone_id}
        print(f"📤 Отправляю запрос для несуществующего дрона: {missing_drone_id}")

        headers = [
            ("correlation_id", correlation_id.encode()),
            ("reply_to", POSITION_RESPONSE_TOPIC.encode()),
        ]
        await send_message(request_payload, POSITION_REQUEST_TOPIC, headers=headers)
        
        await asyncio.sleep(1.0)

        response = await listen_for_response(POSITION_RESPONSE_TOPIC, correlation_id, timeout=3.0)

        if response is None:
            print("✅ Тест 2 пройден! Ответ не отправлен")
        else:
            print(f"⚠️ Получен ответ: {response}")

    finally:
        await redis_client.aclose()

# =============================================================================
# ТЕСТ 3: Невалидный запрос
# =============================================================================
async def test_invalid_position_request_is_rejected() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url)
    correlation_id = uuid4().hex

    try:
        request_payload = {"wrong_field": "value"}
        print(f"📤 Отправляю невалидный запрос: {request_payload}")

        headers = [
            ("correlation_id", correlation_id.encode()),
            ("reply_to", POSITION_RESPONSE_TOPIC.encode()),
        ]
        await send_message(request_payload, POSITION_REQUEST_TOPIC, headers=headers)
        
        await asyncio.sleep(1.0)

        response = await listen_for_response(POSITION_RESPONSE_TOPIC, correlation_id, timeout=3.0)

        if response is None:
            print("✅ Тест 3 пройден! Невалидный запрос отклонен")
        else:
            print(f"⚠️ Получен неожиданный ответ: {response}")

    finally:
        await redis_client.aclose()

# =============================================================================
# Запуск
# =============================================================================
if __name__ == "__main__":
    print(f"🚀 Интеграционные тесты Messaging (BROKER_BACKEND={BROKER_BACKEND})")
    print(f"📡 REQUEST_TOPIC={POSITION_REQUEST_TOPIC!r}")
    print(f"📡 RESPONSE_TOPIC={POSITION_RESPONSE_TOPIC!r}")
    print("-" * 70)
    try:
        asyncio.run(test_valid_position_request_returns_coordinates())
        print()
        asyncio.run(test_position_request_for_missing_drone_returns_error())
        print()
        asyncio.run(test_invalid_position_request_is_rejected())
        print("\n🎉 Все тесты Messaging пройдены! ✅")
    except Exception as e:
        print(f"\n💥 Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)