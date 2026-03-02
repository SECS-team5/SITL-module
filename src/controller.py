import asyncio
import json
import logging
import os
import math
from datetime import datetime, timezone
from typing import Any
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)


def calculate_new_position(
        lat: float,
        lon: float,
        vector_x: float,
        vector_y: float,
        speed_mps: float,
        delta_time_sec: float
) -> dict[str, float]:
    """
    Рассчитывает новую позицию дрона на основе текущей позиции, вектора направления и скорости.

    Args:
        lat: текущая широта (decimal degrees)
        lon: текущая долгота (decimal degrees)
        vector_x: компонента X единичного вектора направления (восток-запад)
        vector_y: компонента Y единичного вектора направления (север-юг)
        speed_mps: скорость в метрах в секунду
        delta_time_sec: время с последнего обновления в секундах

    Returns:
        dict с новыми lat и lon
    """
    # Расстояние, пройденное за delta_time_sec
    distance_m = speed_mps * delta_time_sec

    # Примерные коэффициенты для перевода метров в градусы
    # 1 градус широты ≈ 111,111 метров
    # 1 градус долготы ≈ 111,111 * cos(lat) метров
    meters_per_degree_lat = 111111.0
    meters_per_degree_lon = 111111.0 * math.cos(math.radians(lat))

    # Рассчитываем смещение в градусах
    delta_lat = (vector_y * distance_m) / meters_per_degree_lat
    delta_lon = (vector_x * distance_m) / meters_per_degree_lon

    # Возвращаем новые координаты с округлением до 7 знаков
    return {
        "lat": round(lat + delta_lat, 7),
        "lon": round(lon + delta_lon, 7)
    }


def get_drone_key(drone_id: str) -> str:
    """Формирует ключ Redis для дрона."""
    return f"SITL:DRONE:{drone_id}"


async def update_drone_position(r: redis.Redis, drone_id: str, update_interval_sec: float):
    """
    Обновляет позицию одного дрона на основе его направления и скорости.

    Args:
        r: Redis клиент
        drone_id: ID дрона
        update_interval_sec: интервал обновления в секундах (0.1 для 10 Гц)
    """
    key = get_drone_key(drone_id)

    # Читаем текущее состояние дрона из Redis
    data_raw = await r.get(key)
    if data_raw is None:
        return

    # Парсим JSON
    try:
        drone_data = json.loads(data_raw)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in Redis for drone_id=%s", drone_id)
        return

    # Извлекаем необходимые данные для расчёта
    start_position = drone_data.get("start_position")
    direction = drone_data.get("direction")
    speed_mps = drone_data.get("speed_mps", 0.0)

    # Без позиции или направления расчёт невозможен
    if not start_position or not direction:
        return

    lat = start_position.get("lat")
    lon = start_position.get("lon")
    vector_unit = direction.get("vector_unit")

    # Проверяем наличие всех компонентов
    if lat is None or lon is None or not vector_unit:
        return

    vector_x = vector_unit.get("x", 0.0)
    vector_y = vector_unit.get("y", 0.0)

    # Рассчитываем новую позицию
    new_position = calculate_new_position(
        lat, lon, vector_x, vector_y, speed_mps, update_interval_sec
    )

    # Обновляем start_position новыми координатами
    drone_data["start_position"] = new_position
    drone_data["last_update"] = datetime.now(timezone.utc).isoformat()

    # Сохраняем обратно в Redis с TTL 2 часа
    await r.set(key, json.dumps(drone_data), ex=7200)


async def position_updater_task(r: redis.Redis, update_hz: float = 10.0):
    """
    Фоновая задача для обновления позиций всех дронов с частотой 10 Гц.

    Args:
        r: Redis клиент
        update_hz: частота обновления в Гц (по умолчанию 10)
    """
    update_interval_sec = 1.0 / update_hz
    log.info("Position updater started with frequency %.1f Hz", update_hz)

    while True:
        try:
            # Собираем все ID дронов из Redis по паттерну
            drone_ids = []
            cursor = 0

            # Используем SCAN для итерации по ключам (не блокирует Redis)
            while True:
                cursor, keys = await r.scan(cursor, match="SITL:DRONE:*", count=100)
                for key in keys:
                    # Извлекаем drone_id из ключа "SITL:DRONE:drone_id"
                    key_str = key.decode() if isinstance(key, bytes) else key
                    drone_id = key_str.replace("SITL:DRONE:", "")
                    drone_ids.append(drone_id)

                # cursor == 0 означает конец итерации
                if cursor == 0:
                    break

            # Обновляем позицию каждого дрона
            for drone_id in drone_ids:
                await update_drone_position(r, drone_id, update_interval_sec)

            # Ждём до следующего цикла обновления
            await asyncio.sleep(update_interval_sec)

        except Exception as e:
            log.error("Error in position updater: %s", e, exc_info=True)
            await asyncio.sleep(update_interval_sec)


async def process_command(r: redis.Redis, verified_message: dict[str, Any]):
    """
    Обрабатывает команду из верификатора.
    Определяет: существует ли дрон в Redis?
    - Если нет: создаёт новую запись
    - Если да: обновляет направление

    Args:
        r: Redis клиент
        verified_message: верифицированное сообщение из Kafka
    """
    data = verified_message.get("data", {})
    message_type = verified_message.get("message_type")

    # Извлекаем drone_id
    drone_id = data.get("drone_id")
    if not drone_id:
        log.warning("Missing drone_id in verified message")
        return

    key = get_drone_key(drone_id)

    # Проверяем, существует ли дрон в Redis
    existing_data_raw = await r.get(key)

    # Извлекаем позицию и направление из сообщения
    start_position = data.get("start_position")
    direction = data.get("direction")

    if existing_data_raw is None:
        # Дрон не существует - создаём новую запись

        # Для нового дрона обязательны start_position и direction
        if not start_position or not direction:
            log.warning("Missing start_position or direction for new drone_id=%s", drone_id)
            return

        # Формируем запись дрона
        drone_data = {
            "drone_id": drone_id,
            "message_type": message_type,
            "start_position": start_position,
            "direction": direction,
            "speed_mps": data.get("speed_mps", 5.0),  # Скорость по умолчанию 5 м/с
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_update": datetime.now(timezone.utc).isoformat()
        }

        # Сохраняем в Redis с TTL 2 часа
        await r.set(key, json.dumps(drone_data), ex=7200)
        log.info(
            "✨ Created drone: id=%s pos=%s dir=%s",
            drone_id, start_position, direction
        )
    else:
        # Дрон существует - обновляем направление

        # Парсим существующие данные
        try:
            drone_data = json.loads(existing_data_raw)
        except json.JSONDecodeError:
            log.warning("Invalid JSON in Redis for drone_id=%s", drone_id)
            return

        # Обновляем направление, если оно есть в команде
        if direction:
            drone_data["direction"] = direction
            drone_data["last_update"] = datetime.now(timezone.utc).isoformat()

            # Обновляем скорость, если она указана в команде
            if "speed_mps" in data:
                drone_data["speed_mps"] = data["speed_mps"]

            # Сохраняем обновлённые данные
            await r.set(key, json.dumps(drone_data), ex=7200)
            log.info(
                "🔄 Updated drone: id=%s new_dir=%s type=%s",
                drone_id, direction, message_type
            )


async def command_processor_task(r: redis.Redis, input_topic: str, kafka_servers: str):
    """
    Фоновая задача для обработки команд из Kafka.
    Слушает топик с верифицированными сообщениями и обрабатывает их.

    Args:
        r: Redis клиент
        input_topic: топик для чтения верифицированных сообщений
        kafka_servers: адреса Kafka брокеров
    """
    # Создаём Kafka consumer
    consumer = AIOKafkaConsumer(
        input_topic,
        bootstrap_servers=kafka_servers,
        group_id="SITL-drone-controller-v1",
        value_deserializer=lambda m: json.loads(m.decode())
    )

    await consumer.start()
    log.info("Command processor started, topic=%s", input_topic)

    try:
        # Читаем сообщения из Kafka
        async for msg in consumer:
            verified_message = msg.value

            # Проверяем метку верификатора - принимаем только проверенные сообщения
            if verified_message.get("verifier_stage") != "SITL-v1":
                log.warning("❌ Invalid verifier stamp")
                continue

            # Обрабатываем команду
            await process_command(r, verified_message)

    except Exception as e:
        log.error("Error in command processor: %s", e, exc_info=True)
    finally:
        await consumer.stop()


async def main():
    # Читаем конфигурацию из переменных окружения
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    kafka_servers = os.getenv("KAFKA_SERVERS", "kafka:9092")
    input_topic = os.getenv("INPUT_TOPIC", "sitl-verified-messages")
    update_hz = float(os.getenv("UPDATE_FREQUENCY_HZ", "10.0"))

    # Создаём подключение к Redis
    r = redis.from_url(redis_url)

    log.info("🚁 Drone Controller started")
    log.info("📡 Redis: %s", redis_url)
    log.info("📡 Kafka: %s", kafka_servers)
    log.info("📥 Input topic: %s", input_topic)
    log.info("⚡ Update frequency: %.1f Hz", update_hz)

    try:
        # Запускаем две параллельные задачи:
        # 1. Position Updater - обновляет координаты с частотой 10 Гц
        # 2. Command Processor - обрабатывает команды из Kafka
        await asyncio.gather(
            position_updater_task(r, update_hz),
            command_processor_task(r, input_topic, kafka_servers)
        )
    finally:
        await r.aclose()
        log.info("🛑 Drone Controller stopped")


if __name__ == "__main__":
    asyncio.run(main())