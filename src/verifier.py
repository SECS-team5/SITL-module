import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from typing import Iterable
from typing import Tuple
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)


def parse_csv_env(name: str, default: str = "") -> list[str]:
    # Читаем строку из env и превращаем "a,b,c" в список ["a","b","c"]
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def parse_json_payload(raw: Any) -> dict[str, Any] | None:
    
    # Если мы получили уже dict, возвращаем его
    if isinstance(raw, dict):
        return raw
    
    # Если мы получили bytes - декодируем, парсим json.loads
    if isinstance(raw, (bytes, bytearray)):
        try:
            decoded = raw.decode()
            parsed = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    
    # Если не получилось декодировать (напр., сообщение было битое) - возвращаем None
    return None


def has_non_empty(payload: dict[str, Any], field: str) -> bool:
    # Проверка на то что обязательные поля не пусты
    value = payload.get(field)
    if value is None:
        return False
    return str(value).strip() != ""


def calculate_nmea_checksum(body: str) -> int:
    # расчёт checksum для NMEA
    checksum = 0
    for char in body:
        checksum ^= ord(char)
    return checksum


def is_valid_nmea_gn(sentence: Any) -> bool:
    # Проверка на формат NMEA
    
    # Проверяем, что строка
    if not isinstance(sentence, str):
        return False

    # Проверяем, что начинается с $GN
    text = sentence.strip()
    if not text.startswith("$GN"):
        return False

    # Ищем *
    star_index = text.find("*")
    if star_index == -1:
        return False

    body = text[1:star_index]
    provided = text[star_index + 1 : star_index + 3]

    if len(provided) != 2:
        return False

    try:
        expected = int(provided, 16)
    except ValueError:
        return False

    # Считаем checksum
    actual = calculate_nmea_checksum(body)
    
    # Сравниваем нашу контрольную сумму с ожидаемой
    return actual == expected


def validate_required_fields(payload: dict[str, Any]) -> Tuple[bool, str]:
    # Проверяем обязательные поля для сообщений
    required = ("drone_id", "message_id", "nmea")
    for field in required:
        if not has_non_empty(payload, field):
            return False, f"missing or empty field '{field}'"

    # Проверяем корректность NMEA формата
    if not is_valid_nmea_gn(payload["nmea"]):
        return False, "invalid NMEA sentence, expected GN source with valid checksum"

    return True, ""


def classify_message(topic: str, home_topic: str, payload: dict[str, Any]) -> Tuple[bool, str, str]:
    # Читаем поле type, если оно есть в payload
    msg_type_raw = str(payload.get("type", "")).strip().upper()

    # Все сообщения из home_topic считаем типом HOME
    if topic == home_topic:
        return True, "HOME", ""

    # Если сообщение помечено как HOME, но пришло не из home_topic - отклоняем
    if msg_type_raw == "HOME":
        return False, "", "HOME message in non-HOME topic"

    # Все остальные сообщения считаем DRONE_INFO
    return True, "DRONE_INFO", ""


def source_is_trusted(payload: dict[str, Any], trusted_sources: Iterable[str]) -> bool:
    # Готовим whitelist доверенных источников в нижнем регистре
    trusted_lower = {item.lower() for item in trusted_sources}
    source = str(payload.get("source", "")).strip().lower()

    # Если whitelist пуст, не блокируем сообщение
    if not trusted_lower:
        return True

    # Проверяем, входит ли source сообщения в whitelist
    return source in trusted_lower


async def main():
    # Читаем конфигурацию топиков и брокера из env
    kafka_servers = os.getenv("KAFKA_SERVERS", "kafka:9092")
    input_topics = parse_csv_env("INPUT_TOPICS", "sitl-drone-info,sitl-drone-home")
    home_topic = os.getenv("HOME_TOPIC", "sitl-drone-home")
    output_topic = os.getenv("OUTPUT_TOPIC", "sitl-verified-messages")
    trusted_home_sources = parse_csv_env("TRUSTED_HOME_SOURCES")

    # Продюсер отправляет только проверенные сообщения
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode()
    )

    # Без входных топиков запускать фильтр нельзя
    if not input_topics:
        raise RuntimeError("INPUT_TOPICS cannot be empty")

    # Консьюмер слушает сразу несколько входных топиков
    consumer = AIOKafkaConsumer(
        *input_topics,
        bootstrap_servers=kafka_servers,
        group_id="SITL-verifier-v1",
    )

    await producer.start()
    await consumer.start()
    log.info("Verifier started. Listening topics=%s; home_topic=%s", input_topics, home_topic)

    try:
        async for msg in consumer:
            # Пробуем привести входящее сообщение к dict
            payload = parse_json_payload(msg.value)
            if payload is None:
                log.warning("Rejected message from topic=%s: invalid JSON payload", msg.topic)
                continue

            # Определяем тип сообщения (HOME / DRONE_INFO) и проверяем связку с топиком
            ok, message_type, reason = classify_message(msg.topic, home_topic, payload)
            if not ok:
                log.warning("Rejected message from topic=%s: %s", msg.topic, reason)
                continue

            # Проверяем обязательные поля и NMEA
            ok, reason = validate_required_fields(payload)
            if not ok:
                log.warning("Rejected message from topic=%s: %s", msg.topic, reason)
                continue

            # Для HOME дополнительно проверяем доверенный источник
            if message_type == "HOME" and not source_is_trusted(payload, trusted_home_sources):
                log.warning("Rejected HOME message from topic=%s: untrusted source", msg.topic)
                continue

            # Формируем "очищенное" сообщение для следующего компонента
            verified = {
                "data": payload,
                "message_type": message_type,
                "input_topic": msg.topic,
                "verifier_stage": "SITL-v1",
                "verified_at": datetime.now(timezone.utc).isoformat()
            }
            # Публикуем только валидные сообщения в output_topic
            await producer.send_and_wait(output_topic, verified)
            log.info(
                "Verified message_type=%s drone_id=%s message_id=%s",
                message_type,
                payload.get("drone_id"),
                payload.get("message_id"),
            )
    finally:
        await producer.stop()
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
