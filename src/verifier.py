import asyncio
import json
import logging
import math
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


def to_float(value: Any) -> float | None:
    # Безопасно приводим значение к float
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
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


def parse_nmea_parts(sentence: str) -> Tuple[str, list[str]]:
    # Разбираем NMEA строку на тип сообщения и поля без checksum
    text = sentence.strip()
    star_index = text.find("*")
    if not text.startswith("$") or star_index == -1:
        return "", []

    body = text[1:star_index]
    parts = body.split(",")
    if not parts:
        return "", []

    return parts[0].upper(), parts


def nmea_to_decimal(coord_raw: Any, hemisphere_raw: Any) -> float | None:
    # Переводим координаты NMEA вида ddmm.mmmm / dddmm.mmmm в decimal degrees
    coord = to_float(coord_raw)
    if coord is None:
        return None

    hemisphere = str(hemisphere_raw).strip().upper()
    if hemisphere not in {"N", "S", "E", "W"}:
        return None

    degrees = int(coord // 100)
    minutes = coord - degrees * 100
    decimal = degrees + minutes / 60

    if hemisphere in {"S", "W"}:
        decimal *= -1

    return round(decimal, 7)


def extract_position_from_nmea(sentence: str) -> dict[str, float] | None:
    # Извлекаем позицию из сообщений GGA или RMC
    msg_type, parts = parse_nmea_parts(sentence)
    if not msg_type or not parts:
        return None

    lat = None
    lon = None

    if msg_type.endswith("GGA") and len(parts) > 5:
        lat = nmea_to_decimal(parts[2], parts[3])
        lon = nmea_to_decimal(parts[4], parts[5])
    elif msg_type.endswith("RMC") and len(parts) > 6:
        lat = nmea_to_decimal(parts[3], parts[4])
        lon = nmea_to_decimal(parts[5], parts[6])

    if lat is None or lon is None:
        return None

    return {"lat": lat, "lon": lon}


def extract_course_from_nmea(sentence: str) -> float | None:
    # Извлекаем курс из RMC (поле course over ground)
    msg_type, parts = parse_nmea_parts(sentence)
    if msg_type.endswith("RMC") and len(parts) > 8:
        return to_float(parts[8])
    return None


def course_to_vector(course_deg: float) -> dict[str, float]:
    # Переводим курс (градусы от севера по часовой стрелке) в unit-вектор x/y
    radians = math.radians(course_deg)
    vector_x = round(math.sin(radians), 6)
    vector_y = round(math.cos(radians), 6)
    return {"x": vector_x, "y": vector_y}


def extract_direction(payload: dict[str, Any]) -> dict[str, Any] | None:
    # Пытаемся получить направление из payload или из NMEA
    direction_obj = payload.get("direction")
    if isinstance(direction_obj, dict):
        course_from_direction = to_float(direction_obj.get("course_deg"))
        if course_from_direction is not None:
            return {
                "course_deg": course_from_direction,
                "vector_unit": course_to_vector(course_from_direction),
            }

        dir_x = to_float(direction_obj.get("x"))
        dir_y = to_float(direction_obj.get("y"))
        if dir_x is not None and dir_y is not None:
            return {"course_deg": None, "vector_unit": {"x": dir_x, "y": dir_y}}

    course_deg = to_float(payload.get("course_deg"))
    if course_deg is None:
        course_deg = to_float(payload.get("heading_deg"))
    if course_deg is None:
        nmea = payload.get("nmea")
        if isinstance(nmea, str):
            course_deg = extract_course_from_nmea(nmea)

    if course_deg is None:
        return None

    return {"course_deg": course_deg, "vector_unit": course_to_vector(course_deg)}


def enrich_payload_with_command(payload: dict[str, Any]) -> Tuple[bool, dict[str, Any], str]:
    # Добавляем в payload стартовую позицию и направление в явном виде
    normalized = dict(payload)

    start_position = None
    raw_start = payload.get("start_position")
    if isinstance(raw_start, dict):
        lat = to_float(raw_start.get("lat"))
        lon = to_float(raw_start.get("lon"))
        if lat is not None and lon is not None:
            start_position = {"lat": lat, "lon": lon}

    if start_position is None:
        nmea = payload.get("nmea")
        if isinstance(nmea, str):
            start_position = extract_position_from_nmea(nmea)

    if start_position is None:
        return False, normalized, "cannot extract start position from payload/nmea"

    direction = extract_direction(payload)
    if direction is None:
        return False, normalized, "cannot extract direction from payload/nmea"

    normalized["start_position"] = start_position
    normalized["direction"] = direction

    return True, normalized, ""


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

            # Добавляем в сообщение явные поля start_position и direction
            ok, normalized_payload, reason = enrich_payload_with_command(payload)
            if not ok:
                log.warning("Rejected message from topic=%s: %s", msg.topic, reason)
                continue

            # Формируем "очищенное" сообщение для следующего компонента
            verified = {
                "data": normalized_payload,
                "message_type": message_type,
                "input_topic": msg.topic,
                "verifier_stage": "SITL-v1",
                "verified_at": datetime.now(timezone.utc).isoformat()
            }
            # Публикуем только валидные сообщения в output_topic
            await producer.send_and_wait(output_topic, verified)
            log.info(
                "Verified message_type=%s drone_id=%s message_id=%s start=%s direction=%s",
                message_type,
                normalized_payload.get("drone_id"),
                normalized_payload.get("message_id"),
                normalized_payload.get("start_position"),
                normalized_payload.get("direction"),
            )
    finally:
        await producer.stop()
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
