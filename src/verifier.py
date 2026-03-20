import asyncio
import json
import logging
import os
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer

from contracts import classify_input_topic
from contracts import parse_json_payload
from contracts import resolve_verified_topic
from contracts import validate_schema
from contracts import VERIFIED_COMMAND_TOPIC_DEFAULT
from contracts import VERIFIED_HOME_TOPIC_DEFAULT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def parse_csv_env(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def process_input_message(
    topic: str,
    raw_payload: Any,
    commands_topic: str,
    home_topic: str,
) -> tuple[bool, str | None, dict[str, Any] | None, str]:
    payload = parse_json_payload(raw_payload)
    if payload is None:
        return False, None, None, "invalid JSON payload"

    ok, message_type, schema_name = classify_input_topic(
        topic,
        commands_topic,
        home_topic,
    )
    if not ok:
        return False, None, None, schema_name

    ok, reason = validate_schema(payload, schema_name)
    if not ok:
        return False, None, None, reason

    return True, message_type, payload, ""


async def main() -> None:
    kafka_servers = os.getenv("KAFKA_SERVERS", "kafka:9092")
    commands_topic = os.getenv("COMMAND_TOPIC", "sitl.commands")
    home_topic = os.getenv("HOME_TOPIC", "sitl-drone-home")
    input_topics = parse_csv_env("INPUT_TOPICS", f"{commands_topic},{home_topic}")
    verified_commands_topic = os.getenv(
        "VERIFIED_COMMAND_TOPIC",
        VERIFIED_COMMAND_TOPIC_DEFAULT,
    )
    verified_home_topic = os.getenv(
        "VERIFIED_HOME_TOPIC",
        VERIFIED_HOME_TOPIC_DEFAULT,
    )

    if not input_topics:
        raise RuntimeError("INPUT_TOPICS cannot be empty")

    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    consumer = AIOKafkaConsumer(
        *input_topics,
        bootstrap_servers=kafka_servers,
        group_id="SITL-verifier-v1",
    )

    await producer.start()
    await consumer.start()
    log.info(
        "Verifier started. input=%s verified_commands=%s verified_home=%s",
        input_topics,
        verified_commands_topic,
        verified_home_topic,
    )

    try:
        async for msg in consumer:
            ok, message_type, payload, reason = process_input_message(
                msg.topic,
                msg.value,
                commands_topic,
                home_topic,
            )
            if not ok or message_type is None or payload is None:
                log.warning("Rejected message from topic=%s: %s", msg.topic, reason)
                continue

            output_topic = resolve_verified_topic(
                message_type,
                verified_commands_topic,
                verified_home_topic,
            )
            await producer.send_and_wait(output_topic, payload)
            log.info(
                "Verified message_type=%s drone_id=%s output_topic=%s",
                message_type,
                payload["drone_id"],
                output_topic,
            )
    finally:
        await producer.stop()
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
