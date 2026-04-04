import asyncio
import os
from typing import Any

from broker import create_broker_client_from_env
from broker import iter_broker_messages_with_retry
from broker import start_broker_with_retry

from contracts import classify_input_topic
from contracts import parse_json_payload
from contracts import resolve_verified_topic
from contracts import validate_schema
from contracts import VERIFIED_COMMAND_TOPIC_DEFAULT
from contracts import VERIFIED_HOME_TOPIC_DEFAULT
from infopanel_client import create_infopanel_client_from_env

infopanel = create_infopanel_client_from_env()


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

    broker = create_broker_client_from_env()

    await infopanel.start()
    await start_broker_with_retry(broker, infopanel, "Verifier broker")

    infopanel.log_event(
        f"Verifier started. input={input_topics} "
        f"verified_commands={verified_commands_topic} verified_home={verified_home_topic}",
        "info"
    )

    try:
        async for msg in iter_broker_messages_with_retry(
                broker,
                input_topics,
                infopanel,
                "Verifier broker",
                group_id="SITL-verifier-v1",
        ):
            ok, message_type, payload, reason = process_input_message(
                msg.topic,
                msg.payload,
                commands_topic,
                home_topic,
            )
            if not ok or message_type is None or payload is None:
                infopanel.log_event(
                    f"Rejected message from topic={msg.topic}: {reason}",
                    "warning"
                )
                continue

            output_topic = resolve_verified_topic(
                message_type,
                verified_commands_topic,
                verified_home_topic,
            )
            await broker.publish(output_topic, payload)
            infopanel.log_event(
                f"Verified message_type={message_type} drone_id={payload['drone_id']} "
                f"output_topic={output_topic}",
                "info"
            )
    finally:
        await broker.stop()
        await infopanel.stop()


if __name__ == "__main__":
    asyncio.run(main())