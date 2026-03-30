import asyncio
import logging
import os
from typing import Any

import redis.asyncio as redis
from broker import create_broker_client_from_env
from broker import iter_broker_messages_with_retry
from broker import start_broker_with_retry

from contracts import classify_input_topic
from contracts import parse_json_payload
from contracts import validate_schema
from contracts import VERIFIED_COMMAND_TOPIC_DEFAULT
from contracts import VERIFIED_HOME_TOPIC_DEFAULT
from state import apply_command_update
from state import build_home_state
from state import get_drone_state_key
from state import normalize_state
from state import serialize_state
from state import state_has_home

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


async def persist_state(
    r: redis.Redis,
    drone_id: str,
    state: dict[str, Any],
    state_ttl_sec: int,
) -> None:
    state_key = get_drone_state_key(drone_id)
    await r.hset(state_key, mapping=serialize_state(state))
    if state_ttl_sec > 0:
        await r.expire(state_key, state_ttl_sec)


async def process_verified_message(
    r: redis.Redis,
    topic: str,
    payload: dict[str, Any],
    verified_commands_topic: str,
    verified_home_topic: str,
    state_ttl_sec: int,
) -> bool:
    ok, message_type, schema_name = classify_input_topic(
        topic,
        verified_commands_topic,
        verified_home_topic,
    )
    if not ok:
        log.warning("Rejected verified message from topic=%s: %s", topic, schema_name)
        return False

    ok, reason = validate_schema(payload, schema_name)
    if not ok:
        log.warning("Rejected verified message from topic=%s: %s", topic, reason)
        return False

    drone_id = payload["drone_id"]
    existing_state_raw = await r.hgetall(get_drone_state_key(drone_id))
    existing_state = normalize_state(existing_state_raw) if existing_state_raw else {}

    if message_type == "HOME":
        next_state = build_home_state(payload, existing_state or None)
        await persist_state(r, drone_id, next_state, state_ttl_sec)
        log.info("Stored HOME for drone_id=%s status=%s", drone_id, next_state["status"])
        return True

    if not existing_state or not state_has_home(existing_state):
        log.warning("Ignored COMMAND for drone_id=%s: HOME state is missing", drone_id)
        return False

    next_state = apply_command_update(existing_state, payload)
    await persist_state(r, drone_id, next_state, state_ttl_sec)
    log.info(
        "Applied COMMAND for drone_id=%s status=%s vx=%s vy=%s vz=%s",
        drone_id,
        next_state["status"],
        next_state["vx"],
        next_state["vy"],
        next_state["vz"],
    )
    return True


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    verified_commands_topic = os.getenv(
        "VERIFIED_COMMAND_TOPIC",
        VERIFIED_COMMAND_TOPIC_DEFAULT,
    )
    verified_home_topic = os.getenv(
        "VERIFIED_HOME_TOPIC",
        VERIFIED_HOME_TOPIC_DEFAULT,
    )
    input_topics = [
        verified_commands_topic,
        verified_home_topic,
    ]
    state_ttl_sec = int(os.getenv("STATE_TTL_SEC", "7200"))
    r = redis.from_url(redis_url, decode_responses=True)
    broker = create_broker_client_from_env()
    await start_broker_with_retry(broker, log, "Controller broker")

    log.info(
        "Controller started. input_topics=%s redis=%s",
        input_topics,
        redis_url,
    )

    try:
        async for msg in iter_broker_messages_with_retry(
            broker,
            input_topics,
            log,
            "Controller broker",
            group_id="SITL-controller-v1",
        ):
            payload = parse_json_payload(msg.payload)
            if payload is None:
                log.warning("Rejected message from topic=%s: invalid JSON payload", msg.topic)
                continue

            await process_verified_message(
                r,
                msg.topic,
                payload,
                verified_commands_topic,
                verified_home_topic,
                state_ttl_sec,
            )
    finally:
        await broker.stop()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
