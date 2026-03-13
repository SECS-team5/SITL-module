import asyncio
import logging
import os
from typing import Any

import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

from contracts import parse_json_payload
from contracts import validate_verified_message
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
    key = get_drone_state_key(drone_id)
    await r.hset(key, mapping=serialize_state(state))
    if state_ttl_sec > 0:
        await r.expire(key, state_ttl_sec)


async def process_verified_message(
    r: redis.Redis,
    verified_message: dict[str, Any],
    commands_topic: str,
    home_topic: str,
    state_ttl_sec: int,
) -> bool:
    ok, reason = validate_verified_message(
        verified_message,
        commands_topic,
        home_topic,
    )
    if not ok:
        log.warning("Rejected verified message: %s", reason)
        return False

    data = verified_message["data"]
    drone_id = data["drone_id"]
    state_key = get_drone_state_key(drone_id)
    existing_state_raw = await r.hgetall(state_key)
    existing_state = normalize_state(existing_state_raw) if existing_state_raw else {}

    if verified_message["message_type"] == "HOME":
        next_state = build_home_state(data, existing_state or None)
        await persist_state(r, drone_id, next_state, state_ttl_sec)
        log.info("Stored HOME for drone_id=%s status=%s", drone_id, next_state["status"])
        return True

    if not existing_state or not state_has_home(existing_state):
        log.warning("Ignored COMMAND for drone_id=%s: HOME state is missing", drone_id)
        return False

    next_state = apply_command_update(existing_state, data)
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
    kafka_servers = os.getenv("KAFKA_SERVERS", "kafka:9092")
    input_topic = os.getenv("INPUT_TOPIC", "sitl-verified-messages")
    commands_topic = os.getenv("COMMAND_TOPIC", "sitl/commands")
    home_topic = os.getenv("HOME_TOPIC", "sitl-drone-home")
    state_ttl_sec = int(os.getenv("STATE_TTL_SEC", "7200"))
    r = redis.from_url(redis_url, decode_responses=True)
    consumer = AIOKafkaConsumer(
        input_topic,
        bootstrap_servers=kafka_servers,
        group_id="SITL-controller-v1",
    )

    await consumer.start()
    log.info(
        "Controller started. input=%s redis=%s command_topic=%s home_topic=%s",
        input_topic,
        redis_url,
        commands_topic,
        home_topic,
    )

    try:
        async for msg in consumer:
            verified_message = parse_json_payload(msg.value)
            if verified_message is None:
                log.warning("Rejected message from topic=%s: invalid JSON payload", msg.topic)
                continue

            await process_verified_message(
                r,
                verified_message,
                commands_topic,
                home_topic,
                state_ttl_sec,
            )
    finally:
        await consumer.stop()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
