import asyncio
import json
import logging
import os
from typing import Any

import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer

from contracts import build_request_headers
from contracts import decode_headers
from contracts import get_transport_value
from contracts import parse_json_payload
from contracts import POSITION_REQUEST_SCHEMA_NAME
from contracts import POSITION_REQUEST_TOPIC_DEFAULT
from contracts import POSITION_RESPONSE_TOPIC_DEFAULT
from contracts import validate_schema
from state import advance_drone_state
from state import build_position_response
from state import normalize_state
from state import serialize_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


async def refresh_state_ttl(
    r: redis.Redis,
    state_key: str,
    state_ttl_sec: int,
) -> None:
    if state_ttl_sec > 0:
        await r.expire(state_key, state_ttl_sec)


async def update_drone_position(
    r: redis.Redis,
    state_key: str,
    update_interval_sec: float,
    state_ttl_sec: int,
) -> bool:
    raw_state = await r.hgetall(state_key)
    if not raw_state:
        return False

    state = normalize_state(raw_state)
    if state.get("status") != "MOVING":
        return False

    next_state = advance_drone_state(state, update_interval_sec)
    await r.hset(state_key, mapping=serialize_state(next_state))
    await refresh_state_ttl(r, state_key, state_ttl_sec)
    return True


async def position_updater_task(
    r: redis.Redis,
    update_hz: float,
    state_ttl_sec: int,
) -> None:
    update_interval_sec = 1.0 / update_hz
    log.info("Position updater started at %.1f Hz", update_hz)

    while True:
        try:
            async for state_key in r.scan_iter(match="drone:*:state"):
                await update_drone_position(r, state_key, update_interval_sec, state_ttl_sec)
            await asyncio.sleep(update_interval_sec)
        except Exception as exc:
            log.error("Position updater failed: %s", exc, exc_info=True)
            await asyncio.sleep(update_interval_sec)


async def resolve_position_request(
    r: redis.Redis,
    payload: dict[str, Any],
) -> tuple[dict[str, float] | None, str]:
    ok, reason = validate_schema(
        payload,
        POSITION_REQUEST_SCHEMA_NAME,
        allow_transport_fields=True,
    )
    if not ok:
        return None, reason

    drone_id = payload["drone_id"]
    raw_state = await r.hgetall(f"drone:{drone_id}:state")
    if not raw_state:
        return None, f"drone '{drone_id}' state not found"

    response = build_position_response(normalize_state(raw_state))
    if response is None:
        return None, f"drone '{drone_id}' state does not contain a valid position"

    return response, ""


async def position_request_processor_task(
    r: redis.Redis,
    kafka_servers: str,
    request_topic: str,
    default_response_topic: str,
) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    consumer = AIOKafkaConsumer(
        request_topic,
        bootstrap_servers=kafka_servers,
        group_id="SITL-position-service-v1",
    )

    await producer.start()
    await consumer.start()
    log.info(
        "Position responder started. request_topic=%s response_topic=%s",
        request_topic,
        default_response_topic,
    )

    try:
        async for msg in consumer:
            payload = parse_json_payload(msg.value)
            if payload is None:
                log.warning("Rejected position request from topic=%s: invalid JSON payload", msg.topic)
                continue

            response, reason = await resolve_position_request(r, payload)
            if response is None:
                log.warning("Rejected position request: %s", reason)
                continue

            headers = decode_headers(msg.headers)
            reply_to = get_transport_value(payload, headers, "reply_to") or default_response_topic
            correlation_id = get_transport_value(payload, headers, "correlation_id")
            response_headers = build_request_headers(correlation_id, reply_to) if correlation_id else []

            await producer.send_and_wait(
                reply_to,
                response,
                headers=response_headers or None,
            )
            log.info("Returned position for drone_id=%s reply_to=%s", payload["drone_id"], reply_to)
    finally:
        await producer.stop()
        await consumer.stop()


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    kafka_servers = os.getenv("KAFKA_SERVERS", "kafka:9092")
    update_hz = float(os.getenv("UPDATE_FREQUENCY_HZ", "10.0"))
    state_ttl_sec = int(os.getenv("STATE_TTL_SEC", "7200"))
    request_topic = os.getenv("POSITION_REQUEST_TOPIC", POSITION_REQUEST_TOPIC_DEFAULT)
    response_topic = os.getenv("POSITION_RESPONSE_TOPIC", POSITION_RESPONSE_TOPIC_DEFAULT)
    r = redis.from_url(redis_url, decode_responses=True)

    log.info(
        "Core started. redis=%s kafka=%s position_request=%s",
        redis_url,
        kafka_servers,
        request_topic,
    )

    try:
        await asyncio.gather(
            position_updater_task(r, update_hz, state_ttl_sec),
            position_request_processor_task(r, kafka_servers, request_topic, response_topic),
        )
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
