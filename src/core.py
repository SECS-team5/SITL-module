import asyncio
import logging
import os
from typing import Any

import redis.asyncio as redis

from contracts import POSITION_REQUEST_SCHEMA_NAME
from contracts import POSITION_REQUEST_TOPIC_DEFAULT
from contracts import POSITION_RESPONSE_TOPIC_DEFAULT
from contracts import validate_schema
from messaging import KafkaRequestResponder
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


async def handle_position_request(
    r: redis.Redis,
    payload: dict[str, Any],
) -> dict[str, float] | None:
    response, reason = await resolve_position_request(r, payload)
    if response is None:
        log.warning("Rejected position request: %s", reason)
        return None
    return response


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    kafka_servers = os.getenv("KAFKA_SERVERS", "kafka:9092")
    update_hz = float(os.getenv("UPDATE_FREQUENCY_HZ", "10.0"))
    state_ttl_sec = int(os.getenv("STATE_TTL_SEC", "7200"))
    request_topic = os.getenv("POSITION_REQUEST_TOPIC", POSITION_REQUEST_TOPIC_DEFAULT)
    response_topic = os.getenv("POSITION_RESPONSE_TOPIC", POSITION_RESPONSE_TOPIC_DEFAULT)
    r = redis.from_url(redis_url, decode_responses=True)
    responder = KafkaRequestResponder(kafka_servers)

    log.info(
        "Core started. redis=%s kafka=%s position_request=%s",
        redis_url,
        kafka_servers,
        request_topic,
    )

    async def position_request_handler(payload: dict[str, Any]) -> dict[str, float] | None:
        return await handle_position_request(r, payload)

    try:
        await asyncio.gather(
            position_updater_task(r, update_hz, state_ttl_sec),
            responder.serve(request_topic, position_request_handler, response_topic),
        )
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
