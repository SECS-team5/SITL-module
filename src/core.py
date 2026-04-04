import asyncio
import os

import redis.asyncio as redis

from infopanel_client import create_infopanel_client_from_env
from state import advance_drone_state
from state import normalize_state
from state import serialize_state

# Глобальный клиент инфопанели
infopanel = create_infopanel_client_from_env()


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
    infopanel.log_event(f"Position updater started at {update_hz:.1f} Hz", "info")

    while True:
        try:
            async for state_key in r.scan_iter(match="drone:*:state"):
                await update_drone_position(r, state_key, update_interval_sec, state_ttl_sec)
            await asyncio.sleep(update_interval_sec)
        except Exception as exc:
            infopanel.log_event(f"Position updater failed: {exc}", "error")
            await asyncio.sleep(update_interval_sec)


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    update_hz = float(os.getenv("UPDATE_FREQUENCY_HZ", "10.0"))
    state_ttl_sec = int(os.getenv("STATE_TTL_SEC", "7200"))
    r = redis.from_url(redis_url, decode_responses=True)

    await infopanel.start()
    infopanel.log_event(
        f"Core started. redis={redis_url} update_hz={update_hz:.1f}",
        "info"
    )

    try:
        await position_updater_task(r, update_hz, state_ttl_sec)
    finally:
        await r.aclose()
        await infopanel.stop()


if __name__ == "__main__":
    asyncio.run(main())