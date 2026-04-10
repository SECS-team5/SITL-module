"""
Интеграционный тест: HOME-позиция в Redis.
"""
import asyncio
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]  # new-SITL/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis.asyncio as redis

from shared import state
from shared.contracts import HOME_SCHEMA_NAME, validate_schema

DRONE_ID = "drone_001"


async def test_home_position_stored_in_redis() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    home_payload = {
        "drone_id": DRONE_ID,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 120.0,
    }

    try:
        ok, reason = validate_schema(home_payload, HOME_SCHEMA_NAME)
        assert ok, f"HOME не валиден: {reason}"

        home_state = state.build_home_state(home_payload)
        await redis_client.hset(drone_key, mapping=home_state)
        await redis_client.expire(drone_key, 7200)

        stored = await redis_client.hgetall(drone_key)
        stored_state = state.normalize_state(stored)

        assert stored_state["status"] == "ARMED"
        assert float(stored_state["home_lat"]) == 59.9386
        assert float(stored_state["home_lon"]) == 30.3141
        assert float(stored_state["home_alt"]) == 120.0

        print(f"HOME сохранён: status={stored_state['status']}, lat={stored_state['home_lat']}")
        print("Тест пройден!")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    print("Тест: HOME-позиция")
    print("-" * 40)
    try:
        asyncio.run(test_home_position_stored_in_redis())
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
