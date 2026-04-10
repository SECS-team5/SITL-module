"""
Интеграционный тест: запрос позиции дрона из Redis.
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
from shared.contracts import POSITION_REQUEST_SCHEMA_NAME, validate_schema

DRONE_ID = "drone_001"


async def test_position_request_returns_coordinates() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)

    test_state = {
        "status": "MOVING",
        "lat": "59.9386",
        "lon": "30.3141",
        "alt": "150.0",
        "home_lat": "59.9386",
        "home_lon": "30.3141",
        "home_alt": "120.0",
        "vx": "0.0",
        "vy": "0.0",
        "vz": "0.0",
        "speed_h_ms": "0.0",
        "speed_v_ms": "0.0",
        "mag_heading": "90.0",
    }

    try:
        await redis_client.hset(drone_key, mapping=test_state)

        request = {"drone_id": DRONE_ID}
        ok, reason = validate_schema(request, POSITION_REQUEST_SCHEMA_NAME)
        assert ok, f"Запрос не валиден: {reason}"

        stored = await redis_client.hgetall(drone_key)
        response = state.build_position_response(state.normalize_state(stored))
        
        assert response is not None
        assert response["lat"] == 59.9386
        assert response["lon"] == 30.3141
        assert response["alt"] == 150.0

        print(f"Координаты получены: lat={response['lat']}, lon={response['lon']}, alt={response['alt']}")
        print("Тест пройден!")

    finally:
        try:
            await redis_client.delete(drone_key)
            await redis_client.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    print("Тест: запрос позиции")
    print("-" * 40)
    try:
        asyncio.run(test_position_request_returns_coordinates())
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
