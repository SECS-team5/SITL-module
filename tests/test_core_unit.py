import asyncio
import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
TESTS = ROOT / "tests"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

import core  # type: ignore  # noqa: E402
import state  # type: ignore  # noqa: E402
from fakes import FakeRedis  # type: ignore  # noqa: E402


def test_update_drone_position_persists_new_coordinates_and_refreshes_ttl() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        drone_key = state.get_drone_state_key("drone_001")
        moving_state = state.apply_command_update(
            state.build_home_state(
                {
                    "drone_id": "drone_001",
                    "home_lat": 59.9386,
                    "home_lon": 30.3141,
                    "home_alt": 120.0,
                }
            ),
            {
                "drone_id": "drone_001",
                "vx": 2.0,
                "vy": 3.0,
                "vz": 0.5,
                "mag_heading": 30.0,
            },
        )
        await redis_client.hset(drone_key, mapping=state.serialize_state(moving_state))

        ok = await core.update_drone_position(redis_client, drone_key, 1.0, 7200)
        updated_state = state.normalize_state(await redis_client.hgetall(drone_key))

        assert ok is True
        assert updated_state["lat"] > moving_state["lat"]
        assert updated_state["lon"] > moving_state["lon"]
        assert updated_state["alt"] == 120.5
        assert redis_client.expirations[drone_key] == 7200

    asyncio.run(scenario())
