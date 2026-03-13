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


def test_advance_drone_state_moves_lat_lon_and_alt() -> None:
    initial_state = state.apply_command_update(
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
            "vz": 1.0,
            "mag_heading": 45.0,
        },
    )

    updated_state = state.advance_drone_state(initial_state, 2.0)

    assert updated_state["lat"] > initial_state["lat"]
    assert updated_state["lon"] > initial_state["lon"]
    assert updated_state["alt"] == 122.0


def test_build_position_response_matches_architecture_contract() -> None:
    response = state.build_position_response(
        {
            "lat": 59.9386,
            "lon": 30.3141,
            "alt": 120.0,
        }
    )

    assert response == {
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 120.0,
    }


def test_resolve_position_request_accepts_transport_metadata_and_returns_position() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        drone_key = state.get_drone_state_key("drone_001")
        await redis_client.hset(
            drone_key,
            mapping=state.serialize_state(
                state.build_home_state(
                    {
                        "drone_id": "drone_001",
                        "home_lat": 59.9386,
                        "home_lon": 30.3141,
                        "home_alt": 120.0,
                    }
                )
            ),
        )

        response, reason = await core.resolve_position_request(
            redis_client,
            {
                "drone_id": "drone_001",
                "correlation_id": "req-123",
                "reply_to": "custom-topic",
            },
        )

        assert reason == ""
        assert response == {
            "lat": 59.9386,
            "lon": 30.3141,
            "alt": 120.0,
        }

    asyncio.run(scenario())


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
