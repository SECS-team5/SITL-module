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

import controller  # type: ignore  # noqa: E402
import state  # type: ignore  # noqa: E402
from fakes import FakeRedis  # type: ignore  # noqa: E402


VERIFIED_COMMAND_TOPIC = "sitl.verified-commands"
VERIFIED_HOME_TOPIC = "sitl.verified-home"
STATE_TTL_SEC = 7200


def test_home_message_creates_armed_state_hash() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        payload = {
            "drone_id": "drone_001",
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 120.0,
        }

        ok = await controller.process_verified_message(
            redis_client,
            VERIFIED_HOME_TOPIC,
            payload,
            VERIFIED_COMMAND_TOPIC,
            VERIFIED_HOME_TOPIC,
            STATE_TTL_SEC,
        )

        stored_state = state.normalize_state(
            await redis_client.hgetall(state.get_drone_state_key("drone_001"))
        )
        assert ok is True
        assert stored_state["status"] == "ARMED"
        assert stored_state["lat"] == 59.9386
        assert stored_state["lon"] == 30.3141
        assert stored_state["alt"] == 120.0
        assert stored_state["speed_h_ms"] == 0.0
        assert redis_client.expirations[state.get_drone_state_key("drone_001")] == STATE_TTL_SEC

    asyncio.run(scenario())


def test_command_is_ignored_until_home_is_known() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        payload = {
            "drone_id": "drone_001",
            "vx": 1.0,
            "vy": 0.0,
            "vz": 0.0,
            "mag_heading": 15.0,
        }

        ok = await controller.process_verified_message(
            redis_client,
            VERIFIED_COMMAND_TOPIC,
            payload,
            VERIFIED_COMMAND_TOPIC,
            VERIFIED_HOME_TOPIC,
            STATE_TTL_SEC,
        )

        assert ok is False
        assert await redis_client.hgetall(state.get_drone_state_key("drone_001")) == {}

    asyncio.run(scenario())


def test_command_updates_existing_hash_and_marks_drone_moving() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        home_payload = {
            "drone_id": "drone_001",
            "home_lat": 59.9386,
            "home_lon": 30.3141,
            "home_alt": 120.0,
        }
        command_payload = {
            "drone_id": "drone_001",
            "vx": 3.0,
            "vy": 4.0,
            "vz": 1.5,
            "mag_heading": 90.0,
        }

        await controller.process_verified_message(
            redis_client,
            VERIFIED_HOME_TOPIC,
            home_payload,
            VERIFIED_COMMAND_TOPIC,
            VERIFIED_HOME_TOPIC,
            STATE_TTL_SEC,
        )
        ok = await controller.process_verified_message(
            redis_client,
            VERIFIED_COMMAND_TOPIC,
            command_payload,
            VERIFIED_COMMAND_TOPIC,
            VERIFIED_HOME_TOPIC,
            STATE_TTL_SEC,
        )

        stored_state = state.normalize_state(
            await redis_client.hgetall(state.get_drone_state_key("drone_001"))
        )
        assert ok is True
        assert stored_state["status"] == "MOVING"
        assert stored_state["vx"] == 3.0
        assert stored_state["vy"] == 4.0
        assert stored_state["vz"] == 1.5
        assert stored_state["speed_h_ms"] == 5.0
        assert stored_state["speed_v_ms"] == 1.5

    asyncio.run(scenario())


def test_invalid_home_message_is_rejected() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        payload = {
            "drone_id": "drone_001",
            "home_lat": 59.9386,
            "home_lon": 30.3141,
        }

        ok = await controller.process_verified_message(
            redis_client,
            VERIFIED_HOME_TOPIC,
            payload,
            VERIFIED_COMMAND_TOPIC,
            VERIFIED_HOME_TOPIC,
            STATE_TTL_SEC,
        )

        assert ok is False

    asyncio.run(scenario())
