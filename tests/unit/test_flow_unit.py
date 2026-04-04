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
import messaging  # type: ignore  # noqa: E402
import verifier  # type: ignore  # noqa: E402
from fakes import FakeRedis  # type: ignore  # noqa: E402


COMMAND_TOPIC = "sitl.commands"
HOME_TOPIC = "sitl-drone-home"
VERIFIED_COMMAND_TOPIC = "sitl.verified-commands"
VERIFIED_HOME_TOPIC = "sitl.verified-home"


def test_end_to_end_flow_from_verifier_to_position_response() -> None:
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
            "vx": 1.0,
            "vy": 2.0,
            "vz": 0.0,
            "mag_heading": 45.0,
        }

        ok_home, home_type, verified_home_payload, home_reason = verifier.process_input_message(
            HOME_TOPIC,
            home_payload,
            COMMAND_TOPIC,
            HOME_TOPIC,
        )
        ok_command, command_type, verified_command_payload, command_reason = verifier.process_input_message(
            COMMAND_TOPIC,
            command_payload,
            COMMAND_TOPIC,
            HOME_TOPIC,
        )

        assert ok_home is True
        assert home_type == "HOME"
        assert verified_home_payload == home_payload
        assert home_reason == ""
        assert ok_command is True
        assert command_type == "COMMAND"
        assert verified_command_payload == command_payload
        assert command_reason == ""

        await controller.process_verified_message(
            redis_client,
            VERIFIED_HOME_TOPIC,
            verified_home_payload,
            VERIFIED_COMMAND_TOPIC,
            VERIFIED_HOME_TOPIC,
            7200,
        )
        await controller.process_verified_message(
            redis_client,
            VERIFIED_COMMAND_TOPIC,
            verified_command_payload,
            VERIFIED_COMMAND_TOPIC,
            VERIFIED_HOME_TOPIC,
            7200,
        )

        response, reason = await messaging.resolve_position_request(
            redis_client,
            {"drone_id": "drone_001"},
        )

        assert reason == ""
        assert response == {
            "lat": 59.9386,
            "lon": 30.3141,
            "alt": 120.0,
        }

    asyncio.run(scenario())
