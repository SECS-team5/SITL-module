import asyncio
import json
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
from fakes import FakeRedis  # type: ignore  # noqa: E402


def build_command_payload() -> dict:
    return {
        "drone_id": "drone_001",
        "msg_id": "msg-1",
        "timestamp": "2026-03-14T10:00:00Z",
        "nmea": {
            "rmc": {
                "talker_id": "GN",
                "time": "123456.789",
                "status": "A",
                "latitude": "5545.1234",
                "lat_dir": "N",
                "longitude": "03737.5678",
                "lon_dir": "E",
                "speed_knots": 15.5,
                "course_degrees": 270.0,
                "date": "150625",
            },
            "gga": {
                "talker_id": "GN",
                "time": "123456.789",
                "latitude": "5545.1234",
                "lat_dir": "N",
                "longitude": "03737.5678",
                "lon_dir": "E",
                "quality": 2,
                "satellites": 10,
                "hdop": 1.2,
            },
        },
        "derived": {
            "lat_decimal": 55.7558,
            "lon_decimal": 37.6173,
            "altitude_msl": 200.0,
            "speed_vertical_ms": 2.5,
        },
        "actions": {
            "drop": False,
            "emergency_landing": False,
        },
    }


def test_process_command_persists_expected_state_payload() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        payload = build_command_payload()

        await controller.process_command(redis_client, payload)

        stored = redis_client.decode_json("SITL:drone_001:state")
        assert stored["drone_id"] == "drone_001"
        assert stored["msg_id"] == "msg-1"
        assert stored["nmea"]["gga"]["hdop"] == 1.2
        assert stored["derived"]["speed_vertical_ms"] == 2.5
        assert stored["actions"]["drop"] is False
        assert redis_client.expirations["SITL:drone_001:state"] == controller.KEY_TTL

    asyncio.run(scenario())


def test_update_drone_position_rewrites_coordinates() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        initial_state = build_command_payload()
        initial_state["nmea"]["rmc"]["speed_knots"] = 10.0
        initial_state["nmea"]["rmc"]["course_degrees"] = 90.0
        await redis_client.set(
            "SITL:drone_001:state",
            json.dumps(initial_state),
            ex=controller.KEY_TTL,
        )

        await controller.update_drone_position(
            redis_client,
            "SITL:drone_001:state",
            1.0,
        )

        updated_state = redis_client.decode_json("SITL:drone_001:state")
        assert updated_state["derived"]["lat_decimal"] == initial_state["derived"]["lat_decimal"]
        assert updated_state["derived"]["lon_decimal"] > initial_state["derived"]["lon_decimal"]
        assert "last_position_update" in updated_state

    asyncio.run(scenario())


def test_calculate_new_position_moves_north_for_zero_degree_course() -> None:
    vx, vy = controller.course_to_vector(0.0)
    new_lat, new_lon = controller.calculate_new_position(
        59.9386,
        30.3141,
        vx,
        vy,
        5.0,
        2.0,
    )

    assert new_lat > 59.9386
    assert new_lon == 30.3141


def test_update_drone_position_ignores_invalid_json() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        await redis_client.set(
            "SITL:drone_001:state",
            "{not-json",
            ex=controller.KEY_TTL,
        )

        await controller.update_drone_position(
            redis_client,
            "SITL:drone_001:state",
            1.0,
        )

        assert redis_client.values["SITL:drone_001:state"] == "{not-json"

    asyncio.run(scenario())
