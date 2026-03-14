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
from fakes import FakeRedis  # type: ignore  # noqa: E402


def build_home_payload() -> dict:
    return {
        "drone_id": "drone_001",
        "msg_id": "msg-home",
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
                "speed_knots": 0.0,
                "course_degrees": 90.0,
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
            },
        },
        "derived": {
            "lat_decimal": 55.7558,
            "lon_decimal": 37.6173,
            "altitude_msl": 200.0,
            "gps_valid": True,
            "satellites_used": 10,
            "heading_at_home": 90.0,
            "position_accuracy_hdop": 1.1,
            "coord_system": "WGS84",
        },
    }


def test_process_home_persists_expected_home_payload() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        payload = build_home_payload()

        await core.process_home(redis_client, payload)

        stored = redis_client.decode_json("SITL:drone_001:home")
        assert stored["drone_id"] == "drone_001"
        assert stored["derived"]["gps_valid"] is True
        assert stored["derived"]["heading_at_home"] == 90.0
        assert stored["derived"]["position_accuracy_hdop"] == 1.1
        assert stored["derived"]["coord_system"] == "WGS84"
        assert redis_client.expirations["SITL:drone_001:home"] == core.KEY_TTL

    asyncio.run(scenario())


def test_process_home_keeps_required_nested_fields() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        payload = build_home_payload()
        payload["drone_id"] = "drone_002"
        payload["msg_id"] = "msg-home-2"
        payload["nmea"]["rmc"]["course_degrees"] = 180.0
        payload["nmea"]["gga"]["quality"] = 1
        payload["derived"]["gps_valid"] = False
        payload["derived"]["satellites_used"] = 8

        await core.process_home(redis_client, payload)

        stored = redis_client.decode_json("SITL:drone_002:home")
        assert stored["nmea"]["rmc"]["course_degrees"] == 180.0
        assert stored["nmea"]["gga"]["quality"] == 1
        assert stored["derived"]["satellites_used"] == 8
        assert stored["derived"]["gps_valid"] is False

    asyncio.run(scenario())
