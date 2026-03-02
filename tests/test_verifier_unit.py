import json
import math
import pathlib
import sys
from typing import Any

import pytest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import verifier  # type: ignore  # noqa: E402


def test_parse_json_payload_accepts_dict() -> None:
    payload = {"drone_id": "d1"}
    assert verifier.parse_json_payload(payload) is payload


def test_parse_json_payload_accepts_valid_bytes() -> None:
    raw = json.dumps({"drone_id": "d1"}).encode()
    parsed = verifier.parse_json_payload(raw)
    assert isinstance(parsed, dict)
    assert parsed["drone_id"] == "d1"


def test_parse_json_payload_rejects_invalid_bytes() -> None:
    raw = b"{not-a-json"
    assert verifier.parse_json_payload(raw) is None


def test_to_float_handles_valid_and_invalid_values() -> None:
    assert verifier.to_float("42.5") == 42.5
    assert verifier.to_float("  10 ") == 10.0
    assert verifier.to_float(None) is None
    assert verifier.to_float("nan-value") is None


def test_has_non_empty_behaviour() -> None:
    payload = {"a": "1", "b": "  ", "c": None}
    assert verifier.has_non_empty(payload, "a") is True
    assert verifier.has_non_empty(payload, "b") is False
    assert verifier.has_non_empty(payload, "c") is False
    assert verifier.has_non_empty(payload, "d") is False


def test_calculate_nmea_checksum_known_example() -> None:
    # Body string without leading '$' and without '*checksum'
    body = "GNRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W"
    checksum = verifier.calculate_nmea_checksum(body)
    # Ожидаем конкретное значение checksum, посчитанное по NMEA‑правилу XOR
    # Предварительно проверено отдельным скриптом: должно быть 0x74 (116 десятичное).
    assert checksum == int("74", 16)


def test_is_valid_nmea_gn_accepts_correct_sentence() -> None:
    # Валидное предложение NMEA с корректным checksum (0x74)
    sentence = (
        "$GNRMC,123519,A,4807.038,N,01131.000,E,"
        "022.4,084.4,230394,003.1,W*74"
    )
    assert verifier.is_valid_nmea_gn(sentence) is True


def test_is_valid_nmea_gn_rejects_incorrect_prefix_and_checksum() -> None:
    sentence_bad_prefix = "$GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A"
    sentence_bad_checksum = "$GNRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*00"
    assert verifier.is_valid_nmea_gn(sentence_bad_prefix) is False
    assert verifier.is_valid_nmea_gn(sentence_bad_checksum) is False


def test_nmea_to_decimal_northern_and_southern() -> None:
    lat_n = verifier.nmea_to_decimal("4807.038", "N")
    lon_e = verifier.nmea_to_decimal("01131.000", "E")
    lat_s = verifier.nmea_to_decimal("4807.038", "S")
    assert lat_n is not None and lon_e is not None and lat_s is not None
    assert lat_n == pytest.approx(48.1173, rel=1e-4)
    assert lon_e == pytest.approx(11.5167, rel=1e-4)
    assert lat_s == pytest.approx(-48.1173, rel=1e-4)


def test_extract_position_from_nmea_rmc() -> None:
    sentence = (
        "$GNRMC,123519,A,4807.038,N,01131.000,E,"
        "022.4,084.4,230394,003.1,W*74"
    )
    pos = verifier.extract_position_from_nmea(sentence)
    assert pos is not None
    assert pos["lat"] == pytest.approx(48.1173, rel=1e-4)
    assert pos["lon"] == pytest.approx(11.5167, rel=1e-4)


def test_extract_course_from_nmea_and_speed_from_nmea() -> None:
    # course over ground = 084.4 degrees, speed over ground = 22.4 knots
    sentence = (
        "$GNRMC,123519,A,4807.038,N,01131.000,E,"
        "022.4,084.4,230394,003.1,W*74"
    )
    course = verifier.extract_course_from_nmea(sentence)
    speed_mps = verifier.extract_speed_from_nmea(sentence)
    assert course == pytest.approx(84.4)
    expected_speed = 22.4 * 0.514444
    assert speed_mps == pytest.approx(expected_speed, rel=1e-4)


def test_course_to_vector_returns_unit_direction() -> None:
    vec = verifier.course_to_vector(90.0)
    assert vec["x"] == pytest.approx(1.0, rel=1e-6)
    assert vec["y"] == pytest.approx(0.0, abs=1e-6)
    length = math.hypot(vec["x"], vec["y"])
    assert length == pytest.approx(1.0, rel=1e-6)


def test_extract_direction_from_embedded_dict() -> None:
    payload = {"direction": {"course_deg": 45}}
    direction = verifier.extract_direction(payload)
    assert direction is not None
    assert direction["course_deg"] == 45
    vec = direction["vector_unit"]
    assert math.isfinite(vec["x"]) and math.isfinite(vec["y"])


def test_extract_direction_from_course_fields_and_nmea() -> None:
    base: dict[str, Any] = {"course_deg": 180}
    from_course = verifier.extract_direction(base)
    assert from_course is not None and from_course["course_deg"] == 180

    base2: dict[str, Any] = {"heading_deg": 270}
    from_heading = verifier.extract_direction(base2)
    assert from_heading is not None and from_heading["course_deg"] == 270

    base3: dict[str, Any] = {
        "nmea": (
            "$GNRMC,123519,A,4807.038,N,01131.000,E,"
            "022.4,084.4,230394,003.1,W*74"
        )
    }
    from_nmea = verifier.extract_direction(base3)
    assert from_nmea is not None and from_nmea["course_deg"] == pytest.approx(84.4)


def test_extract_speed_from_various_sources() -> None:
    assert verifier.extract_speed({"speed_mps": 5}) == 5
    assert verifier.extract_speed({"speed_knots": 10}) == pytest.approx(10 * 0.514444, rel=1e-4)
    assert (
        verifier.extract_speed({"speed": {"mps": 7.5}})
        == pytest.approx(7.5, rel=1e-4)
    )
    # В коде скорость из knots округляется до 3 знаков после запятой
    assert verifier.extract_speed({"speed": {"knots": 3}}) == pytest.approx(
        round(3 * 0.514444, 3), rel=1e-4
    )
    assert verifier.extract_speed({"speed": "2.5"}) == pytest.approx(2.5, rel=1e-4)

    # Fallback to NMEA
    payload = {
        "nmea": (
            "$GNRMC,123519,A,4807.038,N,01131.000,E,"
            "022.4,084.4,230394,003.1,W*74"
        )
    }
    speed_from_nmea = verifier.extract_speed(payload)
    assert speed_from_nmea == pytest.approx(22.4 * 0.514444, rel=1e-4)


def test_extract_speed_rejects_negative_and_invalid() -> None:
    assert verifier.extract_speed({"speed_mps": -1}) is None
    assert verifier.extract_speed({"speed_knots": -5}) is None
    assert verifier.extract_speed({"speed": {"mps": -3}}) is None
    assert verifier.extract_speed({"speed": "not-a-number"}) is None


def test_enrich_payload_with_command_success_and_failures() -> None:
    # Successful enrichment with explicit start_position
    base = {
        "drone_id": "d1",
        "message_id": "m1",
        "nmea": (
            "$GNRMC,123519,A,4807.038,N,01131.000,E,"
            "022.4,084.4,230394,003.1,W*74"
        ),
        "start_position": {"lat": 48.0, "lon": 11.0},
        "direction": {"course_deg": 90},
        "speed_mps": 3.5,
    }
    ok, enriched, reason = verifier.enrich_payload_with_command(base)
    assert ok is True
    assert reason == ""
    assert enriched["start_position"] == {"lat": 48.0, "lon": 11.0}
    assert enriched["speed_mps"] == 3.5
    assert "direction" in enriched

    # Failure when position cannot be extracted
    no_position = {
        "drone_id": "d1",
        "message_id": "m2",
        "nmea": "invalid-nmea",
        "direction": {"course_deg": 90},
        "speed_mps": 3.5,
    }
    ok2, _, reason2 = verifier.enrich_payload_with_command(no_position)
    assert ok2 is False
    assert "cannot extract start position" in reason2


def test_validate_required_fields_happy_and_error_paths() -> None:
    valid = {
        "drone_id": "d1",
        "message_id": "m1",
        "nmea": (
            "$GNRMC,123519,A,4807.038,N,01131.000,E,"
            "022.4,084.4,230394,003.1,W*74"
        ),
    }
    ok, reason = verifier.validate_required_fields(valid)
    assert ok is True
    assert reason == ""

    for missing in ("drone_id", "message_id", "nmea"):
        invalid = dict(valid)
        invalid[missing] = ""
        ok2, reason2 = verifier.validate_required_fields(invalid)
        assert ok2 is False
        assert missing in reason2

    invalid_nmea = dict(valid)
    invalid_nmea["nmea"] = "bad-nmea"
    ok3, reason3 = verifier.validate_required_fields(invalid_nmea)
    assert ok3 is False
    assert "invalid NMEA" in reason3


def test_classify_message_home_and_drone_info() -> None:
    payload = {}
    ok, msg_type, reason = verifier.classify_message(
        topic="sitl-drone-home", home_topic="sitl-drone-home", payload=payload
    )
    assert ok is True
    assert msg_type == "HOME"
    assert reason == ""

    payload_home_wrong_topic = {"type": "HOME"}
    ok2, msg_type2, reason2 = verifier.classify_message(
        topic="sitl-drone-info", home_topic="sitl-drone-home", payload=payload_home_wrong_topic
    )
    assert ok2 is False
    assert msg_type2 == ""
    assert "HOME message in non-HOME topic" in reason2

    payload_info = {}
    ok3, msg_type3, reason3 = verifier.classify_message(
        topic="sitl-drone-info", home_topic="sitl-drone-home", payload=payload_info
    )
    assert ok3 is True
    assert msg_type3 == "DRONE_INFO"
    assert reason3 == ""


def test_source_is_trusted_behaviour() -> None:
    # Empty whitelist => always trusted
    assert verifier.source_is_trusted({"source": "any"}, []) is True

    trusted = ["GroundStation", "OtherSource"]
    assert (
        verifier.source_is_trusted({"source": "groundstation"}, trusted) is True
    )
    assert verifier.source_is_trusted({"source": "unknown"}, trusted) is False


def test_parse_csv_env_splits_and_strips(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INPUT_TOPICS", "topic1, topic2 ,topic3")
    result = verifier.parse_csv_env("INPUT_TOPICS")
    assert result == ["topic1", "topic2", "topic3"]


def test_full_pipeline_valid_home_message_trusted_source() -> None:
    # Собираем полезную нагрузку HOME только на основе NMEA:
    # координаты, курс и скорость берутся из RMC.
    nmea_sentence = (
        "$GNRMC,123519,A,4807.038,N,01131.000,E,"
        "022.4,084.4,230394,003.1,W*74"
    )

    payload = {
        "drone_id": "drone-1",
        "message_id": "msg-1",
        "nmea": nmea_sentence,
        "source": "GroundStation",
    }

    topic = "sitl-drone-home"
    home_topic = "sitl-drone-home"
    trusted = ["GroundStation"]

    ok_type, msg_type, reason_type = verifier.classify_message(topic, home_topic, payload)
    assert ok_type is True
    assert msg_type == "HOME"
    assert reason_type == ""

    ok_fields, reason_fields = verifier.validate_required_fields(payload)
    assert ok_fields is True
    assert reason_fields == ""

    assert verifier.source_is_trusted(payload, trusted) is True

    ok_enrich, normalized, reason_enrich = verifier.enrich_payload_with_command(payload)
    assert ok_enrich is True
    assert reason_enrich == ""

    # После обогащения должны быть явные поля
    assert "start_position" in normalized
    assert "direction" in normalized
    assert "speed_mps" in normalized

    pos = normalized["start_position"]
    assert pos["lat"] == pytest.approx(48.1173, rel=1e-4)
    assert pos["lon"] == pytest.approx(11.5167, rel=1e-4)


def test_full_pipeline_home_message_untrusted_source_rejected() -> None:
    nmea_sentence = (
        "$GNRMC,123519,A,4807.038,N,01131.000,E,"
        "022.4,084.4,230394,003.1,W*74"
    )

    payload = {
        "drone_id": "drone-1",
        "message_id": "msg-1",
        "nmea": nmea_sentence,
        "source": "UnknownSource",
    }

    topic = "sitl-drone-home"
    home_topic = "sitl-drone-home"
    trusted = ["GroundStation"]

    ok_type, msg_type, reason_type = verifier.classify_message(topic, home_topic, payload)
    assert ok_type is True
    assert msg_type == "HOME"
    assert reason_type == ""

    ok_fields, reason_fields = verifier.validate_required_fields(payload)
    assert ok_fields is True
    assert reason_fields == ""

    # На этапе доверенных источников такое сообщение должно отсекаться
    assert verifier.source_is_trusted(payload, trusted) is False


