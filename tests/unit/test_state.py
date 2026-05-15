import pathlib
import sys
import pytest
from shared import state, contracts  # noqa: E402

def test_state_has_home_false():
    assert state.state_has_home({"home_lat": 1}) is False
    assert state.state_has_home({}) is False

def test_serialize_state_preserves_data():
    original = {"a": 1, "b": "test"}
    assert state.serialize_state(original) == original

def test_is_moving_command_false():
    # ниже порогов
    assert state.is_moving_command(0.001, 0.0, 0.0) is False
    assert state.is_moving_command(0.0, 0.0, 0.001) is False

def test_build_position_response_missing_fields():
    assert state.build_position_response({"lat": 1}) is None

def test_build_position_response_schema_fail(monkeypatch):
    monkeypatch.setattr("shared.state.validate_schema", lambda p, s: (False, "fail"))
    state_data = {"lat": 1.0, "lon": 2.0, "alt": 3.0}
    assert state.build_position_response(state_data) is None


def test_distance_from_home_and_geofence():
    state_data = {
        "lat": 59.9386,
        "lon": 30.3141,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
    }
    assert state.distance_from_home_meters(state_data) == pytest.approx(0.0)
    assert state.is_within_home_geofence(state_data, 1.0) is True


def test_advance_drone_state_stops_on_geofence_violation():
    moving_state = {
        "status": "MOVING",
        "lat": 59.9386,
        "lon": 30.3141,
        "alt": 100.0,
        "home_lat": 59.9386,
        "home_lon": 30.3141,
        "home_alt": 100.0,
        "vx": 50.0,
        "vy": 0.0,
        "vz": 0.0,
    }
    next_state = state.advance_drone_state(
        moving_state,
        1.0,
        geofence_radius_m=1.0,
    )
    assert next_state["status"] == "ARMED"
    assert next_state["vx"] == 0.0
    assert next_state["policy_violation"] == state.GEOFENCE_VIOLATION_REASON
