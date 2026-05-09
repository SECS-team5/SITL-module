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