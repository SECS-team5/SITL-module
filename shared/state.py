import math
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Mapping

from shared.contracts import POSITION_RESPONSE_SCHEMA_NAME
from shared.contracts import validate_schema

NUMERIC_STATE_FIELDS = frozenset(
    {
        "alt",
        "home_alt",
        "home_lat",
        "home_lon",
        "lat",
        "lon",
        "mag_heading",
        "speed_h_ms",
        "speed_v_ms",
        "vx",
        "vy",
        "vz",
    }
)
HORIZONTAL_MOVING_THRESHOLD_SQUARED = 0.01
VERTICAL_MOVING_THRESHOLD = 0.01


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_drone_state_key(drone_id: str) -> str:
    return f"drone:{drone_id}:state"


def normalize_state(raw_state: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_state.items():
        key = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
        value: Any = raw_value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode()

        if key in NUMERIC_STATE_FIELDS:
            normalized[key] = float(value)
        else:
            normalized[key] = value

    return normalized


def serialize_state(state: Mapping[str, Any]) -> dict[str, Any]:
    return dict(state)


def state_has_home(state: Mapping[str, Any]) -> bool:
    required_fields = ("home_lat", "home_lon", "home_alt")
    return all(field in state for field in required_fields)


def compute_speed_metrics(vx: float, vy: float, vz: float) -> tuple[float, float]:
    speed_h_ms = round(math.sqrt(vx * vx + vy * vy), 3)
    speed_v_ms = round(abs(vz), 3)
    return speed_h_ms, speed_v_ms


def is_moving_command(vx: float, vy: float, vz: float) -> bool:
    return (vx * vx + vy * vy) > HORIZONTAL_MOVING_THRESHOLD_SQUARED or abs(vz) > VERTICAL_MOVING_THRESHOLD


def build_home_state(
    payload: Mapping[str, Any],
    existing_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    timestamp = utc_now_iso()
    home_lat = float(payload["home_lat"])
    home_lon = float(payload["home_lon"])
    home_alt = float(payload["home_alt"])
    previous_heading = 0.0
    if existing_state and "mag_heading" in existing_state:
        previous_heading = float(existing_state["mag_heading"])

    return {
        "status": "ARMED",
        "lat": home_lat,
        "lon": home_lon,
        "alt": home_alt,
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "speed_h_ms": 0.0,
        "speed_v_ms": 0.0,
        "mag_heading": previous_heading,
        "home_lat": home_lat,
        "home_lon": home_lon,
        "home_alt": home_alt,
        "last_update": timestamp,
    }


def apply_command_update(
    state: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    vx = float(payload["vx"])
    vy = float(payload["vy"])
    vz = float(payload["vz"])
    speed_h_ms, speed_v_ms = compute_speed_metrics(vx, vy, vz)
    next_state = dict(state)
    next_state.update(
        {
            "vx": vx,
            "vy": vy,
            "vz": vz,
            "mag_heading": float(payload["mag_heading"]),
            "speed_h_ms": speed_h_ms,
            "speed_v_ms": speed_v_ms,
            "status": "MOVING" if is_moving_command(vx, vy, vz) else "ARMED",
            "last_update": utc_now_iso(),
        }
    )
    return next_state


def advance_drone_state(
    state: Mapping[str, Any],
    delta_time_sec: float,
) -> dict[str, Any]:
    current_state = dict(state)
    if current_state.get("status") != "MOVING":
        return current_state

    lat = float(current_state["lat"])
    lon = float(current_state["lon"])
    alt = float(current_state["alt"])
    vx = float(current_state["vx"])
    vy = float(current_state["vy"])
    vz = float(current_state["vz"])
    meters_per_degree_lat = 111111.0
    meters_per_degree_lon = 111111.0 * math.cos(math.radians(lat))
    if abs(meters_per_degree_lon) < 1e-6:
        meters_per_degree_lon = 1e-6

    next_state = dict(current_state)
    next_state["lat"] = round(lat + (vy * delta_time_sec) / meters_per_degree_lat, 7)
    next_state["lon"] = round(lon + (vx * delta_time_sec) / meters_per_degree_lon, 7)
    next_state["alt"] = round(alt + (vz * delta_time_sec), 3)
    next_state["last_update"] = utc_now_iso()
    return next_state


def build_position_response(state: Mapping[str, Any]) -> dict[str, float] | None:
    required_fields = ("lat", "lon", "alt")
    if not all(field in state for field in required_fields):
        return None

    response = {
        "lat": float(state["lat"]),
        "lon": float(state["lon"]),
        "alt": float(state["alt"]),
    }
    ok, _ = validate_schema(response, POSITION_RESPONSE_SCHEMA_NAME)
    return response if ok else None
