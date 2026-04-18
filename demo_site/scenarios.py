from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable


MAX_DRONES = 16
MIN_DRONES = 1
DRONE_ID_START = 901
METERS_PER_DEG_LAT = 111_111.0
FORMATION_RADIUS_M = 120.0
TRAIL_POINTS = 60

DRONE_COLORS = (
    "#f25f5c", "#ffca3a", "#8ac926", "#1982c4",
    "#6a4c93", "#ff9f1c", "#2ec4b6", "#f18f01",
    "#70c1b3", "#f7a072", "#c1ff72", "#5bc0eb",
    "#d7263d", "#4ecdc4", "#b2dbbf", "#ff595e",
)


@dataclass
class DroneLayout:
    drone_id: str
    label: str
    color: str
    index: int
    home_lat: float
    home_lon: float
    home_alt: float
    auto: bool = True          # True — дрон под управлением авто-сценария


def clamp_drone_count(value: int) -> int:
    return max(MIN_DRONES, min(MAX_DRONES, int(value)))


def _meters_to_geo(base_lat: float, base_lon: float, dx_m: float, dy_m: float) -> tuple[float, float]:
    lat = base_lat + dy_m / METERS_PER_DEG_LAT
    m_per_deg_lon = METERS_PER_DEG_LAT * math.cos(math.radians(base_lat))
    lon = base_lon + dx_m / m_per_deg_lon
    return round(lat, 7), round(lon, 7)


def build_layout(drone_count: int, base_lat: float, base_lon: float, base_alt: float) -> list[DroneLayout]:
    count = clamp_drone_count(drone_count)
    layouts: list[DroneLayout] = []
    for i in range(count):
        angle = (2.0 * math.pi * i) / count
        dx = FORMATION_RADIUS_M * math.cos(angle)
        dy = FORMATION_RADIUS_M * math.sin(angle)
        home_lat, home_lon = _meters_to_geo(base_lat, base_lon, dx, dy)
        layouts.append(
            DroneLayout(
                drone_id=f"drone_{DRONE_ID_START + i:03d}",
                label=f"D{i + 1:02d}",
                color=DRONE_COLORS[i % len(DRONE_COLORS)],
                index=i,
                home_lat=home_lat,
                home_lon=home_lon,
                home_alt=round(base_alt + (i % 3) * 3.0, 1),
            )
        )
    return layouts


def build_home_payload(layout: DroneLayout) -> dict:
    return {
        "drone_id": layout.drone_id,
        "home_lat": layout.home_lat,
        "home_lon": layout.home_lon,
        "home_alt": layout.home_alt,
    }


# ───────────────── сценарии: (layout, t, total) → (vx, vy, vz) ─────────────────


ScenarioFn = Callable[[DroneLayout, float, int], tuple[float, float, float]]


def _phase(layout: DroneLayout, total: int) -> float:
    return (2.0 * math.pi * layout.index) / max(1, total)


def _scenario_orbit(layout: DroneLayout, t: float, total: int) -> tuple[float, float, float]:
    """Хоровод — синхронная орбита роя с пульсацией радиуса."""
    phase = _phase(layout, total)
    orbit_omega = 0.25                     # период ~25с
    orbit_radius = FORMATION_RADIUS_M
    pulse_omega = 0.35
    pulse_amp = 30.0

    r = orbit_radius + pulse_amp * math.sin(pulse_omega * t)
    dr = pulse_amp * pulse_omega * math.cos(pulse_omega * t)
    angle = orbit_omega * t + phase
    cos_a, sin_a = math.cos(angle), math.sin(angle)

    vx = dr * cos_a - r * orbit_omega * sin_a
    vy = dr * sin_a + r * orbit_omega * cos_a
    vz = 1.8 * math.sin(0.35 * t + 2.0 * phase)
    return vx, vy, vz


def _scenario_figure_eight(layout: DroneLayout, t: float, total: int) -> tuple[float, float, float]:
    """Восьмёрка — каждый дрон трассирует лемнискату со сдвигом фазы."""
    phase = _phase(layout, total)
    omega = 0.35
    A = 110.0                              # A*omega = 38.5 м/с по оси
    theta = omega * t + phase
    # x = A·sin(θ), y = A·sin(θ)·cos(θ) = A/2·sin(2θ)
    vx = A * omega * math.cos(theta)
    vy = A * omega * math.cos(2.0 * theta)
    vz = 1.2 * math.sin(0.3 * t + phase)
    return vx, vy, vz


def _scenario_formation(layout: DroneLayout, t: float, total: int) -> tuple[float, float, float]:
    """V-строй — коллективный марш с плавным покачиванием курса."""
    heading = math.sin(0.15 * t) * math.radians(70.0)   # ±70° от севера
    speed = 11.0
    vx = speed * math.sin(heading)
    vy = speed * math.cos(heading)
    vz = 0.0
    return vx, vy, vz


def _scenario_spread(layout: DroneLayout, t: float, total: int) -> tuple[float, float, float]:
    """Разлёт — синусоидальное радиальное движение от центра и обратно."""
    phase = _phase(layout, total)
    amp = 14.0
    omega = 0.3
    speed = amp * math.sin(omega * t)
    vx = speed * math.cos(phase)
    vy = speed * math.sin(phase)
    vz = 0.0
    return vx, vy, vz


def _scenario_gather(layout: DroneLayout, t: float, total: int) -> tuple[float, float, float]:
    """Сбор — инверсия «Разлёта»: стартуем к центру."""
    phase = _phase(layout, total)
    amp = 14.0
    omega = 0.3
    speed = -amp * math.sin(omega * t)
    vx = speed * math.cos(phase)
    vy = speed * math.sin(phase)
    vz = 0.0
    return vx, vy, vz


SCENARIOS: dict[str, ScenarioFn] = {
    "orbit": _scenario_orbit,
    "figure8": _scenario_figure_eight,
    "formation": _scenario_formation,
    "spread": _scenario_spread,
    "gather": _scenario_gather,
}

SCENARIO_LIST = [
    {"id": "orbit",     "label": "Хоровод"},
    {"id": "figure8",   "label": "Восьмёрка"},
    {"id": "formation", "label": "V-строй"},
    {"id": "spread",    "label": "Разлёт"},
    {"id": "gather",    "label": "Сбор"},
]

DEFAULT_SCENARIO = "orbit"


def resolve_scenario(scenario_id: str) -> str:
    return scenario_id if scenario_id in SCENARIOS else DEFAULT_SCENARIO


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def build_command_payload(
    layout: DroneLayout,
    elapsed_sec: float,
    total_drones: int,
    scenario_id: str = DEFAULT_SCENARIO,
) -> dict:
    fn = SCENARIOS.get(scenario_id, SCENARIOS[DEFAULT_SCENARIO])
    vx, vy, vz = fn(layout, elapsed_sec, total_drones)
    heading = (math.degrees(math.atan2(vx, vy)) + 360.0) % 360.0
    return {
        "drone_id": layout.drone_id,
        "vx": round(_clamp(vx, -50.0, 50.0), 3),
        "vy": round(_clamp(vy, -50.0, 50.0), 3),
        "vz": round(_clamp(vz, -10.0, 10.0), 3),
        "mag_heading": round(heading % 360.0, 1),
    }


def build_stop_payload(layout: DroneLayout) -> dict:
    return {
        "drone_id": layout.drone_id,
        "vx": 0.0,
        "vy": 0.0,
        "vz": 0.0,
        "mag_heading": 0.0,
    }


def build_manual_command(
    drone_id: str,
    vx: float,
    vy: float,
    vz: float = 0.0,
    mag_heading: float | None = None,
) -> dict:
    vx = _clamp(float(vx), -50.0, 50.0)
    vy = _clamp(float(vy), -50.0, 50.0)
    vz = _clamp(float(vz), -10.0, 10.0)
    if mag_heading is None:
        heading = (math.degrees(math.atan2(vx, vy)) + 360.0) % 360.0
    else:
        heading = float(mag_heading) % 360.0
    return {
        "drone_id": drone_id,
        "vx": round(vx, 3),
        "vy": round(vy, 3),
        "vz": round(vz, 3),
        "mag_heading": round(heading, 1),
    }


def build_manual_home(
    drone_id: str,
    home_lat: float,
    home_lon: float,
    home_alt: float,
) -> dict:
    return {
        "drone_id": drone_id,
        "home_lat": round(float(home_lat), 7),
        "home_lon": round(float(home_lon), 7),
        "home_alt": round(float(home_alt), 1),
    }
