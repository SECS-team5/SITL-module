from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from broker.system_bus import SystemBus

from demo_site.scenarios import (
    DEFAULT_SCENARIO,
    DRONE_COLORS,
    DroneLayout,
    SCENARIO_LIST,
    TRAIL_POINTS,
    build_command_payload,
    build_home_payload,
    build_manual_command,
    build_manual_home,
    build_stop_payload,
    resolve_scenario,
)


@dataclass
class DroneState:
    lat: float
    lon: float
    alt: float
    heading: float = 0.0
    vx: float = 0.0
    vy: float = 0.0
    vz: float = 0.0
    speed: float = 0.0
    trail: list[dict[str, float]] = field(default_factory=list)
    last_latency_ms: float | None = None
    last_update_at: float = 0.0
    moved_recently: bool = False

    def push_trail(self, lat: float, lon: float) -> None:
        point = {"lat": round(lat, 7), "lon": round(lon, 7)}
        if not self.trail or self.trail[-1] != point:
            self.trail.append(point)
        if len(self.trail) > TRAIL_POINTS:
            del self.trail[: -TRAIL_POINTS]


class Simulation:
    """Отправляет HOME + COMMAND всем дронам и собирает их позиции через request/reply."""

    def __init__(
        self,
        *,
        home_topic: str,
        command_topic: str,
        request_topic: str,
        response_topic: str,
        verified_command_topic: str,
        verified_home_topic: str,
        command_interval_sec: float = 0.75,
        poll_interval_sec: float = 0.4,
        request_timeout_sec: float = 2.0,
    ) -> None:
        self._home_topic = home_topic
        self._command_topic = command_topic
        self._request_topic = request_topic
        self._response_topic = response_topic
        self._verified_command_topic = verified_command_topic
        self._verified_home_topic = verified_home_topic
        self._command_interval_sec = command_interval_sec
        self._poll_interval_sec = poll_interval_sec
        self._request_timeout_sec = request_timeout_sec

        self._bus: SystemBus | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscribed = False

        self._layouts: list[DroneLayout] = []
        self._states: dict[str, DroneState] = {}
        self._pending: dict[str, tuple[str, float]] = {}
        self._pending_by_drone: dict[str, str] = {}

        self._running = False
        self._started_at = 0.0
        self._scenario_id = DEFAULT_SCENARIO
        self._command_task: asyncio.Task | None = None
        self._poll_task: asyncio.Task | None = None

        # counters / activity tracking
        self._commands_sent = 0
        self._manual_commands_sent = 0
        self._manual_homes_sent = 0
        self._raw_published = 0
        self._raw_rejected_local = 0
        self._homes_sent = 0
        self._requests_sent = 0
        self._responses_received = 0
        self._verified_commands = 0
        self._verified_homes = 0
        self._last_command_at = 0.0
        self._last_home_at = 0.0
        self._last_request_at = 0.0
        self._last_response_at = 0.0
        self._last_verified_command_at = 0.0
        self._last_verified_home_at = 0.0
        self._last_movement_at = 0.0
        self._event_times: deque[float] = deque(maxlen=200)

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def attach_bus(self, bus: SystemBus) -> None:
        self._bus = bus
        if not self._subscribed:
            bus.subscribe(self._response_topic, self._on_response)
            bus.subscribe(self._verified_command_topic, self._on_verified_command)
            bus.subscribe(self._verified_home_topic, self._on_verified_home)
            self._subscribed = True
        if self._poll_task is None and self._loop is not None:
            self._poll_task = self._loop.create_task(self._poll_loop())

    @property
    def running(self) -> bool:
        return self._running

    @property
    def scenario_id(self) -> str:
        return self._scenario_id

    def set_scenario(self, scenario_id: str) -> str:
        self._scenario_id = resolve_scenario(scenario_id)
        return self._scenario_id

    def send_manual_command(
        self,
        drone_id: str,
        vx: float,
        vy: float,
        vz: float = 0.0,
        mag_heading: float | None = None,
    ) -> dict:
        if drone_id not in self._states:
            raise ValueError(f"unknown drone_id '{drone_id}'")
        if self._bus is None:
            raise RuntimeError("broker not connected")
        payload = build_manual_command(drone_id, vx, vy, vz, mag_heading)
        self._bus.publish(self._command_topic, payload)
        self._manual_commands_sent += 1
        self._event_times.append(time.monotonic())
        return payload

    def publish_raw(self, topic: str, payload: Any, local_valid: bool) -> None:
        if self._bus is None:
            raise RuntimeError("broker not connected")
        self._bus.publish(topic, payload)
        self._raw_published += 1
        if not local_valid:
            self._raw_rejected_local += 1
        self._event_times.append(time.monotonic())

    def send_manual_home(
        self,
        drone_id: str,
        home_lat: float,
        home_lon: float,
        home_alt: float,
    ) -> dict:
        if self._bus is None:
            raise RuntimeError("broker not connected")
        # Новый drone_id здесь допустим: после верификации он пройдёт через
        # sitl.verified-home и будет автоматически добавлен в карту как ручной.
        payload = build_manual_home(drone_id, home_lat, home_lon, home_alt)
        self._bus.publish(self._home_topic, payload)
        self._manual_homes_sent += 1
        self._event_times.append(time.monotonic())
        return payload

    async def start(self, layouts: list[DroneLayout]) -> None:
        await self._cancel_command_task()
        # Сохраняем ранее добавленных «ручных» дронов (auto=False), чтобы
        # перезапуск авто-сценария не убирал их из карты.
        manual_layouts = [l for l in self._layouts if not l.auto]
        manual_ids = {l.drone_id for l in manual_layouts}
        manual_states = {
            drone_id: self._states[drone_id]
            for drone_id in manual_ids
            if drone_id in self._states
        }

        self._layouts = [l for l in layouts if l.drone_id not in manual_ids] + manual_layouts
        self._states = {
            layout.drone_id: DroneState(lat=layout.home_lat, lon=layout.home_lon, alt=layout.home_alt)
            for layout in self._layouts
            if layout.auto
        }
        # Ручные дроны сохраняют накопленный след / последнюю позицию.
        self._states.update(manual_states)
        self._pending.clear()
        self._pending_by_drone.clear()

        if self._bus:
            for layout in self._layouts:
                if not layout.auto:
                    continue
                self._bus.publish(self._home_topic, build_home_payload(layout))
                self._homes_sent += 1
                self._last_home_at = time.monotonic()

        self._running = True
        self._started_at = time.monotonic()
        self._command_task = asyncio.create_task(self._command_loop())

    async def stop(self) -> None:
        await self._cancel_command_task()
        self._running = False
        if self._bus:
            for layout in self._layouts:
                if not layout.auto:
                    continue
                self._bus.publish(self._command_topic, build_stop_payload(layout))

    async def shutdown(self) -> None:
        await self._cancel_command_task()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

    def snapshot(self) -> dict[str, Any]:
        drones: list[dict[str, Any]] = []
        for layout in self._layouts:
            state = self._states.get(layout.drone_id)
            if state is None:
                continue
            drones.append({
                "drone_id": layout.drone_id,
                "label": layout.label,
                "color": layout.color,
                "auto": layout.auto,
                "lat": state.lat,
                "lon": state.lon,
                "alt": state.alt,
                "heading": state.heading,
                "speed": round(state.speed, 2),
                "vx": round(state.vx, 2),
                "vy": round(state.vy, 2),
                "vz": round(state.vz, 2),
                "home_lat": layout.home_lat,
                "home_lon": layout.home_lon,
                "trail": list(state.trail),
                "latency_ms": state.last_latency_ms,
                "moved": state.moved_recently,
            })
        return {
            "running": self._running,
            "drones": drones,
            "metrics": self._metrics(),
            "scenario": self._scenario_id,
            "scenarios": list(SCENARIO_LIST),
        }

    def _metrics(self) -> dict[str, Any]:
        now = time.monotonic()
        window = 2.0
        cutoff = now - 5.0
        while self._event_times and self._event_times[0] < cutoff:
            self._event_times.popleft()
        rate = len(self._event_times) / 5.0

        def fresh(ts: float) -> bool:
            return ts > 0 and (now - ts) < window

        any_moved = any(s.moved_recently for s in self._states.values())
        last_moved = self._last_movement_at

        return {
            "commands_sent": self._commands_sent,
            "homes_sent": self._homes_sent,
            "manual_commands_sent": self._manual_commands_sent,
            "manual_homes_sent": self._manual_homes_sent,
            "raw_published": self._raw_published,
            "raw_rejected_local": self._raw_rejected_local,
            "requests_sent": self._requests_sent,
            "responses_received": self._responses_received,
            "verified_commands": self._verified_commands,
            "verified_homes": self._verified_homes,
            "rate_msg_per_sec": round(rate, 1),
            "pipeline": {
                "runner": {
                    "active": fresh(self._last_command_at),
                    "count": self._commands_sent,
                    "label": "Demo Runner",
                },
                "verifier": {
                    "active": fresh(self._last_verified_command_at) or fresh(self._last_verified_home_at),
                    "count": self._verified_commands + self._verified_homes,
                    "label": "Verifier",
                },
                "controller": {
                    "active": any_moved and fresh(last_moved),
                    "count": self._responses_received,
                    "label": "Controller",
                },
                "core": {
                    "active": any_moved and fresh(last_moved),
                    "count": self._responses_received,
                    "label": "Core",
                },
                "messaging": {
                    "active": fresh(self._last_response_at),
                    "count": self._responses_received,
                    "label": "Messaging",
                },
            },
        }

    async def _cancel_command_task(self) -> None:
        if self._command_task is None:
            return
        self._command_task.cancel()
        try:
            await self._command_task
        except asyncio.CancelledError:
            pass
        self._command_task = None

    async def _command_loop(self) -> None:
        await asyncio.sleep(0.6)
        while self._running:
            if self._bus and self._layouts:
                elapsed = time.monotonic() - self._started_at
                auto_layouts = [l for l in self._layouts if l.auto]
                total = len(auto_layouts)
                if total:
                    now = time.monotonic()
                    for idx, layout in enumerate(auto_layouts):
                        # index в формуле сценариев привязан к позиции в авто-группе
                        layout.index = idx
                        payload = build_command_payload(layout, elapsed, total, self._scenario_id)
                        self._bus.publish(self._command_topic, payload)
                        state = self._states.get(layout.drone_id)
                        if state is not None:
                            state.vx = float(payload["vx"])
                            state.vy = float(payload["vy"])
                            state.vz = float(payload["vz"])
                    self._commands_sent += total
                    self._last_command_at = now
                    self._event_times.append(now)
            await asyncio.sleep(self._command_interval_sec)

    async def _poll_loop(self) -> None:
        while True:
            self._expire_pending()
            if self._bus and self._layouts:
                now = time.monotonic()
                for layout in self._layouts:
                    if layout.drone_id in self._pending_by_drone:
                        continue
                    corr_id = uuid4().hex
                    self._pending[corr_id] = (layout.drone_id, now)
                    self._pending_by_drone[layout.drone_id] = corr_id
                    self._bus.publish(self._request_topic, {
                        "action": "request_position",
                        "drone_id": layout.drone_id,
                        "payload": {"drone_id": layout.drone_id},
                        "correlation_id": corr_id,
                        "reply_to": self._response_topic,
                    })
                    self._requests_sent += 1
                    self._last_request_at = now
            await asyncio.sleep(self._poll_interval_sec)

    def _expire_pending(self) -> None:
        now = time.monotonic()
        expired = [cid for cid, (_, sent) in self._pending.items() if (now - sent) > self._request_timeout_sec]
        for cid in expired:
            drone_id, _ = self._pending.pop(cid)
            self._pending_by_drone.pop(drone_id, None)

    def _on_response(self, message: dict[str, Any]) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._record_response, message)

    def _on_verified_command(self, _message: dict[str, Any]) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._bump_verified, "command")

    def _on_verified_home(self, message: dict[str, Any]) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._record_verified_home, message)

    def _bump_verified(self, kind: str) -> None:
        now = time.monotonic()
        if kind == "command":
            self._verified_commands += 1
            self._last_verified_command_at = now
        else:
            self._verified_homes += 1
            self._last_verified_home_at = now
        self._event_times.append(now)

    def _record_verified_home(self, message: dict[str, Any]) -> None:
        self._bump_verified("home")

        payload = message.get("payload", message)
        if not isinstance(payload, dict):
            return
        drone_id = str(payload.get("drone_id", "")).strip()
        if not drone_id:
            return
        try:
            home_lat = float(payload["home_lat"])
            home_lon = float(payload["home_lon"])
            home_alt = float(payload.get("home_alt", 0.0))
        except (KeyError, TypeError, ValueError):
            return

        existing = next((l for l in self._layouts if l.drone_id == drone_id), None)
        if existing is not None:
            existing.home_lat = round(home_lat, 7)
            existing.home_lon = round(home_lon, 7)
            existing.home_alt = round(home_alt, 1)
            state = self._states.get(drone_id)
            if state is None:
                self._states[drone_id] = DroneState(lat=home_lat, lon=home_lon, alt=home_alt)
            return

        # Новый дрон: заведён ручной публикацией HOME. Добавим в карту
        # как «ручной» — авто-сценарий его не трогает, но позицию поллим.
        index = len(self._layouts)
        label_num = len([l for l in self._layouts if not l.auto]) + 1
        layout = DroneLayout(
            drone_id=drone_id,
            label=f"M{label_num:02d}",
            color=DRONE_COLORS[index % len(DRONE_COLORS)],
            index=index,
            home_lat=round(home_lat, 7),
            home_lon=round(home_lon, 7),
            home_alt=round(home_alt, 1),
            auto=False,
        )
        self._layouts.append(layout)
        self._states[drone_id] = DroneState(lat=home_lat, lon=home_lon, alt=home_alt)

    def _record_response(self, message: dict[str, Any]) -> None:
        corr_id = str(message.get("correlation_id", "")).strip()
        if not corr_id or corr_id not in self._pending:
            return
        drone_id, sent_at = self._pending.pop(corr_id)
        self._pending_by_drone.pop(drone_id, None)

        payload = message.get("payload", message)
        if not isinstance(payload, dict):
            return
        state = self._states.get(drone_id)
        if state is None:
            return

        now = time.monotonic()
        prev_lat, prev_lon = state.lat, state.lon
        state.lat = float(payload.get("lat", state.lat))
        state.lon = float(payload.get("lon", state.lon))
        state.alt = float(payload.get("alt", state.alt))
        state.last_latency_ms = round((now - sent_at) * 1000.0, 1)
        state.last_update_at = now
        state.push_trail(state.lat, state.lon)
        state.speed = (state.vx * state.vx + state.vy * state.vy) ** 0.5

        moved = abs(state.lat - prev_lat) > 1e-7 or abs(state.lon - prev_lon) > 1e-7
        state.moved_recently = moved
        if moved:
            self._last_movement_at = now

        if len(state.trail) >= 2:
            import math
            prev = state.trail[-2]
            dx = state.lon - prev["lon"]
            dy = state.lat - prev["lat"]
            if abs(dx) > 1e-9 or abs(dy) > 1e-9:
                state.heading = (math.degrees(math.atan2(dx, dy)) + 360.0) % 360.0

        self._responses_received += 1
        self._last_response_at = now
        self._event_times.append(now)
