from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

from aiohttp import web

from broker.bus_factory import create_system_bus
from demo_site.runtime import Simulation
from demo_site.scenarios import MAX_DRONES, build_layout, clamp_drone_count
from shared.contracts import (
    COMMAND_SCHEMA_NAME,
    HOME_SCHEMA_NAME,
    POSITION_REQUEST_SCHEMA_NAME,
    POSITION_REQUEST_TOPIC_DEFAULT,
    POSITION_RESPONSE_TOPIC_DEFAULT,
    VERIFIED_COMMAND_TOPIC_DEFAULT,
    VERIFIED_HOME_TOPIC_DEFAULT,
    validate_schema,
)


RAW_TOPIC_SCHEMA: dict[str, str] = {
    "sitl.commands": COMMAND_SCHEMA_NAME,
    "sitl-drone-home": HOME_SCHEMA_NAME,
    "sitl.telemetry.request": POSITION_REQUEST_SCHEMA_NAME,
}


STATIC_DIR = Path(__file__).resolve().parent / "static"


class DemoService:
    def __init__(self) -> None:
        self._bus = None
        self._lock = asyncio.Lock()
        self._connect_error = "не подключено"
        self._next_attempt = 0.0
        self._reconnect_sec = 5.0

        self._base_lat = float(os.getenv("DEMO_CENTER_LAT", "59.9386"))
        self._base_lon = float(os.getenv("DEMO_CENTER_LON", "30.3141"))
        self._base_alt = float(os.getenv("DEMO_CENTER_ALT", "120.0"))

        self._sim = Simulation(
            home_topic=os.getenv("HOME_TOPIC", "sitl-drone-home"),
            command_topic=os.getenv("COMMAND_TOPIC", "sitl.commands"),
            request_topic=os.getenv("POSITION_REQUEST_TOPIC", POSITION_REQUEST_TOPIC_DEFAULT),
            response_topic=os.getenv(
                "DEMO_RESPONSE_TOPIC",
                f"{os.getenv('POSITION_RESPONSE_TOPIC', POSITION_RESPONSE_TOPIC_DEFAULT)}.demo",
            ),
            verified_command_topic=os.getenv(
                "VERIFIED_COMMAND_TOPIC", VERIFIED_COMMAND_TOPIC_DEFAULT
            ),
            verified_home_topic=os.getenv(
                "VERIFIED_HOME_TOPIC", VERIFIED_HOME_TOPIC_DEFAULT
            ),
        )

    async def ensure_connected(self) -> bool:
        if self._bus is not None:
            return True
        now = time.monotonic()
        if now < self._next_attempt:
            return False

        async with self._lock:
            if self._bus is not None:
                return True
            self._next_attempt = now + self._reconnect_sec
            try:
                bus = await asyncio.to_thread(self._connect_bus)
            except Exception as exc:
                self._connect_error = str(exc)
                return False

            self._bus = bus
            self._connect_error = ""
            self._sim.set_loop(asyncio.get_running_loop())
            self._sim.attach_bus(bus)
            return True

    def _connect_bus(self):
        backend = os.environ.get("BROKER_BACKEND", "mqtt").lower()
        os.environ.setdefault("BROKER_TYPE", backend)
        os.environ.setdefault("SYSTEM_ID", "demo-site")
        os.environ.setdefault(
            "KAFKA_BOOTSTRAP_SERVERS",
            os.environ.get("KAFKA_SERVERS", "localhost:9092"),
        )
        bus = create_system_bus()
        bus.start()
        return bus

    async def shutdown(self) -> None:
        await self._sim.shutdown()
        if self._bus is not None:
            bus = self._bus
            self._bus = None
            await asyncio.to_thread(bus.stop)

    async def state(self) -> dict[str, Any]:
        await self.ensure_connected()
        snap = self._sim.snapshot()
        snap["connected"] = self._bus is not None
        snap["error"] = self._connect_error if self._bus is None else ""
        snap["max_drones"] = MAX_DRONES
        return snap

    async def start(self, count: int) -> dict[str, Any]:
        count = clamp_drone_count(count)
        if not await self.ensure_connected():
            raise web.HTTPServiceUnavailable(
                text=f'{{"error": "broker недоступен: {self._connect_error}"}}',
                content_type="application/json",
            )
        layouts = build_layout(count, self._base_lat, self._base_lon, self._base_alt)
        await self._sim.start(layouts)
        return await self.state()

    async def stop(self) -> dict[str, Any]:
        await self._sim.stop()
        return await self.state()

    async def set_scenario(self, scenario_id: str) -> dict[str, Any]:
        self._sim.set_scenario(scenario_id)
        return await self.state()

    async def manual_command(self, body: dict[str, Any]) -> dict[str, Any]:
        drone_id = str(body.get("drone_id", "")).strip()
        if not drone_id:
            raise web.HTTPBadRequest(
                text='{"error": "drone_id обязателен"}', content_type="application/json"
            )
        if not await self.ensure_connected():
            raise web.HTTPServiceUnavailable(
                text=f'{{"error": "broker недоступен: {self._connect_error}"}}',
                content_type="application/json",
            )
        try:
            payload = self._sim.send_manual_command(
                drone_id,
                float(body.get("vx", 0.0)),
                float(body.get("vy", 0.0)),
                float(body.get("vz", 0.0)),
                body.get("mag_heading"),
            )
        except (ValueError, RuntimeError) as exc:
            raise web.HTTPBadRequest(
                text=f'{{"error": "{exc}"}}', content_type="application/json"
            )
        return {"ok": True, "sent": payload}

    async def publish_raw(self, body: dict[str, Any]) -> dict[str, Any]:
        topic = str(body.get("topic", "")).strip()
        if topic not in RAW_TOPIC_SCHEMA:
            allowed = ", ".join(RAW_TOPIC_SCHEMA)
            raise web.HTTPBadRequest(
                text=f'{{"error": "топик должен быть одним из: {allowed}"}}',
                content_type="application/json",
            )
        payload = body.get("payload")
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(
                text='{"error": "payload должен быть JSON-объектом"}',
                content_type="application/json",
            )
        if not await self.ensure_connected():
            raise web.HTTPServiceUnavailable(
                text=f'{{"error": "broker недоступен: {self._connect_error}"}}',
                content_type="application/json",
            )

        schema = RAW_TOPIC_SCHEMA[topic]
        local_valid, reason = validate_schema(payload, schema)
        self._sim.publish_raw(topic, payload, local_valid)
        return {
            "ok": True,
            "published": True,
            "topic": topic,
            "schema": schema,
            "local_valid": local_valid,
            "reason": reason,
        }

    async def manual_home(self, body: dict[str, Any]) -> dict[str, Any]:
        drone_id = str(body.get("drone_id", "")).strip()
        if not drone_id:
            raise web.HTTPBadRequest(
                text='{"error": "drone_id обязателен"}', content_type="application/json"
            )
        if not await self.ensure_connected():
            raise web.HTTPServiceUnavailable(
                text=f'{{"error": "broker недоступен: {self._connect_error}"}}',
                content_type="application/json",
            )
        try:
            payload = self._sim.send_manual_home(
                drone_id,
                float(body["home_lat"]),
                float(body["home_lon"]),
                float(body["home_alt"]),
            )
        except (KeyError, ValueError, RuntimeError) as exc:
            raise web.HTTPBadRequest(
                text=f'{{"error": "{exc}"}}', content_type="application/json"
            )
        return {"ok": True, "sent": payload}


async def _index(_: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC_DIR / "index.html")


async def _state(request: web.Request) -> web.Response:
    return web.json_response(await request.app["svc"].state())


async def _start(request: web.Request) -> web.Response:
    body = await request.json()
    return web.json_response(await request.app["svc"].start(int(body.get("count", 6))))


async def _stop(request: web.Request) -> web.Response:
    return web.json_response(await request.app["svc"].stop())


async def _scenario(request: web.Request) -> web.Response:
    body = await request.json()
    scenario_id = str(body.get("id") or body.get("scenario") or "").strip()
    return web.json_response(await request.app["svc"].set_scenario(scenario_id))


async def _manual_command(request: web.Request) -> web.Response:
    body = await request.json()
    return web.json_response(await request.app["svc"].manual_command(body))


async def _manual_home(request: web.Request) -> web.Response:
    body = await request.json()
    return web.json_response(await request.app["svc"].manual_home(body))


async def _raw_publish(request: web.Request) -> web.Response:
    body = await request.json()
    return web.json_response(await request.app["svc"].publish_raw(body))


async def _cleanup(app: web.Application) -> None:
    await app["svc"].shutdown()


def create_demo_app() -> web.Application:
    app = web.Application()
    app["svc"] = DemoService()
    app.router.add_get("/", _index)
    app.router.add_get("/api/state", _state)
    app.router.add_post("/api/start", _start)
    app.router.add_post("/api/stop", _stop)
    app.router.add_post("/api/scenario", _scenario)
    app.router.add_post("/api/manual/command", _manual_command)
    app.router.add_post("/api/manual/home", _manual_home)
    app.router.add_post("/api/raw/publish", _raw_publish)
    app.router.add_static("/static/", path=str(STATIC_DIR), show_index=False)
    app.on_cleanup.append(_cleanup)
    return app
