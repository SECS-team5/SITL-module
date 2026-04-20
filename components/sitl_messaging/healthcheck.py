from __future__ import annotations

import asyncio
import os
import sys
import threading
from uuid import uuid4

import redis.asyncio as redis

from broker.bus_factory import create_system_bus
from shared.contracts import POSITION_REQUEST_TOPIC_DEFAULT
from shared.state import get_drone_state_key


def _configure_broker_env() -> str:
    backend = os.environ.get("BROKER_BACKEND", "kafka").lower()
    os.environ.setdefault("BROKER_TYPE", backend)
    os.environ.setdefault(
        "KAFKA_BOOTSTRAP_SERVERS",
        os.environ.get("KAFKA_SERVERS", "kafka:29092"),
    )
    os.environ.setdefault("SYSTEM_ID", "sitl-messaging-healthcheck")
    return backend


async def _seed_healthcheck_state(drone_id: str) -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    client = redis.from_url(redis_url, decode_responses=True)
    try:
        await client.hset(
            get_drone_state_key(drone_id),
            mapping={
                "status": "ARMED",
                "lat": "59.9386",
                "lon": "30.3141",
                "alt": "120.0",
                "home_lat": "59.9386",
                "home_lon": "30.3141",
                "home_alt": "120.0",
                "vx": "0.0",
                "vy": "0.0",
                "vz": "0.0",
                "mag_heading": "0.0",
                "speed_h_ms": "0.0",
                "speed_v_ms": "0.0",
            },
        )
        await client.expire(get_drone_state_key(drone_id), 60)
    finally:
        await client.aclose()


def _run_bus_healthcheck(
    topic: str,
    drone_id: str,
    timeout: float,
    client_id: str,
) -> dict | None:
    """
    Functional healthcheck идёт через тот же code path SystemBus, что и
    integration-тест: subscribe → publish request → wait for response.
    Если test_sitl_telemetry_request ломается — этот healthcheck тоже упадёт.
    """
    correlation_id = uuid4().hex
    response_topic = f"replies.{client_id}.healthcheck.{uuid4().hex[:8]}"
    response_holder: dict[str, object] = {}
    response_event = threading.Event()

    bus = create_system_bus(client_id=client_id)
    try:
        bus.start()

        def _on_response(message: dict) -> None:
            if message.get("correlation_id") != correlation_id:
                return
            response_holder["message"] = message
            response_event.set()

        bus.subscribe(response_topic, _on_response)
        bus.publish(
            topic,
            {
                "action": "request_position",
                "drone_id": drone_id,
                "correlation_id": correlation_id,
                "reply_to": response_topic,
            },
        )
        if not response_event.wait(timeout):
            return None
        response = response_holder.get("message")
        return response if isinstance(response, dict) else None
    finally:
        bus.stop()


def main() -> int:
    _configure_broker_env()
    topic = os.environ.get("POSITION_REQUEST_TOPIC", POSITION_REQUEST_TOPIC_DEFAULT)
    timeout = float(os.environ.get("SITL_MESSAGING_HEALTHCHECK_TIMEOUT_SEC", "10.0"))
    drone_id = os.environ.get("SITL_MESSAGING_HEALTHCHECK_DRONE_ID", "drone_9999")
    client_id = os.environ.get("SYSTEM_ID", "sitl-messaging-healthcheck")

    asyncio.run(_seed_healthcheck_state(drone_id))

    response = _run_bus_healthcheck(topic, drone_id, timeout, client_id)

    if response is None:
        print("healthcheck failed: no response from sitl_messaging", file=sys.stderr)
        return 1
    if response.get("payload", {}).get("lat") != 59.9386:
        print(f"healthcheck failed: unexpected payload {response}", file=sys.stderr)
        return 1

    print("healthcheck ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
