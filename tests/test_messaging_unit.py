import asyncio
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
TESTS = ROOT / "tests"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

import messaging  # type: ignore  # noqa: E402
import state  # type: ignore  # noqa: E402
from fakes import FakeRedis  # type: ignore  # noqa: E402


class FakeProducer:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, Any]] = []

    async def publish(
        self,
        topic: str,
        value: dict[str, Any],
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.sent_messages.append(
            {
                "topic": topic,
                "value": value,
                "headers": headers,
            }
        )


def test_dispatch_request_message_sends_response_to_reply_topic() -> None:
    async def scenario() -> None:
        producer = FakeProducer()

        async def handler(payload: dict[str, Any]) -> dict[str, Any] | None:
            assert payload == {"drone_id": "drone_001"}
            return {
                "lat": 59.9386,
                "lon": 30.3141,
                "alt": 120.0,
            }

        ok, detail, drone_id = await messaging.dispatch_request_message(
            producer,
            b'{"drone_id":"drone_001"}',
            [
                ("correlation_id", b"req-123"),
                ("reply_to", b"custom-position-response"),
            ],
            handler,
            "sitl.telemetry.response",
        )

        assert ok is True
        assert detail == "custom-position-response"
        assert drone_id == "drone_001"
        assert producer.sent_messages == [
            {
                "topic": "custom-position-response",
                "value": {
                    "lat": 59.9386,
                    "lon": 30.3141,
                    "alt": 120.0,
                    "correlation_id": "req-123",
                },
                "headers": [
                    ("correlation_id", b"req-123"),
                    ("reply_to", b"custom-position-response"),
                ],
            }
        ]

    asyncio.run(scenario())


def test_dispatch_request_message_accepts_transport_metadata_from_payload() -> None:
    async def scenario() -> None:
        producer = FakeProducer()

        async def handler(payload: dict[str, Any]) -> dict[str, Any] | None:
            assert payload == {
                "drone_id": "drone_001",
                "correlation_id": "req-456",
                "reply_to": "mqtt-position-response",
            }
            return {
                "lat": 59.9386,
                "lon": 30.3141,
                "alt": 120.0,
            }

        ok, detail, drone_id = await messaging.dispatch_request_message(
            producer,
            {
                "drone_id": "drone_001",
                "correlation_id": "req-456",
                "reply_to": "mqtt-position-response",
            },
            None,
            handler,
            "sitl.telemetry.response",
        )

        assert ok is True
        assert detail == "mqtt-position-response"
        assert drone_id == "drone_001"
        assert producer.sent_messages == [
            {
                "topic": "mqtt-position-response",
                "value": {
                    "lat": 59.9386,
                    "lon": 30.3141,
                    "alt": 120.0,
                    "correlation_id": "req-456",
                },
                "headers": [
                    ("correlation_id", b"req-456"),
                    ("reply_to", b"mqtt-position-response"),
                ],
            }
        ]

    asyncio.run(scenario())


def test_dispatch_request_message_rejects_invalid_response_shape() -> None:
    async def scenario() -> None:
        producer = FakeProducer()

        async def handler(payload: dict[str, Any]) -> dict[str, Any] | None:
            assert payload == {"drone_id": "drone_001"}
            return {
                "lat": 59.9386,
            }

        ok, detail, drone_id = await messaging.dispatch_request_message(
            producer,
            {"drone_id": "drone_001"},
            None,
            handler,
            "sitl.telemetry.response",
        )

        assert ok is False
        assert "required property" in detail
        assert drone_id == "drone_001"
        assert producer.sent_messages == []

    asyncio.run(scenario())


def test_resolve_position_request_accepts_transport_metadata_and_returns_position() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        drone_key = state.get_drone_state_key("drone_001")
        await redis_client.hset(
            drone_key,
            mapping=state.serialize_state(
                state.build_home_state(
                    {
                        "drone_id": "drone_001",
                        "home_lat": 59.9386,
                        "home_lon": 30.3141,
                        "home_alt": 120.0,
                    }
                )
            ),
        )

        response, reason = await messaging.resolve_position_request(
            redis_client,
            {
                "drone_id": "drone_001",
                "correlation_id": "req-123",
                "reply_to": "custom-topic",
            },
        )

        assert reason == ""
        assert response == {
            "lat": 59.9386,
            "lon": 30.3141,
            "alt": 120.0,
        }

    asyncio.run(scenario())


def test_resolve_position_request_returns_error_for_unknown_drone() -> None:
    async def scenario() -> None:
        redis_client = FakeRedis()
        response, reason = await messaging.resolve_position_request(
            redis_client,
            {"drone_id": "drone_404"},
        )

        assert response is None
        assert "state not found" in reason

    asyncio.run(scenario())
