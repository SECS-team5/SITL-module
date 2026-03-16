import asyncio
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import messaging  # type: ignore  # noqa: E402


class FakeProducer:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, Any]] = []

    async def send_and_wait(
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
            "sitl-position-response",
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
                },
                "headers": [
                    ("correlation_id", b"req-123"),
                    ("reply_to", b"custom-position-response"),
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
            "sitl-position-response",
        )

        assert ok is False
        assert "required property" in detail
        assert drone_id == "drone_001"
        assert producer.sent_messages == []

    asyncio.run(scenario())
