import logging
import os
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional

import asyncio
import redis.asyncio as redis
from broker import BrokerClient
from broker import create_broker_client_from_env
from broker import iter_broker_messages_with_retry
from broker import start_broker_with_retry

from contracts import build_request_headers
from contracts import decode_headers
from contracts import get_transport_value
from contracts import parse_json_payload
from contracts import POSITION_REQUEST_SCHEMA_NAME
from contracts import POSITION_REQUEST_TOPIC_DEFAULT
from contracts import POSITION_RESPONSE_TOPIC_DEFAULT
from contracts import POSITION_RESPONSE_SCHEMA_NAME
from contracts import validate_schema
from state import build_position_response
from state import normalize_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

RequestHandler = Callable[[dict[str, Any]], Awaitable[Optional[dict[str, Any]]]]


class RequestResponder(ABC):
    @abstractmethod
    async def serve(
        self,
        topic: str,
        handler: RequestHandler,
        default_response_topic: str,
    ) -> None:
        """
        Принимает request-сообщения из topic и публикует ответы.

        Использует correlation_id и reply_to из transport metadata.
        """


async def resolve_position_request(
    r: redis.Redis,
    payload: dict[str, Any],
) -> tuple[dict[str, float] | None, str]:
    ok, reason = validate_schema(payload, POSITION_REQUEST_SCHEMA_NAME)
    if not ok:
        return None, reason

    drone_id = payload["drone_id"]
    raw_state = await r.hgetall(f"drone:{drone_id}:state")
    if not raw_state:
        return None, f"drone '{drone_id}' state not found"

    response = build_position_response(normalize_state(raw_state))
    if response is None:
        return None, f"drone '{drone_id}' state does not contain a valid position"

    return response, ""


async def handle_position_request(
    r: redis.Redis,
    payload: dict[str, Any],
) -> dict[str, float] | None:
    response, reason = await resolve_position_request(r, payload)
    if response is None:
        log.warning("Rejected position request: %s", reason)
        return None
    return response


async def dispatch_request_message(
    publisher: Any,
    raw_payload: Any,
    raw_headers: list[tuple[str, bytes]] | None,
    handler: RequestHandler,
    default_response_topic: str,
) -> tuple[bool, str, str]:
    payload = parse_json_payload(raw_payload)
    if payload is None:
        return False, "invalid JSON payload", ""

    drone_id = str(payload.get("drone_id", ""))
    headers = decode_headers(raw_headers)
    reply_to = get_transport_value(payload, headers, "reply_to") or default_response_topic
    correlation_id = get_transport_value(payload, headers, "correlation_id")

    response = await handler(payload)
    if response is None:
        return False, "handler returned no response", drone_id

    response_payload = dict(response)
    if correlation_id:
        response_payload["correlation_id"] = correlation_id

    ok, reason = validate_schema(response_payload, POSITION_RESPONSE_SCHEMA_NAME)
    if not ok:
        return False, reason, drone_id

    response_headers = build_request_headers(correlation_id, reply_to) if correlation_id else None
    await publisher.publish(
        reply_to,
        response_payload,
        headers=response_headers,
    )
    return True, reply_to, drone_id


class BrokerRequestResponder(RequestResponder):
    def __init__(
        self,
        broker: BrokerClient,
        consumer_group_id: str = "SITL-position-service-v1",
    ) -> None:
        self.broker = broker
        self.consumer_group_id = consumer_group_id

    async def serve(
        self,
        topic: str,
        handler: RequestHandler,
        default_response_topic: str,
    ) -> None:
        await start_broker_with_retry(self.broker, log, "Messaging broker")
        log.info(
            "Request responder started. request_topic=%s response_topic=%s",
            topic,
            default_response_topic,
        )

        try:
            async for msg in iter_broker_messages_with_retry(
                self.broker,
                [topic],
                log,
                "Messaging broker",
                group_id=self.consumer_group_id,
            ):
                try:
                    ok, detail, drone_id = await dispatch_request_message(
                        self.broker,
                        msg.payload,
                        msg.headers,
                        handler,
                        default_response_topic,
                    )
                    if not ok:
                        log.warning("Rejected request from topic=%s: %s", msg.topic, detail)
                        continue

                    log.info(
                        "Returned position for drone_id=%s reply_to=%s",
                        drone_id,
                        detail,
                    )
                except Exception as exc:
                    log.error("Failed to process request from topic=%s: %s", msg.topic, exc, exc_info=True)
        finally:
            await self.broker.stop()


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    request_topic = os.getenv("POSITION_REQUEST_TOPIC", POSITION_REQUEST_TOPIC_DEFAULT)
    response_topic = os.getenv("POSITION_RESPONSE_TOPIC", POSITION_RESPONSE_TOPIC_DEFAULT)
    r = redis.from_url(redis_url, decode_responses=True)
    broker = create_broker_client_from_env()
    responder = BrokerRequestResponder(broker)

    log.info(
        "Messaging started. redis=%s request_topic=%s",
        redis_url,
        request_topic,
    )

    async def position_request_handler(payload: dict[str, Any]) -> dict[str, float] | None:
        return await handle_position_request(r, payload)

    try:
        await responder.serve(request_topic, position_request_handler, response_topic)
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
