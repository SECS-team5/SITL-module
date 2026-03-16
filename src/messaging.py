import json
import logging
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Optional

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer

from contracts import build_request_headers
from contracts import decode_headers
from contracts import get_transport_value
from contracts import parse_json_payload
from contracts import POSITION_RESPONSE_SCHEMA_NAME
from contracts import validate_schema

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


async def dispatch_request_message(
    producer: Any,
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

    ok, reason = validate_schema(response, POSITION_RESPONSE_SCHEMA_NAME)
    if not ok:
        return False, reason, drone_id

    response_headers = build_request_headers(correlation_id, reply_to) if correlation_id else None
    await producer.send_and_wait(
        reply_to,
        response,
        headers=response_headers,
    )
    return True, reply_to, drone_id


class KafkaRequestResponder(RequestResponder):
    def __init__(
        self,
        kafka_servers: str,
        consumer_group_id: str = "SITL-position-service-v1",
    ) -> None:
        self.kafka_servers = kafka_servers
        self.consumer_group_id = consumer_group_id
        self._producer = AIOKafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda value: json.dumps(value).encode(),
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def serve(
        self,
        topic: str,
        handler: RequestHandler,
        default_response_topic: str,
    ) -> None:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.consumer_group_id,
        )

        await self.start()
        await consumer.start()
        log.info(
            "Request responder started. request_topic=%s response_topic=%s",
            topic,
            default_response_topic,
        )

        try:
            async for msg in consumer:
                try:
                    ok, detail, drone_id = await dispatch_request_message(
                        self._producer,
                        msg.value,
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
            await consumer.stop()
            await self.stop()
