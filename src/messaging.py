import asyncio
import json
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Optional

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer

from contracts import build_request_headers
from contracts import decode_headers
from contracts import generate_correlation_id
from contracts import get_transport_value
from contracts import parse_json_payload
from contracts import POSITION_RESPONSE_TOPIC_DEFAULT


class RequestTransport(ABC):
    @abstractmethod
    async def request(
        self,
        topic: str,
        message: dict[str, Any],
        timeout: float = 30.0,
    ) -> Optional[dict[str, Any]]:
        """
        Отправляет запрос и ожидает ответ (синхронный request/response).

        Использует correlation_id для связывания запроса и ответа.

        Args:
            topic: Топик для отправки запроса
            message: Сообщение запроса (correlation_id будет добавлен автоматически)
            timeout: Таймаут ожидания ответа в секундах

        Returns:
            Dict: Ответное сообщение или None при таймауте
        """


class KafkaRequestTransport(RequestTransport):
    def __init__(
        self,
        kafka_servers: str,
        response_topic: str = POSITION_RESPONSE_TOPIC_DEFAULT,
    ) -> None:
        self.kafka_servers = kafka_servers
        self.response_topic = response_topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda value: json.dumps(value).encode(),
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def request(
        self,
        topic: str,
        message: dict[str, Any],
        timeout: float = 30.0,
    ) -> Optional[dict[str, Any]]:
        correlation_id = generate_correlation_id()
        request_payload = dict(message)
        reply_to = self.response_topic
        consumer = AIOKafkaConsumer(
            reply_to,
            bootstrap_servers=self.kafka_servers,
            group_id=None,
            auto_offset_reset="latest",
        )

        await consumer.start()
        try:
            await self._producer.send_and_wait(
                topic,
                request_payload,
                headers=build_request_headers(correlation_id, reply_to),
            )

            deadline = asyncio.get_running_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return None

                batch = await consumer.getmany(timeout_ms=min(int(remaining * 1000), 1000))
                for messages in batch.values():
                    for response_message in messages:
                        payload = parse_json_payload(response_message.value)
                        if payload is None:
                            continue

                        headers = decode_headers(response_message.headers)
                        response_correlation_id = get_transport_value(
                            payload,
                            headers,
                            "correlation_id",
                        )
                        if response_correlation_id == correlation_id:
                            return payload
        finally:
            await consumer.stop()
