import asyncio
import json
import logging
import os
from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from typing import Callable

import aiomqtt
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer

DEFAULT_BROKER_BACKEND = "kafka"
DEFAULT_KAFKA_SERVERS = "kafka:9092"
DEFAULT_MQTT_HOST = "mosquitto"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_QOS = 1
RETRY_DELAYS_SEC = (1.0, 2.0, 4.0, 8.0, 10.0)


@dataclass(frozen=True)
class BrokerMessage:
    topic: str
    payload: Any
    headers: list[tuple[str, bytes]] | None = None


@dataclass(frozen=True)
class BrokerSettings:
    backend: str
    kafka_servers: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_qos: int


def _blank_to_none(value: str) -> str | None:
    stripped = value.strip()
    return stripped or None


def load_broker_settings_from_env(env: Mapping[str, str] | None = None) -> BrokerSettings:
    source = env if env is not None else os.environ
    backend = source.get("BROKER_BACKEND", DEFAULT_BROKER_BACKEND).strip().lower() or DEFAULT_BROKER_BACKEND
    if backend not in {"kafka", "mqtt"}:
        raise ValueError(f"unsupported BROKER_BACKEND '{backend}'")

    mqtt_port = int(source.get("MQTT_PORT", str(DEFAULT_MQTT_PORT)))
    mqtt_qos = int(source.get("MQTT_QOS", str(DEFAULT_MQTT_QOS)))
    if mqtt_qos not in {0, 1, 2}:
        raise ValueError(f"unsupported MQTT_QOS '{mqtt_qos}'")

    return BrokerSettings(
        backend=backend,
        kafka_servers=source.get("KAFKA_SERVERS", DEFAULT_KAFKA_SERVERS).strip() or DEFAULT_KAFKA_SERVERS,
        mqtt_host=source.get("MQTT_HOST", DEFAULT_MQTT_HOST).strip() or DEFAULT_MQTT_HOST,
        mqtt_port=mqtt_port,
        mqtt_username=_blank_to_none(source.get("MQTT_USERNAME", "")),
        mqtt_password=_blank_to_none(source.get("MQTT_PASSWORD", "")),
        mqtt_qos=mqtt_qos,
    )


class BrokerClient(ABC):
    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        pass

    @abstractmethod
    def iter_messages(
        self,
        topics: Sequence[str],
        group_id: str | None = None,
    ) -> AsyncIterator[BrokerMessage]:
        pass


class KafkaBrokerClient(BrokerClient):
    def __init__(
        self,
        kafka_servers: str,
        producer_factory: Callable[..., Any] = AIOKafkaProducer,
        consumer_factory: Callable[..., Any] = AIOKafkaConsumer,
    ) -> None:
        self.kafka_servers = kafka_servers
        self._producer_factory = producer_factory
        self._consumer_factory = consumer_factory
        self._producer: Any | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return

        self._producer = self._producer_factory(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda value: json.dumps(value).encode(),
        )
        try:
            await self._producer.start()
        except Exception:
            self._producer = None
            raise

    async def stop(self) -> None:
        if self._producer is None:
            return

        producer = self._producer
        self._producer = None
        await producer.stop()

    async def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaBrokerClient must be started before publish")

        await self._producer.send_and_wait(topic, payload, headers=headers)

    async def _iter_consumer_messages(
        self,
        topics: Sequence[str],
        group_id: str | None = None,
    ) -> AsyncIterator[BrokerMessage]:
        consumer = self._consumer_factory(
            *topics,
            bootstrap_servers=self.kafka_servers,
            group_id=group_id,
        )
        started = False
        try:
            await consumer.start()
            started = True
            async for message in consumer:
                yield BrokerMessage(
                    topic=message.topic,
                    payload=message.value,
                    headers=message.headers,
                )
        finally:
            if started:
                await consumer.stop()

    def iter_messages(
        self,
        topics: Sequence[str],
        group_id: str | None = None,
    ) -> AsyncIterator[BrokerMessage]:
        return self._iter_consumer_messages(topics, group_id=group_id)


class MqttBrokerClient(BrokerClient):
    def __init__(
        self,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        qos: int = DEFAULT_MQTT_QOS,
        client_factory: Callable[..., Any] = aiomqtt.Client,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.qos = qos
        self._client_factory = client_factory
        self._client: Any | None = None

    async def start(self) -> None:
        if self._client is not None:
            return

        client = self._client_factory(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )
        try:
            await client.__aenter__()
        except Exception:
            raise
        self._client = client

    async def stop(self) -> None:
        if self._client is None:
            return

        client = self._client
        self._client = None
        await client.__aexit__(None, None, None)

    async def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        del headers
        if self._client is None:
            raise RuntimeError("MqttBrokerClient must be started before publish")

        await self._client.publish(
            topic,
            payload=json.dumps(payload),
            qos=self.qos,
            retain=False,
        )

    async def _iter_client_messages(
        self,
        topics: Sequence[str],
        group_id: str | None = None,
    ) -> AsyncIterator[BrokerMessage]:
        del group_id
        if self._client is None:
            raise RuntimeError("MqttBrokerClient must be started before iter_messages")

        for topic in topics:
            await self._client.subscribe(topic, qos=self.qos)

        async for message in self._client.messages:
            yield BrokerMessage(
                topic=str(message.topic),
                payload=message.payload,
                headers=None,
            )

    def iter_messages(
        self,
        topics: Sequence[str],
        group_id: str | None = None,
    ) -> AsyncIterator[BrokerMessage]:
        return self._iter_client_messages(topics, group_id=group_id)


def create_broker_client(settings: BrokerSettings) -> BrokerClient:
    if settings.backend == "kafka":
        return KafkaBrokerClient(settings.kafka_servers)
    if settings.backend == "mqtt":
        return MqttBrokerClient(
            settings.mqtt_host,
            settings.mqtt_port,
            username=settings.mqtt_username,
            password=settings.mqtt_password,
            qos=settings.mqtt_qos,
        )
    raise ValueError(f"unsupported BROKER_BACKEND '{settings.backend}'")


def create_broker_client_from_env(env: Mapping[str, str] | None = None) -> BrokerClient:
    return create_broker_client(load_broker_settings_from_env(env))


def _next_retry_delay(current_delay: float) -> float:
    for delay in RETRY_DELAYS_SEC:
        if delay > current_delay:
            return delay
    return RETRY_DELAYS_SEC[-1]


async def start_broker_with_retry(
    client: BrokerClient,
    log: logging.Logger,
    label: str,
) -> None:
    delay = RETRY_DELAYS_SEC[0]
    while True:
        try:
            await client.start()
            return
        except Exception as exc:
            log.warning("%s unavailable: %s. Retrying in %.0fs", label, exc, delay)
            await asyncio.sleep(delay)
            delay = _next_retry_delay(delay)


async def iter_broker_messages_with_retry(
    client: BrokerClient,
    topics: Sequence[str],
    log: logging.Logger,
    label: str,
    group_id: str | None = None,
) -> AsyncIterator[BrokerMessage]:
    delay = RETRY_DELAYS_SEC[0]
    while True:
        try:
            async for message in client.iter_messages(topics, group_id=group_id):
                delay = RETRY_DELAYS_SEC[0]
                yield message
            return
        except Exception as exc:
            log.warning("%s subscription unavailable: %s. Retrying in %.0fs", label, exc, delay)
            await asyncio.sleep(delay)
            delay = _next_retry_delay(delay)
