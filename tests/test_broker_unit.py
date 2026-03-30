import asyncio
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import broker  # type: ignore  # noqa: E402


class FakeKafkaProducer:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent_messages: list[dict[str, Any]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

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


class FakeKafkaMessage:
    def __init__(
        self,
        topic: str,
        value: bytes,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.topic = topic
        self.value = value
        self.headers = headers


class FakeKafkaConsumer:
    def __init__(self, *topics: str, messages: list[FakeKafkaMessage], **kwargs: Any) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self._messages = list(messages)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self) -> "FakeKafkaConsumer":
        return self

    async def __anext__(self) -> FakeKafkaMessage:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class FakeMqttMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class FakeAsyncMessageStream:
    def __init__(self, messages: list[FakeMqttMessage]) -> None:
        self._messages = list(messages)

    def __aiter__(self) -> "FakeAsyncMessageStream":
        return self

    async def __anext__(self) -> FakeMqttMessage:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class FakeMqttClient:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.entered = False
        self.exited = False
        self.subscriptions: list[tuple[str, int]] = []
        self.published: list[dict[str, Any]] = []
        self.messages = FakeAsyncMessageStream(
            [
                FakeMqttMessage(
                    "sitl.commands",
                    b'{"drone_id":"drone_001","vx":1.0,"vy":0.0,"vz":0.0,"mag_heading":45.0}',
                )
            ]
        )

    async def __aenter__(self) -> "FakeMqttClient":
        self.entered = True
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.exited = True

    async def subscribe(self, topic: str, qos: int) -> None:
        self.subscriptions.append((topic, qos))

    async def publish(self, topic: str, payload: str, qos: int, retain: bool) -> None:
        self.published.append(
            {
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retain": retain,
            }
        )


def test_load_broker_settings_defaults_to_kafka() -> None:
    settings = broker.load_broker_settings_from_env({})

    assert settings.backend == "kafka"
    assert settings.kafka_servers == "kafka:9092"
    assert settings.mqtt_host == "mosquitto"
    assert settings.mqtt_port == 1883
    assert settings.mqtt_qos == 1


def test_load_broker_settings_rejects_unknown_backend() -> None:
    try:
        broker.load_broker_settings_from_env({"BROKER_BACKEND": "nats"})
    except ValueError as exc:
        assert "unsupported BROKER_BACKEND" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unsupported broker backend")


def test_create_broker_client_from_env_returns_mqtt_client() -> None:
    client = broker.create_broker_client_from_env(
        {
            "BROKER_BACKEND": "mqtt",
            "MQTT_HOST": "mqtt.local",
            "MQTT_PORT": "1884",
            "MQTT_QOS": "1",
        }
    )

    assert isinstance(client, broker.MqttBrokerClient)


def test_kafka_broker_client_publishes_and_reads_messages() -> None:
    async def scenario() -> None:
        captured: dict[str, Any] = {}

        def producer_factory(**kwargs: Any) -> FakeKafkaProducer:
            producer = FakeKafkaProducer(**kwargs)
            captured["producer"] = producer
            return producer

        def consumer_factory(*topics: str, **kwargs: Any) -> FakeKafkaConsumer:
            consumer = FakeKafkaConsumer(
                *topics,
                messages=[
                    FakeKafkaMessage(
                        "sitl.commands",
                        b'{"drone_id":"drone_001"}',
                        headers=[("correlation_id", b"req-1")],
                    )
                ],
                **kwargs,
            )
            captured["consumer"] = consumer
            return consumer

        client = broker.KafkaBrokerClient(
            "kafka:29092",
            producer_factory=producer_factory,
            consumer_factory=consumer_factory,
        )

        await client.start()
        await client.publish(
            "sitl.verified-home",
            {"drone_id": "drone_001"},
            headers=[("correlation_id", b"req-1")],
        )

        messages: list[broker.BrokerMessage] = []
        async for message in client.iter_messages(["sitl.commands"], group_id="group-1"):
            messages.append(message)

        await client.stop()

        producer = captured["producer"]
        consumer = captured["consumer"]
        assert producer.started is True
        assert producer.stopped is True
        assert producer.sent_messages == [
            {
                "topic": "sitl.verified-home",
                "value": {"drone_id": "drone_001"},
                "headers": [("correlation_id", b"req-1")],
            }
        ]
        assert consumer.started is True
        assert consumer.stopped is True
        assert consumer.topics == ("sitl.commands",)
        assert consumer.kwargs["group_id"] == "group-1"
        assert messages == [
            broker.BrokerMessage(
                topic="sitl.commands",
                payload=b'{"drone_id":"drone_001"}',
                headers=[("correlation_id", b"req-1")],
            )
        ]

    asyncio.run(scenario())


def test_mqtt_broker_client_publishes_and_reads_messages() -> None:
    async def scenario() -> None:
        captured: dict[str, Any] = {}

        def client_factory(**kwargs: Any) -> FakeMqttClient:
            client = FakeMqttClient(**kwargs)
            captured["client"] = client
            return client

        client = broker.MqttBrokerClient(
            "mosquitto",
            1883,
            qos=1,
            client_factory=client_factory,
        )

        await client.start()
        await client.publish("sitl.telemetry.response", {"lat": 1.0, "lon": 2.0, "alt": 3.0})

        messages: list[broker.BrokerMessage] = []
        async for message in client.iter_messages(["sitl.commands"]):
            messages.append(message)

        await client.stop()

        mqtt_client = captured["client"]
        assert mqtt_client.entered is True
        assert mqtt_client.exited is True
        assert mqtt_client.subscriptions == [("sitl.commands", 1)]
        assert mqtt_client.published == [
            {
                "topic": "sitl.telemetry.response",
                "payload": '{"lat": 1.0, "lon": 2.0, "alt": 3.0}',
                "qos": 1,
                "retain": False,
            }
        ]
        assert messages == [
            broker.BrokerMessage(
                topic="sitl.commands",
                payload=b'{"drone_id":"drone_001","vx":1.0,"vy":0.0,"vz":0.0,"mag_heading":45.0}',
                headers=None,
            )
        ]

    asyncio.run(scenario())
