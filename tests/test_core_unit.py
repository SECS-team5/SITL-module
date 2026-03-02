import asyncio
import json
import pathlib
import sys
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import core  # type: ignore  # noqa: E402


@dataclass
class FakeMessage:
    value: Dict[str, Any]


class FakeRedis:
    def __init__(self) -> None:
        self.storage: Dict[str, str] = {}
        self.set_calls: List[Dict[str, Any]] = []
        self.closed = False

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.storage[key] = value
        self.set_calls.append({"key": key, "value": value, "ex": ex})

    async def aclose(self) -> None:
        self.closed = True


class FakeProducer:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.sent: List[Dict[str, Any]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic: str, value: Dict[str, Any]) -> None:
        # Имитируем поведение сериализации из core: value уже dict,
        # но нам важен сам dict и топик.
        self.sent.append({"topic": topic, "value": value})


class FakeConsumer:
    def __init__(self, messages: List[FakeMessage]) -> None:
        self.messages = messages
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for msg in self.messages:
            yield msg


@pytest.mark.asyncio
async def test_core_processes_only_verified_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Проверяем, что core:
    - пропускает сообщения без verifier_stage == 'SITL-v1';
    - для валидных пишет полное сообщение в Redis под ключ SITL:<drone_id>;
    - публикует результат в выходной топик с core_stage == 'SITL-v1'.
    """

    fake_redis = FakeRedis()
    fake_producer = FakeProducer()

    valid_msg = FakeMessage(
        value={
            "verifier_stage": "SITL-v1",
            "data": {"drone_id": "dr-1", "foo": "bar"},
        }
    )
    invalid_msg = FakeMessage(
        value={
            "verifier_stage": "OTHER",
            "data": {"drone_id": "dr-2", "foo": "baz"},
        }
    )

    fake_consumer = FakeConsumer(messages=[invalid_msg, valid_msg])

    class DummyRedisModule:
        @staticmethod
        def from_url(url: str):
            return fake_redis

    # Подменяем зависимости внутри core
    monkeypatch.setenv("REDIS_URL", "redis://example:6379")
    monkeypatch.setenv("KAFKA_SERVERS", "kafka:29092")
    monkeypatch.setenv("INPUT_TOPIC", "verified-messages")
    monkeypatch.setenv("OUTPUT_TOPIC", "core-results")

    monkeypatch.setattr(core, "redis", DummyRedisModule)
    monkeypatch.setattr(core, "AIOKafkaProducer", lambda *args, **kwargs: fake_producer)
    monkeypatch.setattr(core, "AIOKafkaConsumer", lambda *args, **kwargs: fake_consumer)

    await core.main()

    # Проверяем, что consumer и producer корректно стартовали/остановились
    assert fake_consumer.started is True
    assert fake_consumer.stopped is True
    assert fake_producer.started is True
    assert fake_producer.stopped is True
    assert fake_redis.closed is True

    # В Redis должен попасть только второй (валидный) месседж
    assert len(fake_redis.set_calls) == 1
    call = fake_redis.set_calls[0]
    assert call["key"] == "SITL:dr-1"
    decoded_value = json.loads(call["value"])
    assert decoded_value["verifier_stage"] == "SITL-v1"
    assert decoded_value["data"]["drone_id"] == "dr-1"
    assert call["ex"] == 7200

    # Producer должен отправить ровно один результат в ожидаемый топик
    assert len(fake_producer.sent) == 1
    sent = fake_producer.sent[0]
    assert sent["topic"] == "core-results"
    assert sent["value"]["core_stage"] == "SITL-v1"
    assert sent["value"]["data"] == {"drone_id": "dr-1", "foo": "bar"}

