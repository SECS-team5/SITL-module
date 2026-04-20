import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import broker.kafka.kafka_system_bus as kafka_system_bus
from broker.bus_factory import create_system_bus


def test_create_system_bus_returns_mqtt_bus(monkeypatch):
    monkeypatch.setenv("BROKER_TYPE", "mqtt")
    monkeypatch.setenv("MQTT_BROKER", "mosquitto")
    monkeypatch.setenv("MQTT_PORT", "1883")

    bus = create_system_bus()

    assert bus.__class__.__name__ == "MQTTSystemBus"
    assert bus.broker == "mosquitto"
    assert bus.port == 1883


def test_create_system_bus_uses_broker_backend_when_broker_type_absent(monkeypatch):
    monkeypatch.delenv("BROKER_TYPE", raising=False)
    monkeypatch.setenv("BROKER_BACKEND", "mqtt")
    monkeypatch.setenv("MQTT_BROKER", "mosquitto")
    monkeypatch.setenv("MQTT_PORT", "1883")

    bus = create_system_bus()

    assert bus.__class__.__name__ == "MQTTSystemBus"


def test_create_system_bus_uses_kafka_servers_fallback(monkeypatch):
    monkeypatch.delenv("BROKER_TYPE", raising=False)
    monkeypatch.setenv("BROKER_BACKEND", "kafka")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setenv("KAFKA_SERVERS", "kafka:29092")
    monkeypatch.setattr(kafka_system_bus, "KAFKA_AVAILABLE", True)

    bus = create_system_bus(client_id="fallback-bus")

    assert bus.__class__.__name__ == "KafkaSystemBus"
    assert bus.bootstrap_servers == "kafka:29092"


def test_kafka_system_bus_uses_explicit_sasl_env(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "alice")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "secret")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setattr(kafka_system_bus, "KAFKA_AVAILABLE", True)

    bus = kafka_system_bus.KafkaSystemBus(
        client_id="test-bus",
        bootstrap_servers="kafka:9092",
    )

    assert bus.get_sasl_config() == {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": "alice",
        "sasl_plain_password": "secret",
    }
