"""
Конфигурация подключения к брокерам.

Читает параметры из переменных окружения.
"""
import os


def get_kafka_bootstrap() -> str:
    """Возвращает bootstrap servers для Kafka."""
    env = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if env:
        return env
    host = os.environ.get("KAFKA_HOST", "localhost")
    port = os.environ.get("KAFKA_PORT", "9092")
    return f"{host}:{port}"


def get_mqtt_broker() -> tuple:
    """Возвращает (host, port) для MQTT брокера."""
    host = os.environ.get("MQTT_HOST", os.environ.get("MQTT_BROKER", "localhost"))
    port = int(os.environ.get("MQTT_PORT", "1883"))
    return host, port
