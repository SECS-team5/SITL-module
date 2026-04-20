"""
Factory для создания SystemBus на основе конфигурации.
Поддерживаемые типы: kafka, mqtt.
"""
import os
from typing import Dict, Optional

from .system_bus import SystemBus


def create_system_bus(
    bus_type: Optional[str] = None,
    client_id: Optional[str] = None,
    config: Optional[Dict] = None
) -> SystemBus:
    """
    Создает SystemBus указанного типа для межсистемного взаимодействия.

    Args:
        bus_type: Тип SystemBus ("kafka", "mqtt").
                  Если None, берется из переменной окружения BROKER_TYPE.
        client_id: Идентификатор клиента (для Kafka/MQTT)
        config: Словарь с конфигурацией

    Returns:
        SystemBus: Экземпляр SystemBus указанного типа
    """
    if bus_type is None:
        if config and "broker" in config and "type" in config["broker"]:
            bus_type = config["broker"]["type"]
        else:
            bus_type = os.getenv("BROKER_TYPE") or os.getenv("BROKER_BACKEND", "kafka")

    bus_type = bus_type.lower()

    kafka_config = {}
    mqtt_config = {}

    if config and "broker" in config:
        kafka_config = config["broker"].get("kafka", {})
        mqtt_config = config["broker"].get("mqtt", {})

    if bus_type == "kafka":
        from broker.kafka.kafka_system_bus import KafkaSystemBus

        bootstrap_servers = kafka_config.get(
            "bootstrap_servers",
            os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                os.getenv("KAFKA_SERVERS", "localhost:9092")
            )
        )
        cid = client_id or kafka_config.get(
            "client_id",
            os.getenv("SYSTEM_ID", "system_bus")
        )
        group_id = kafka_config.get(
            "group_id",
            os.getenv("KAFKA_GROUP_ID")
        )
        username = kafka_config.get(
            "username",
            os.getenv("KAFKA_SASL_USERNAME", os.getenv("BROKER_USER"))
        )
        password = kafka_config.get(
            "password",
            os.getenv("KAFKA_SASL_PASSWORD", os.getenv("BROKER_PASSWORD"))
        )
        security_protocol = kafka_config.get(
            "security_protocol",
            os.getenv("KAFKA_SECURITY_PROTOCOL")
        )
        sasl_mechanism = kafka_config.get(
            "sasl_mechanism",
            os.getenv("KAFKA_SASL_MECHANISM")
        )
        return KafkaSystemBus(
            bootstrap_servers=bootstrap_servers,
            client_id=cid,
            group_id=group_id,
            username=username,
            password=password,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
        )

    elif bus_type == "mqtt":
        from broker.mqtt.mqtt_system_bus import MQTTSystemBus

        broker = mqtt_config.get("broker", os.getenv("MQTT_BROKER", "localhost"))
        port = mqtt_config.get("port", int(os.getenv("MQTT_PORT", "1883")))
        cid = client_id or mqtt_config.get(
            "client_id",
            os.getenv("SYSTEM_ID", "system_bus")
        )
        qos = mqtt_config.get("qos", int(os.getenv("MQTT_QOS", "1")))
        return MQTTSystemBus(broker=broker, port=port, client_id=cid, qos=qos)

    else:
        raise ValueError(
            f"Unknown broker type: {bus_type}. Supported types: 'kafka', 'mqtt'"
        )
