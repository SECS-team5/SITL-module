"""
Factory для создания SystemBus на основе конфигурации.
Поддерживаемые типы: kafka, mqtt.
"""
import os
from typing import Dict, Optional

from .system_bus import SystemBus
from broker.kafka.kafka_system_bus import KafkaSystemBus
from broker.mqtt.mqtt_system_bus import MQTTSystemBus


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
            bus_type = os.getenv("BROKER_TYPE", "kafka")

    bus_type = bus_type.lower()

    kafka_config = {}
    mqtt_config = {}

    if config and "broker" in config:
        kafka_config = config["broker"].get("kafka", {})
        mqtt_config = config["broker"].get("mqtt", {})

    if bus_type == "kafka":
        bootstrap_servers = kafka_config.get(
            "bootstrap_servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        )
        cid = client_id or kafka_config.get(
            "client_id",
            os.getenv("SYSTEM_ID", "system_bus")
        )
        group_id = kafka_config.get(
            "group_id",
            os.getenv("KAFKA_GROUP_ID")
        )
        return KafkaSystemBus(
            bootstrap_servers=bootstrap_servers,
            client_id=cid,
            group_id=group_id
        )

    elif bus_type == "mqtt":
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
