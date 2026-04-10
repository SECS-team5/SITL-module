"""
Broker module — единая шина SystemBus для систем и компонентов.

Структура:
- broker/src/       - SystemBus (абстракция), bus_factory
- broker/kafka/     - KafkaSystemBus
- broker/mqtt/      - MQTTSystemBus

Используй create_system_bus() из broker.bus_factory для создания шины.
"""
