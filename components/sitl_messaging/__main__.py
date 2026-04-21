"""
Точка входа для запуска SITL Messaging.
python -m components.sitl_messaging
"""
import asyncio
import os
import signal
import sys

# Добавляем корень проекта в PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def main():
    """Запуск SITL Messaging с брокером."""
    from broker.src.system_bus import SystemBus
    from broker.src.bus_factory import create_system_bus
    from components.sitl_messaging.src.sitl_messaging import SitlMessagingComponent
    from shared.contracts import POSITION_REQUEST_TOPIC_DEFAULT

    # Маппинг переменных окружения
    backend = os.environ.get("BROKER_BACKEND", "kafka").lower()
    os.environ.setdefault("BROKER_TYPE", backend)
    
    kafka_servers = os.environ.get("KAFKA_SERVERS", "kafka:29092")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", kafka_servers)
    
    os.environ.setdefault("SYSTEM_ID", "sitl-messaging")

    component_id = os.environ.get("COMPONENT_ID", "sitl-messaging")

    print(f"[{component_id}] Creating SystemBus ({backend})...")
    bus = create_system_bus()

    print(f"[{component_id}] Creating Messaging component...")
    component = SitlMessagingComponent(
        component_id=component_id,
        bus=bus,
        topic=os.getenv("POSITION_REQUEST_TOPIC", POSITION_REQUEST_TOPIC_DEFAULT),
    )

    # Запускаем infopanel
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown(sig, frame):
        print(f"[{component_id}] Shutting down...")
        component.stop()
        loop.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"[{component_id}] Starting component...")
    component.start()

    loop.run_until_complete(component._infopanel.start())
    print(f"[{component_id}] Running...")

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        component.stop()
        loop.run_until_complete(component._infopanel.stop())
        loop.close()
        print(f"[{component_id}] Stopped")


if __name__ == "__main__":
    main()
