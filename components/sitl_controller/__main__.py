"""
Точка входа для запуска SITL Controller.
python -m components.sitl_controller
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
    """Запуск SITL Controller с брокером."""
    from broker.src.system_bus import SystemBus
    from broker.src.bus_factory import create_system_bus
    from components.sitl_controller.src.sitl_controller import SitlControllerComponent

    # Маппинг переменных окружения
    backend = os.environ.get("BROKER_BACKEND", "kafka").lower()
    os.environ.setdefault("BROKER_TYPE", backend)
    
    kafka_servers = os.environ.get("KAFKA_SERVERS", "kafka:29092")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", kafka_servers)
    
    os.environ.setdefault("SYSTEM_ID", "sitl-controller")

    component_id = os.environ.get("COMPONENT_ID", "sitl-controller")

    print(f"[{component_id}] Creating SystemBus ({backend})...")
    bus = create_system_bus()

    print(f"[{component_id}] Creating Controller component...")
    component = SitlControllerComponent(
        component_id=component_id,
        bus=bus,
        topic="components.sitl_controller",
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
