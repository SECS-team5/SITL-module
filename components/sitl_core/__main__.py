"""
Точка входа для запуска SITL Core.
python -m components.sitl_core
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
    """Запуск SITL Core с брокером."""
    from broker.src.system_bus import SystemBus
    from broker.src.bus_factory import create_system_bus
    from components.sitl_core.src.sitl_core import SitlCoreComponent

    # Маппинг переменных окружения
    backend = os.environ.get("BROKER_BACKEND", "kafka").lower()
    os.environ.setdefault("BROKER_TYPE", backend)

    kafka_servers = os.environ.get("KAFKA_SERVERS", "kafka:29092")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", kafka_servers)

    os.environ.setdefault("SYSTEM_ID", "sitl-core")

    component_id = os.environ.get("COMPONENT_ID", "sitl-core")

    print(f"[{component_id}] Creating SystemBus ({backend})...")
    bus = create_system_bus()

    print(f"[{component_id}] Creating Core component...")
    component = SitlCoreComponent(
        component_id=component_id,
        bus=bus,
        topic="components.sitl_core",
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown(sig, frame):
        print(f"[{component_id}] Shutting down...")
        component.stop()
        loop.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    async def run_all():
        """Запуск брокера, infopanel и фоновой задачи обновления."""
        component.bus.start()
        loop = asyncio.get_event_loop()
        component.bus.subscribe(component.topic, lambda msg: asyncio.run_coroutine_threadsafe(component._handle_message(msg), loop))
        component._running = True
        print(f"[{component_id}] Started. Listening on topic: {component.topic}")

        await component._infopanel.start()
        component._infopanel.log_event(
            f"Position updater started at {component._update_hz:.1f} Hz", "info"
        )
        print(f"[{component_id}] Running...")

        component.add_background_task(component._position_updater_task())

        try:
            while component._running:
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    try:
        loop.run_until_complete(run_all())
    except KeyboardInterrupt:
        pass
    finally:
        component.stop()
        loop.run_until_complete(component._infopanel.stop())
        loop.close()
        print(f"[{component_id}] Stopped")


if __name__ == "__main__":
    main()
