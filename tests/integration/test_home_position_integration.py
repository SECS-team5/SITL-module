"""
Интеграционный тест: HOME-позиция через входной топик Verifier.
Поток: sitl-drone-home → Verifier → sitl.verified-home → Controller → Redis
Запускается как: python tests/integration/test_home_position_integration.py
"""
import asyncio
import os
import pathlib
import sys
from unittest.mock import patch, MagicMock

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis.asyncio as redis
from broker.src.bus_factory import create_system_bus
from shared import state
from shared.contracts import HOME_SCHEMA_NAME

# ── Настройки из .env ──────────────────────────────────────────
os.environ.setdefault("BROKER_TYPE", os.getenv("BROKER_BACKEND", "mqtt"))
os.environ.setdefault("MQTT_BROKER", os.environ.get("MQTT_BROKER", "mosquitto"))
os.environ.setdefault("MQTT_PORT", os.environ.get("MQTT_PORT", "1883"))
os.environ.setdefault("REDIS_URL", os.environ.get("REDIS_URL", "redis://redis:6379"))
os.environ.setdefault("SYSTEM_ID", "integration-test-home")

INPUT_HOME_TOPIC = os.getenv("HOME_TOPIC", "sitl-drone-home")
DRONE_ID = "drone_001"

def _make_bus():
    bus = create_system_bus()
    bus.start()
    return bus

async def _wait_for_home_in_redis(redis_client: redis.Redis, drone_id: str, timeout: float = 10.0) -> dict:
    key = state.get_drone_state_key(drone_id)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        stored = await redis_client.hgetall(key)
        if stored and "home_lat" in stored:
            return state.normalize_state(stored)
        await asyncio.sleep(0.3)
    raise TimeoutError(f"HOME не появился в Redis за {timeout}с")

async def _safe_stop_components(*components):
    """Безопасная остановка: ловит TypeError из BaseAsyncComponent.stop()"""
    for comp in components:
        try:
            res = comp.stop()
            if asyncio.iscoroutine(res):
                await res
        except TypeError:
            pass  # Игнорируем баг SDK: await super().stop() возвращает None

async def _run_test():
    """Основная логика теста — вызывается из asyncio.run()"""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    drone_key = state.get_drone_state_key(DRONE_ID)
    bus = _make_bus()

    # Патчи event loop/infopanel применяются ВНУТРИ теста, чтобы работать без pytest.
    with patch("asyncio.get_event_loop", asyncio.get_running_loop), \
         patch("asyncio.run_coroutine_threadsafe", lambda coro, loop: asyncio.ensure_future(coro, loop=loop)), \
         patch("shared.infopanel_client.create_infopanel_client_from_env", return_value=MagicMock()):

        from components.sitl_verifier.src.sitl_verifier import SitlVerifierComponent
        from components.sitl_controller.src.sitl_controller import SitlControllerComponent

        # 🚀 Явно создаём и запускаем компоненты
        verifier = SitlVerifierComponent("test-verifier", bus)
        controller = SitlControllerComponent("test-controller", bus)
        verifier.start()
        controller.start()
        await asyncio.sleep(0.5)  # Даём время на регистрацию MQTT-подписок

        try:
            home_payload = {
                "drone_id": DRONE_ID,
                "home_lat": 59.9386,
                "home_lon": 30.3141,
                "home_alt": 120.0,
            }

            print(f"Публикую HOME в {INPUT_HOME_TOPIC}...")
            bus.publish(INPUT_HOME_TOPIC, home_payload)

            stored = await _wait_for_home_in_redis(redis_client, DRONE_ID, timeout=10.0)
            assert stored["status"] == "ARMED"
            assert float(stored["home_lat"]) == 59.9386
            print(f"   ✅ HOME сохранён: status={stored['status']}, lat={stored['home_lat']}")
            print("✅ Тест пройден!")
            return 0

        finally:
            await _safe_stop_components(verifier, controller)
            bus.stop()
            try:
                await redis_client.delete(drone_key)
                await redis_client.aclose()
            except Exception:
                pass

if __name__ == "__main__":
    print(f"Тест: HOME-позиция через топики (BROKER_BACKEND={os.getenv('BROKER_BACKEND', 'mqtt')})")
    print("-" * 60)
    try:
        exit_code = asyncio.run(_run_test())
        sys.exit(exit_code or 0)
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
