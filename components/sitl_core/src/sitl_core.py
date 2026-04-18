"""
SITL Core — компонент обновления позиций дронов.

Адаптирован из SITL-module/core.py для работы через BaseAsyncComponent.
"""
import asyncio
import os
from typing import Dict, Any, Optional

import redis.asyncio as redis

from sdk.base_async_component import BaseAsyncComponent
from broker.system_bus import SystemBus

from shared.infopanel_client import create_infopanel_client_from_env
from shared.state import advance_drone_state, normalize_state, serialize_state


class SitlCoreComponent(BaseAsyncComponent):
    """Компонент для обновления позиций дронов в Redis."""

    def __init__(
        self,
        component_id: str,
        bus: SystemBus,
        topic: str = "components.sitl_core",
    ):
        self._infopanel = create_infopanel_client_from_env()
        self._redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
        self._update_hz = float(os.getenv("UPDATE_FREQUENCY_HZ", "10.0"))
        self._state_ttl_sec = int(os.getenv("STATE_TTL_SEC", "7200"))
        self._redis: Optional[redis.Redis] = None
        self._update_counter = {}  # Счетчик обновлений для каждого дрона
        self._log_every_n_updates = int(os.getenv("LOG_POSITION_EVERY_N", "10"))  # Логировать каждые N обновлений
        super().__init__(
            component_id=component_id,
            component_type="sitl_core",
            topic=topic,
            bus=bus,
        )

    async def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    def _register_handlers(self):
        self.register_handler("get_config", self._handle_get_config)

    async def _handle_get_config(self, message) -> dict:
        return {
            "redis_url": self._redis_url,
            "update_hz": self._update_hz,
            "state_ttl_sec": self._state_ttl_sec,
        }

    def start(self):
        """Запускает компонент и фоновую задачу обновления позиций."""
        super().start()
        # Запускаем фоновую задачу
        self.add_background_task(self._position_updater_task())
        self._infopanel.log_event(
            f"Position updater started at {self._update_hz:.1f} Hz", "info"
        )

    async def _position_updater_task(self):
        """Фоновая задача обновления позиций дронов."""
        update_interval_sec = 1.0 / self._update_hz
        while self._running:
            try:
                r = await self._get_redis()
                async for state_key in r.scan_iter(match="drone:*:state"):
                    await self._update_drone_position(r, state_key, update_interval_sec)
                await asyncio.sleep(update_interval_sec)
            except Exception as exc:
                self._infopanel.log_event(f"Position updater failed: {exc}", "error")
                await asyncio.sleep(update_interval_sec)

    def _print_position_update(self, drone_id: str, state: Dict[str, Any]):
        """Вывод обновления позиции дрона в консоль."""
        print(f"\n[POSITION UPDATE] Дрон: {drone_id} | Статус: {state.get('status', 'N/A')} | "
              f"Позиция: ({state.get('x', 'N/A'):.2f}, {state.get('y', 'N/A'):.2f}, {state.get('z', 'N/A'):.2f}) | "
              f"Скорость: ({state.get('vx', 'N/A'):.2f}, {state.get('vy', 'N/A'):.2f}, {state.get('vz', 'N/A'):.2f})")

    async def _update_drone_position(
        self, r: redis.Redis, state_key: str, update_interval_sec: float
    ) -> bool:
        """Обновляет позицию одного дрона."""
        raw_state = await r.hgetall(state_key)
        if not raw_state:
            return False

        state = normalize_state(raw_state)
        if state.get("status") != "MOVING":
            return False

        next_state = advance_drone_state(state, update_interval_sec)
        await r.hset(state_key, mapping=serialize_state(next_state))
        if self._state_ttl_sec > 0:
            await r.expire(state_key, self._state_ttl_sec)

        # Периодический вывод обновлений позиции
        drone_id = state_key.split(":")[1]  # Извлекаем drone_id из ключа "drone:X:state"
        self._update_counter[drone_id] = self._update_counter.get(drone_id, 0) + 1

        if self._update_counter[drone_id] % self._log_every_n_updates == 0:
            self._print_position_update(drone_id, next_state)

        return True