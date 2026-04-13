"""
SITL Controller — компонент обработки верифицированных команд.

Адаптирован из SITL-module/controller.py для работы через BaseAsyncComponent.
"""
import asyncio
import os
from typing import Dict, Any, Optional

import redis.asyncio as redis

from sdk.base_async_component import BaseAsyncComponent
from broker.system_bus import SystemBus

from shared.contracts import (
    VERIFIED_COMMAND_TOPIC_DEFAULT,
    VERIFIED_HOME_TOPIC_DEFAULT,
    classify_input_topic,
    parse_json_payload,
    validate_schema,
)
from shared.infopanel_client import create_infopanel_client_from_env
from shared.state import (
    apply_command_update,
    build_home_state,
    get_drone_state_key,
    normalize_state,
    serialize_state,
    state_has_home,
)


class SitlControllerComponent(BaseAsyncComponent):
    """Компонент для обработки верифицированных команд дронов."""

    def __init__(
        self,
        component_id: str,
        bus: SystemBus,
        topic: str = "components.sitl_controller",
    ):
        self._infopanel = create_infopanel_client_from_env()
        self._redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
        self._state_ttl_sec = int(os.getenv("STATE_TTL_SEC", "7200"))
        self._verified_commands_topic = os.getenv(
            "VERIFIED_COMMAND_TOPIC", VERIFIED_COMMAND_TOPIC_DEFAULT
        )
        self._verified_home_topic = os.getenv(
            "VERIFIED_HOME_TOPIC", VERIFIED_HOME_TOPIC_DEFAULT
        )
        self._redis: Optional[redis.Redis] = None
        super().__init__(
            component_id=component_id,
            component_type="sitl_controller",
            topic=topic,
            bus=bus,
        )

    async def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    def _register_handlers(self):
        self.register_handler("verified_message", self._handle_verified_message)

    def start(self):
        """Подписывается на verified-топики + свой компонентный топик."""
        self._loop = asyncio.get_event_loop()
        # Подписка на верифицированные топики (от Verifier)
        def _on_verified_commands(msg):
            fut = asyncio.run_coroutine_threadsafe(self._handle_verified_message(msg), self._loop)
            fut.add_done_callback(lambda f: self._log_callback_error(f, "commands"))
        def _on_verified_home(msg):
            fut = asyncio.run_coroutine_threadsafe(self._handle_verified_message(msg), self._loop)
            fut.add_done_callback(lambda f: self._log_callback_error(f, "home"))
        self.bus.subscribe(self._verified_commands_topic, _on_verified_commands)
        self.bus.subscribe(self._verified_home_topic, _on_verified_home)
        print(f"[{self.component_id}] Subscribed to verified topics: {self._verified_commands_topic}, {self._verified_home_topic}")
        # Подписка на компонентный топик (для тестов и прямых вызовов)
        super().start()

    def _log_callback_error(self, future, topic_name):
        try:
            result = future.result()
            print(f"[{self.component_id}] {topic_name} handled: {result}")
        except Exception as e:
            import traceback
            print(f"[{self.component_id}] Error in {topic_name} handler: {e}")
            traceback.print_exc()

    async def _handle_verified_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Обработка верифицированного сообщения."""
        payload = message.get("payload", message)
        message_type = message.get("message_type")

        if message_type is None:
            # Определяем по топику
            source_topic = message.get("source_topic", self._verified_commands_topic)
            ok, message_type, schema_name = classify_input_topic(
                source_topic,
                self._verified_commands_topic,
                self._verified_home_topic,
            )
            if not ok:
                self._infopanel.log_event(
                    f"Cannot classify verified message from topic={source_topic}: {schema_name}",
                    "warning",
                )
                return {"status": "rejected", "reason": schema_name}

        # Определяем схему
        if message_type == "HOME":
            schema_name = "sitl-drone-home.json"
        elif message_type == "COMMAND":
            schema_name = "sitl-commands.json"
        else:
            self._infopanel.log_event(f"Unknown message_type: {message_type}", "warning")
            return {"status": "rejected", "reason": f"unknown message_type: {message_type}"}

        ok, reason = validate_schema(payload, schema_name)
        if not ok:
            self._infopanel.log_event(
                f"Schema validation failed: {reason}", "warning"
            )
            return {"status": "rejected", "reason": reason}

        drone_id = payload["drone_id"]
        r = await self._get_redis()
        existing_state_raw = await r.hgetall(get_drone_state_key(drone_id))
        existing_state = normalize_state(existing_state_raw) if existing_state_raw else {}

        if message_type == "HOME":
            next_state = build_home_state(payload, existing_state or None)
            await self._persist_state(r, drone_id, next_state)
            self._infopanel.log_event(
                f"Stored HOME for drone_id={drone_id} status={next_state['status']}",
                "info",
            )
            return {"status": "home_stored", "drone_id": drone_id}

        if not existing_state or not state_has_home(existing_state):
            self._infopanel.log_event(
                f"Ignored COMMAND for drone_id={drone_id}: HOME state is missing",
                "warning",
            )
            return {"status": "ignored", "reason": "HOME state missing"}

        next_state = apply_command_update(existing_state, payload)
        await self._persist_state(r, drone_id, next_state)
        self._infopanel.log_event(
            f"Applied COMMAND for drone_id={drone_id} status={next_state['status']} "
            f"vx={next_state['vx']} vy={next_state['vy']} vz={next_state['vz']}",
            "info",
        )
        return {
            "status": "command_applied",
            "drone_id": drone_id,
            "new_status": next_state["status"],
        }

    async def _persist_state(self, r: redis.Redis, drone_id: str, state: Dict[str, Any]):
        state_key = get_drone_state_key(drone_id)
        await r.hset(state_key, mapping=serialize_state(state))
        if self._state_ttl_sec > 0:
            await r.expire(state_key, self._state_ttl_sec)
