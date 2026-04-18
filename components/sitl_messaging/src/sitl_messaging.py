"""
SITL Messaging — компонент запросов/ответов позиций дронов.

Адаптирован из SITL-module/messaging.py для работы через BaseAsyncComponent.
"""
import asyncio
import os
from typing import Dict, Any, Optional, List

import redis.asyncio as redis

from sdk.base_async_component import BaseAsyncComponent
from sdk.messages import create_response
from broker.system_bus import SystemBus

from shared.contracts import (
    POSITION_REQUEST_TOPIC_DEFAULT,
    POSITION_RESPONSE_TOPIC_DEFAULT,
    POSITION_REQUEST_SCHEMA_NAME,
    POSITION_RESPONSE_SCHEMA_NAME,
    DRONES_LIST_REQUEST_TOPIC_DEFAULT,
    DRONES_LIST_RESPONSE_TOPIC_DEFAULT,
    ALL_POSITIONS_REQUEST_TOPIC_DEFAULT,
    ALL_POSITIONS_RESPONSE_TOPIC_DEFAULT,
    DRONES_LIST_REQUEST_SCHEMA_NAME,
    ALL_POSITIONS_REQUEST_SCHEMA_NAME,
    validate_schema,
)
from shared.state import build_position_response, normalize_state
from shared.infopanel_client import create_infopanel_client_from_env


class SitlMessagingComponent(BaseAsyncComponent):
    """Компонент для обработки запросов позиций дронов."""

    def __init__(
        self,
        component_id: str,
        bus: SystemBus,
        topic: str = POSITION_REQUEST_TOPIC_DEFAULT,
    ):
        self._infopanel = create_infopanel_client_from_env()
        self._redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
        self._response_topic = os.getenv(
            "POSITION_RESPONSE_TOPIC", POSITION_RESPONSE_TOPIC_DEFAULT
        )
        self._drones_list_request_topic = os.getenv(
            "DRONES_LIST_REQUEST_TOPIC", DRONES_LIST_REQUEST_TOPIC_DEFAULT
        )
        self._drones_list_response_topic = os.getenv(
            "DRONES_LIST_RESPONSE_TOPIC", DRONES_LIST_RESPONSE_TOPIC_DEFAULT
        )
        self._all_positions_request_topic = os.getenv(
            "ALL_POSITIONS_REQUEST_TOPIC", ALL_POSITIONS_REQUEST_TOPIC_DEFAULT
        )
        self._all_positions_response_topic = os.getenv(
            "ALL_POSITIONS_RESPONSE_TOPIC", ALL_POSITIONS_RESPONSE_TOPIC_DEFAULT
        )
        self._redis: Optional[redis.Redis] = None
        super().__init__(
            component_id=component_id,
            component_type="sitl_messaging",
            topic=topic,
            bus=bus,
        )

    async def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    def _register_handlers(self):
        self.register_handler("request_position", self._handle_request_position)
        self.register_handler("list_drones", self._handle_list_drones)
        self.register_handler("request_all_positions", self._handle_request_all_positions)

    @staticmethod
    def _get_transport_value(message: Dict[str, Any], payload: Dict[str, Any], field: str) -> Any:
        if field in message:
            return message[field]
        return payload.get(field)

    def start(self):
        """Запускает шину и подписку на топики запросов."""
        self._loop = asyncio.get_event_loop()

        # Подписка на запросы списка дронов
        def _on_drones_list_request(msg):
            fut = asyncio.run_coroutine_threadsafe(self._handle_list_drones(msg), self._loop)
            fut.add_done_callback(lambda f: self._log_callback_error(f, "drones_list"))

        # Подписка на запросы всех позиций
        def _on_all_positions_request(msg):
            fut = asyncio.run_coroutine_threadsafe(self._handle_request_all_positions(msg), self._loop)
            fut.add_done_callback(lambda f: self._log_callback_error(f, "all_positions"))

        self.bus.subscribe(self._drones_list_request_topic, _on_drones_list_request)
        self.bus.subscribe(self._all_positions_request_topic, _on_all_positions_request)

        print(f"[{self.component_id}] Subscribed to:")
        print(f"  - {self._drones_list_request_topic}")
        print(f"  - {self._all_positions_request_topic}")
        print(f"  - {self.topic} (component topic)")

        super().start()

    def _log_callback_error(self, future, topic_name):
        try:
            result = future.result()
            # Меньше логов для успешных операций
            if result and result.get("count", 0) > 0:
                print(f"[{self.component_id}] {topic_name} handled successfully")
        except Exception as e:
            import traceback
            print(f"[{self.component_id}] Error in {topic_name} handler: {e}")
            traceback.print_exc()

    async def _handle_request_position(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Обработка запроса позиции дрона."""
        payload = message.get("payload", message)

        ok, reason = validate_schema(payload, POSITION_REQUEST_SCHEMA_NAME)
        if not ok:
            self._infopanel.log_event(f"Rejected position request: {reason}", "warning")
            return {"error": reason}

        drone_id = payload["drone_id"]
        r = await self._get_redis()
        raw_state = await r.hgetall(f"drone:{drone_id}:state")
        if not raw_state:
            self._infopanel.log_event(f"drone '{drone_id}' state not found", "warning")
            return {"error": f"drone '{drone_id}' state not found"}

        state = normalize_state(raw_state)
        response = build_position_response(state)
        if response is None:
            self._infopanel.log_event(
                f"drone '{drone_id}' state does not contain a valid position", "warning"
            )
            return {"error": "invalid position in state"}

        # Публикуем ответ в response топик через SDK
        response_message = create_response(
            correlation_id=self._get_transport_value(message, payload, "correlation_id"),
            payload=response,
            sender=self.component_id,
            success=True,
        )
        response_message["drone_id"] = drone_id
        response_topic = (
            self._get_transport_value(message, payload, "reply_to")
            or self._response_topic
        )
        self.bus.publish(response_topic, response_message)

        self._infopanel.log_event(
            f"Returned position for drone_id={drone_id}", "info"
        )
        return response

    async def _handle_list_drones(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка запроса списка активных дронов."""
        payload = message.get("payload", message)

        ok, reason = validate_schema(payload, DRONES_LIST_REQUEST_SCHEMA_NAME)
        if not ok:
            self._infopanel.log_event(f"Rejected drones list request: {reason}", "warning")
            return {"error": reason}

        status_filter = payload.get("filter", "all")
        r = await self._get_redis()
        drone_ids = []

        async for state_key in r.scan_iter(match="drone:*:state"):
            # Извлекаем drone_id из ключа "drone:X:state"
            parts = state_key.split(":")
            if len(parts) == 3:
                drone_id = parts[1]

                # Применяем фильтр если нужно
                if status_filter != "all":
                    raw_state = await r.hgetall(state_key)
                    if raw_state:
                        state = normalize_state(raw_state)
                        current_status = state.get("status", "")
                        if status_filter == "moving" and current_status != "MOVING":
                            continue
                        if status_filter == "idle" and current_status != "IDLE":
                            continue

                drone_ids.append(drone_id)

        drone_ids.sort()
        response_payload = {
            "drone_ids": drone_ids,
            "count": len(drone_ids)
        }

        # Публикуем ответ
        response_message = create_response(
            correlation_id=self._get_transport_value(message, payload, "correlation_id"),
            payload=response_payload,
            sender=self.component_id,
            success=True,
        )

        response_topic = (
            self._get_transport_value(message, payload, "reply_to")
            or self._drones_list_response_topic
        )
        self.bus.publish(response_topic, response_message)

        self._infopanel.log_event(
            f"Returned list of {len(drone_ids)} drones (filter={status_filter})", "info"
        )

        return response_payload

    async def _handle_request_all_positions(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка запроса позиций всех дронов."""
        payload = message.get("payload", message)

        ok, reason = validate_schema(payload, ALL_POSITIONS_REQUEST_SCHEMA_NAME)
        if not ok:
            self._infopanel.log_event(f"Rejected all positions request: {reason}", "warning")
            return {"error": reason}

        r = await self._get_redis()
        requested_drone_ids = payload.get("drone_ids", [])

        positions = {}
        errors = {}

        # Если указаны конкретные ID
        if requested_drone_ids:
            for drone_id in requested_drone_ids:
                try:
                    raw_state = await r.hgetall(f"drone:{drone_id}:state")
                    if not raw_state:
                        errors[drone_id] = "state not found"
                        continue

                    state = normalize_state(raw_state)
                    position = build_position_response(state)
                    if position:
                        positions[drone_id] = position
                    else:
                        errors[drone_id] = "invalid position in state"
                except Exception as e:
                    errors[drone_id] = str(e)
        else:
            # Получаем все дроны
            async for state_key in r.scan_iter(match="drone:*:state"):
                try:
                    parts = state_key.split(":")
                    if len(parts) != 3:
                        continue

                    drone_id = parts[1]
                    raw_state = await r.hgetall(state_key)

                    if raw_state:
                        state = normalize_state(raw_state)
                        position = build_position_response(state)
                        if position:
                            positions[drone_id] = position
                        else:
                            errors[drone_id] = "invalid position in state"
                except Exception as e:
                    if 'drone_id' in locals():
                        errors[drone_id] = str(e)

        response_payload = {
            "positions": positions,
            "count": len(positions)
        }

        if errors:
            response_payload["errors"] = errors

        # Публикуем ответ
        response_message = create_response(
            correlation_id=self._get_transport_value(message, payload, "correlation_id"),
            payload=response_payload,
            sender=self.component_id,
            success=True,
        )

        response_topic = (
            self._get_transport_value(message, payload, "reply_to")
            or self._all_positions_response_topic
        )
        self.bus.publish(response_topic, response_message)

        self._infopanel.log_event(
            f"Returned positions for {len(positions)} drones", "info"
        )

        return response_payload