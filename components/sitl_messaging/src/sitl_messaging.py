"""
SITL Messaging — компонент запросов/ответов позиций дронов.

Адаптирован из SITL-module/messaging.py для работы через BaseAsyncComponent.
"""
import asyncio
import os
from typing import Dict, Any, Optional

import redis.asyncio as redis

from sdk.base_async_component import BaseAsyncComponent
from sdk.messages import create_response
from broker.system_bus import SystemBus

from shared.contracts import (
    POSITION_REQUEST_TOPIC_DEFAULT,
    POSITION_RESPONSE_TOPIC_DEFAULT,
    POSITION_REQUEST_SCHEMA_NAME,
    POSITION_RESPONSE_SCHEMA_NAME,
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

    def start(self):
        """Запускает шину и подписку на топик запросов."""
        # self.topic уже установлен в POSITION_REQUEST_TOPIC через __init__
        # super().start() подпишет на него же — не нужно дублировать
        super().start()

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

        response = build_position_response(normalize_state(raw_state))
        if response is None:
            self._infopanel.log_event(
                f"drone '{drone_id}' state does not contain a valid position", "warning"
            )
            return {"error": "invalid position in state"}

        # Публикуем ответ в response топик через SDK
        response_message = create_response(
            correlation_id=message.get("correlation_id"),
            payload=response,
            sender=self.component_id,
            success=True,
        )
        response_message["drone_id"] = drone_id
        self.bus.publish(self._response_topic, response_message)

        self._infopanel.log_event(
            f"Returned position for drone_id={drone_id}", "info"
        )
        return response
