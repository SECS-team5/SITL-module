"""
SITL Verifier — компонент валидации команд.

Адаптирован из SITL-module/verifier.py для работы через BaseAsyncComponent.
"""
import asyncio
import os
from typing import Dict, Any, Optional

from sdk.base_async_component import BaseAsyncComponent
from broker.system_bus import SystemBus

from shared.contracts import (
    COMMAND_SCHEMA_NAME,
    HOME_SCHEMA_NAME,
    VERIFIED_COMMAND_TOPIC_DEFAULT,
    VERIFIED_HOME_TOPIC_DEFAULT,
    classify_input_topic,
    parse_json_payload,
    resolve_verified_topic,
    validate_schema,
)
from shared.infopanel_client import create_infopanel_client_from_env


def parse_csv_env(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


class SitlVerifierComponent(BaseAsyncComponent):
    """Компонент для валидации команд перед отправкой."""

    def __init__(
        self,
        component_id: str,
        bus: SystemBus,
        topic: str = "components.sitl_verifier",
    ):
        self._infopanel = create_infopanel_client_from_env()
        self._commands_topic = os.getenv("COMMAND_TOPIC", "sitl.commands")
        self._home_topic = os.getenv("HOME_TOPIC", "sitl-drone-home")
        self._verified_commands_topic = os.getenv(
            "VERIFIED_COMMAND_TOPIC", VERIFIED_COMMAND_TOPIC_DEFAULT
        )
        self._verified_home_topic = os.getenv(
            "VERIFIED_HOME_TOPIC", VERIFIED_HOME_TOPIC_DEFAULT
        )
        self._input_topics = parse_csv_env(
            "INPUT_TOPICS", f"{self._commands_topic},{self._home_topic}"
        )

        super().__init__(
            component_id=component_id,
            component_type="sitl_verifier",
            topic=topic,
            bus=bus,
        )

    def _register_handlers(self):
        # Обработчик для входящих сырых команд (через SystemBus)
        self.register_handler("raw_command", self._handle_raw_command)
        self.register_handler("raw_home", self._handle_raw_home)

    async def _handle_raw_command(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Валидация команды и публикация верифицированной."""
        payload = message.get("payload", message)
        ok, message_type, data, reason = self._process_input_message(
            self._commands_topic, payload
        )
        if not ok or message_type is None or data is None:
            self._infopanel.log_event(
                f"Rejected command: {reason}", "warning"
            )
            return {"status": "rejected", "reason": reason}

        output_topic = resolve_verified_topic(
            message_type, self._verified_commands_topic, self._verified_home_topic
        )
        # Публикуем через SystemBus
        verified_message = {
            "action": "verified_message",
            "payload": data,
            "output_topic": output_topic,
            "message_type": message_type,
        }
        self.bus.publish(output_topic, verified_message)
        self._infopanel.log_event(
            f"Verified command drone_id={data.get('drone_id')} output={output_topic}",
            "info",
        )
        return {"status": "verified", "output_topic": output_topic}

    async def _handle_raw_home(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Валидация HOME и публикация верифицированного."""
        payload = message.get("payload", message)
        ok, message_type, data, reason = self._process_input_message(
            self._home_topic, payload
        )
        if not ok or message_type is None or data is None:
            self._infopanel.log_event(
                f"Rejected home: {reason}", "warning"
            )
            return {"status": "rejected", "reason": reason}

        output_topic = resolve_verified_topic(
            message_type, self._verified_commands_topic, self._verified_home_topic
        )
        verified_message = {
            "action": "verified_message",
            "payload": data,
            "output_topic": output_topic,
            "message_type": message_type,
        }
        self.bus.publish(output_topic, verified_message)
        self._infopanel.log_event(
            f"Verified home drone_id={data.get('drone_id')} output={output_topic}",
            "info",
        )
        return {"status": "verified", "output_topic": output_topic}

    def _process_input_message(
        self,
        topic: str,
        raw_payload: Any,
    ) -> tuple[bool, Optional[str], Optional[Dict[str, Any]], str]:
        """Обработка и валидация входящего сообщения."""
        payload = parse_json_payload(raw_payload)
        if payload is None:
            return False, None, None, "invalid JSON payload"

        ok, message_type, schema_name = classify_input_topic(
            topic,
            self._commands_topic,
            self._home_topic,
        )
        if not ok:
            return False, None, None, schema_name

        ok, reason = validate_schema(payload, schema_name)
        if not ok:
            return False, None, None, reason

        return True, message_type, payload, ""
