"""
SITL Verifier — компонент валидации команд.

"""
import asyncio
import os
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

from sdk.base_async_component import BaseAsyncComponent
from sdk.messages import Message
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
    """Парсинг CSV-переменных окружения."""
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass
class ValidationResult:
    """Результат валидации сообщения (для Chain of Responsibility)."""
    success: bool
    message_type: Optional[str]
    validated_payload: Optional[Dict[str, Any]]
    reason: str
    verification_metadata: Dict[str, Any] = field(default_factory=dict)


class ValidationChain:
    """
    Chain of Responsibility для валидации входящих сообщений.

    Этапы цепочки:
    1. JSON Parse — валидация структуры JSON
    2. Topic Classification — определение типа сообщения
    3. Schema Validation — проверка по JSON Schema
    4. Final Verification — финальная проверка и маркировка
    """

    def __init__(
        self,
        commands_topic: str,
        home_topic: str,
        infopanel_client,
    ):
        self._commands_topic = commands_topic
        self._home_topic = home_topic
        self._infopanel = infopanel_client

    def validate(
        self,
        topic: str,
        raw_payload: Any,
    ) -> ValidationResult:
        """
        Выполнение полной цепочки валидации.

        Trust Boundary: UNTRUSTED → VERIFIED
        """
        # Step 1: JSON Parse Handler
        payload = self._handle_json_parse(raw_payload)
        if payload is None:
            return ValidationResult(
                success=False,
                message_type=None,
                validated_payload=None,
                reason="invalid JSON payload"
            )

        # Step 2: Topic Classification Handler
        ok, message_type, schema_name = self._handle_topic_classification(topic)
        if not ok:
            return ValidationResult(
                success=False,
                message_type=None,
                validated_payload=None,
                reason=schema_name  # В случае ошибки schema_name содержит reason
            )

        # Step 3: Schema Validation Handler
        ok, reason = self._handle_schema_validation(payload, schema_name)
        if not ok:
            return ValidationResult(
                success=False,
                message_type=message_type,
                validated_payload=None,
                reason=reason
            )

        # Step 4: Final Verification Handler
        verification_metadata = self._handle_final_verification(payload, message_type)

        return ValidationResult(
            success=True,
            message_type=message_type,
            validated_payload=dict(payload),
            reason="",
            verification_metadata=verification_metadata,
        )

    def _handle_json_parse(self, raw_payload: Any) -> Optional[Dict[str, Any]]:
        """
        Handler 1: Парсинг и валидация JSON структуры.

        ЦПБ-1 Decision Point: Пропустить или отклонить по структуре
        """
        payload = parse_json_payload(raw_payload)
        if payload is None:
            self._infopanel.log_event(
                "JSON parse failed in validation chain",
                severity="warning",
                event_type="validation_error"
            )
        return payload

    def _handle_topic_classification(
        self,
        topic: str
    ) -> Tuple[bool, Optional[str], str]:
        """
        Handler 2: Классификация по типу топика.

        ЦПБ-1 Decision Point: Определить тип сообщения (COMMAND vs HOME)
        """
        ok, message_type, schema_name = classify_input_topic(
            topic,
            self._commands_topic,
            self._home_topic,
        )
        if not ok:
            self._infopanel.log_event(
                f"Topic classification failed: topic={topic}",
                severity="warning",
                event_type="validation_error"
            )
        return ok, message_type, schema_name

    def _handle_schema_validation(
        self,
        payload: Dict[str, Any],
        schema_name: str
    ) -> Tuple[bool, str]:
        """
        Handler 3: Валидация по JSON Schema.

        ЦПБ-1 Decision Point: Проверка структуры сообщения
        """
        ok, reason = validate_schema(payload, schema_name)
        if not ok:
            self._infopanel.log_event(
                f"Schema validation failed: schema={schema_name}, reason={reason}",
                severity="warning",
                event_type="validation_error"
            )
        return ok, reason

    def _handle_final_verification(
        self,
        payload: Dict[str, Any],
        message_type: str
    ) -> Dict[str, Any]:
        """
        Handler 4: Финальная верификация и маркировка доверенным статусом.

        ЦПБ-1 Decision Point: Добавление метаданных верификации
        """
        from datetime import datetime, timezone

        self._infopanel.log_event(
            f"Message verified: type={message_type}, drone_id={payload.get('drone_id')}",
            severity="info",
            event_type="verification_success"
        )

        return {
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "verified_by": "sitl_verifier",
        }


class SitlVerifierComponent(BaseAsyncComponent):
    """
    Компонент валидации команд (ЦПБ-1).

    Реализует шаблон Chain of Responsibility для защиты периметра системы.
    Все входящие сообщения проходят через четырехуровневую валидацию.

    Trust Level: HIGH
    Attack Surface: Максимальная (принимает UNTRUSTED данные)
    """

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

        # Инициализация Chain of Responsibility
        self._validation_chain = ValidationChain(
            commands_topic=self._commands_topic,
            home_topic=self._home_topic,
            infopanel_client=self._infopanel,
        )

        super().__init__(
            component_id=component_id,
            component_type="sitl_verifier",
            topic=topic,
            bus=bus,
        )

    def _register_handlers(self):
        """Регистрация обработчиков для входящих сообщений."""
        self.register_handler("raw_command", self._handle_raw_command)
        self.register_handler("raw_home", self._handle_raw_home)

    def start(self):
        """
        Запуск компонента и подписка на input-топики.

        Подписывается на:
        - sitl.commands (сырые команды)
        - sitl-drone-home (сырые home-позиции)
        """
        self._loop = asyncio.get_event_loop()

        # Подписка на сырые топики (UNTRUSTED источники)
        for topic in self._input_topics:
            if topic == self._commands_topic:
                def _on_command(msg):
                    fut = asyncio.run_coroutine_threadsafe(
                        self._handle_raw_command(msg),
                        self._loop
                    )
                    fut.add_done_callback(
                        lambda f: self._log_callback_error(f, "command")
                    )
                ok = self.bus.subscribe(topic, _on_command)
                self._log_subscription(topic, ok)

            elif topic == self._home_topic:
                def _on_home(msg):
                    fut = asyncio.run_coroutine_threadsafe(
                        self._handle_raw_home(msg),
                        self._loop
                    )
                    fut.add_done_callback(
                        lambda f: self._log_callback_error(f, "home")
                    )
                ok = self.bus.subscribe(topic, _on_home)
                self._log_subscription(topic, ok)

        print(f"[{self.component_id}] Input topics: {self._input_topics}")

        # Подписка на компонентный топик
        super().start()

    def _log_subscription(self, topic: str, success: bool):
        """Логирование результата подписки на топик."""
        status = "SUCCESS" if success else "FAILED"
        severity = "info" if success else "error"

        print(f"[{self.component_id}] Subscribe to {topic}: {success}")
        self._infopanel.log_event(
            f"Subscription to {topic}: {status}",
            severity=severity,
            event_type="subscription"
        )

    def _log_callback_error(self, future, topic_name: str):
        """Обработка ошибок в асинхронных callback'ах."""
        try:
            result = future.result()
            print(f"[{self.component_id}] {topic_name} handled: {result}")
        except Exception as e:
            print(f"[{self.component_id}] Error in {topic_name} handler: {e}")
            self._infopanel.log_event(
                f"Handler error in {topic_name}: {str(e)}",
                severity="error",
                event_type="handler_error"
            )

    async def _handle_raw_command(
        self,
        message: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Валидация команды и публикация верифицированной.

        Trust Boundary Crossing: UNTRUSTED → VERIFIED
        Output Topic: sitl.verified-commands
        """
        payload = message.get("payload", message)

        # Прогон через Chain of Responsibility
        result = self._validation_chain.validate(self._commands_topic, payload)

        if not result.success:
            self._infopanel.log_event(
                f"Rejected command: {result.reason}",
                severity="warning",
                event_type="command_rejected"
            )
            return {"status": "rejected", "reason": result.reason}

        # Определение output топика
        output_topic = resolve_verified_topic(
            result.message_type,
            self._verified_commands_topic,
            self._verified_home_topic
        )

        # Публикация верифицированного сообщения (TRUSTED)
        verified_message = Message(
            action="verified_message",
            payload=result.validated_payload,
            sender=self.component_id,
        ).to_dict()
        verified_message["message_type"] = result.message_type
        verified_message.update(result.verification_metadata)

        self.bus.publish(output_topic, verified_message)

        self._infopanel.log_event(
            f"Verified command drone_id={result.validated_payload.get('drone_id')} "
            f"output={output_topic}",
            severity="info",
            event_type="command_verified"
        )

        return {"status": "verified", "output_topic": output_topic}

    async def _handle_raw_home(
        self,
        message: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Валидация HOME и публикация верифицированного.

        Trust Boundary Crossing: UNTRUSTED → VERIFIED
        Output Topic: sitl.verified-home
        """
        payload = message.get("payload", message)

        # Прогон через Chain of Responsibility
        result = self._validation_chain.validate(self._home_topic, payload)

        if not result.success:
            self._infopanel.log_event(
                f"Rejected home: {result.reason}",
                severity="warning",
                event_type="home_rejected"
            )
            return {"status": "rejected", "reason": result.reason}

        # Определение output топика
        output_topic = resolve_verified_topic(
            result.message_type,
            self._verified_commands_topic,
            self._verified_home_topic
        )

        # Публикация верифицированного сообщения (TRUSTED)
        verified_message = Message(
            action="verified_message",
            payload=result.validated_payload,
            sender=self.component_id,
        ).to_dict()
        verified_message["message_type"] = result.message_type
        verified_message.update(result.verification_metadata)

        self.bus.publish(output_topic, verified_message)

        self._infopanel.log_event(
            f"Verified home drone_id={result.validated_payload.get('drone_id')} "
            f"output={output_topic}",
            severity="info",
            event_type="home_verified"
        )

        return {"status": "verified", "output_topic": output_topic}
