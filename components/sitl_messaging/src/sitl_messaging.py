"""
SITL Messaging — компонент запросов/ответов позиций дронов.
"""
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

import redis.asyncio as redis
from broker.system_bus import SystemBus
from sdk.base_async_component import BaseAsyncComponent
from sdk.messages import create_response
from shared.contracts import (
    POSITION_REQUEST_TOPIC_DEFAULT,
    POSITION_RESPONSE_TOPIC_DEFAULT,
    POSITION_REQUEST_SCHEMA_NAME,
    POSITION_RESPONSE_SCHEMA_NAME,
    validate_schema,
)
from shared.infopanel_client import create_infopanel_client_from_env
from shared.state import build_position_response, normalize_state


@dataclass
class TelemetryRequest:
    """Верифицированный запрос телеметрии."""
    drone_id: str
    correlation_id: Optional[str]
    reply_to: Optional[str]
    raw_message: Dict[str, Any]
    timestamp: str


@dataclass
class TelemetryResponse:
    """Результат обработки запроса телеметрии."""
    success: bool
    drone_id: str
    position: Optional[Dict[str, Any]]
    error: Optional[str]
    reply_topic: str


class TelemetryProxy:
    """
    Proxy для контроля доступа к телеметрии дронов (Шаблон Proxy).

    ЦПБ-4: Центр безопасной телеметрии
    ЦПБ-5: Центр изоляции сообщений

    Функции:
    1. Валидация запроса по схеме
    2. Проверка полномочий (наличие drone_id)
    3. Tracing через correlation_id
    4. Контроль reply_to для предотвращения утечки данных
    5. Read-only доступ к Redis
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        response_topic: str,
        infopanel_client,
        component_id: str,
    ):
        self._redis = redis_client
        self._response_topic = response_topic
        self._infopanel = infopanel_client
        self._component_id = component_id

    async def process_request(
        self,
        message: Dict[str, Any]
    ) -> TelemetryResponse:
        """
        Обработка запроса телеметрии через Proxy.

        Trust Boundary: REQUEST → VALIDATED → AUTHORIZED → RESPONSE
        """
        # Step 1: Validate Request Structure
        telemetry_request = self._validate_request_structure(message)
        if telemetry_request is None:
            return TelemetryResponse(
                success=False,
                drone_id="unknown",
                position=None,
                error="invalid request structure",
                reply_topic=self._response_topic
            )

        # Step 2: Check Authorization (drone exists)
        authorized, state = await self._check_authorization(telemetry_request.drone_id)
        if not authorized:
            return TelemetryResponse(
                success=False,
                drone_id=telemetry_request.drone_id,
                position=None,
                error=f"drone '{telemetry_request.drone_id}' not found",
                reply_topic=self._get_reply_topic(telemetry_request)
            )

        # Step 3: Trace Request for audit
        self._trace_request(telemetry_request)

        # Step 4: Build Position Response (READ-ONLY)
        position = self._build_position_response(state, telemetry_request.drone_id)
        if position is None:
            return TelemetryResponse(
                success=False,
                drone_id=telemetry_request.drone_id,
                position=None,
                error="invalid position in state",
                reply_topic=self._get_reply_topic(telemetry_request)
            )

        # Step 5: Log telemetry for monitoring
        self._log_telemetry_access(telemetry_request.drone_id, state, position)

        return TelemetryResponse(
            success=True,
            drone_id=telemetry_request.drone_id,
            position=position,
            error=None,
            reply_topic=self._get_reply_topic(telemetry_request)
        )

    def _validate_request_structure(
        self,
        message: Dict[str, Any]
    ) -> Optional[TelemetryRequest]:
        """
        Proxy Function 1: Валидация структуры запроса.

        ЦПБ-4 Decision Point: Пропустить или отклонить запрос по схеме
        """
        payload = message.get("payload", message)

        ok, reason = validate_schema(payload, POSITION_REQUEST_SCHEMA_NAME)
        if not ok:
            self._infopanel.log_event(
                f"Rejected position request: {reason}",
                severity="warning",
                event_type="telemetry_request_rejected"
            )
            return None

        # Извлечение метаданных запроса
        drone_id = payload.get("drone_id")
        if not drone_id:
            self._infopanel.log_event(
                "Missing drone_id in position request",
                severity="warning",
                event_type="telemetry_request_rejected"
            )
            return None

        return TelemetryRequest(
            drone_id=drone_id,
            correlation_id=self._get_transport_value(message, payload, "correlation_id"),
            reply_to=self._get_transport_value(message, payload, "reply_to"),
            raw_message=message,
            timestamp=datetime.now(timezone.utc).isoformat()
        )

    async def _check_authorization(
        self,
        drone_id: str
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Proxy Function 2: Проверка полномочий доступа к телеметрии.

        ЦПБ-4 Decision Point: Авторизовать или отклонить доступ к данным дрона

        Политика безопасности:
        - READ-ONLY операция
        - Доступ только к существующим дронам
        - Нет проверки ownership (упрощенная модель для SITL)
        """
        raw_state = await self._redis.hgetall(f"drone:{drone_id}:state")

        if not raw_state:
            self._infopanel.log_event(
                f"Unauthorized access attempt: drone '{drone_id}' not found",
                severity="warning",
                event_type="telemetry_unauthorized"
            )
            return False, None

        state = normalize_state(raw_state)

        self._infopanel.log_event(
            f"Authorized telemetry access for drone_id={drone_id}",
            severity="info",
            event_type="telemetry_authorized"
        )

        return True, state

    def _trace_request(self, request: TelemetryRequest):
        """
        Proxy Function 3: Трассировка запроса для аудита.

        ЦПБ-5 Decision Point: Сохранение correlation_id для traceability
        """
        self._infopanel.log_event(
            f"Telemetry request traced: drone_id={request.drone_id}, "
            f"correlation_id={request.correlation_id}, "
            f"timestamp={request.timestamp}",
            severity="info",
            event_type="telemetry_traced"
        )

    def _build_position_response(
        self,
        state: Dict[str, Any],
        drone_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Proxy Function 4: Построение ответа с позицией (READ-ONLY).

        ЦПБ-4 Decision Point: Формирование безопасного ответа

        Политика безопасности:
        - Только чтение координат (x, y, z)
        - Нет доступа к внутреннему state
        - Валидация корректности позиции
        """
        position = build_position_response(state)

        if position is None:
            self._infopanel.log_event(
                f"Invalid position for drone '{drone_id}' in state",
                severity="warning",
                event_type="telemetry_invalid_position"
            )
            return None

        return position

    def _log_telemetry_access(
        self,
        drone_id: str,
        state: Dict[str, Any],
        position: Dict[str, Any]
    ):
        """
        Вывод информации о доступе к телеметрии (для мониторинга).

        ЦПБ-5: Изоляция — логирование доступа к чувствительным данным
        """
        print("\n" + "="*80)
        print(f"[TELEMETRY ACCESS] Запрос позиции дрона")
        print(f"[TELEMETRY ACCESS] Дрон ID: {drone_id}")
        print(f"[TELEMETRY ACCESS] Статус: {state.get('status', 'N/A')}")
        print("-"*80)
        print(f"[TELEMETRY ACCESS] Текущие координаты:")
        print(f"  X: {position.get('x', 'N/A')}")
        print(f"  Y: {position.get('y', 'N/A')}")
        print(f"  Z: {position.get('z', 'N/A')}")
        print("-"*80)
        print(f"[TELEMETRY ACCESS] Вектор направления (скорость):")
        print(f"  VX: {state.get('vx', 'N/A')}")
        print(f"  VY: {state.get('vy', 'N/A')}")
        print(f"  VZ: {state.get('vz', 'N/A')}")
        print("-"*80)
        if state.get('home_x') is not None:
            print(f"[TELEMETRY ACCESS] Домашняя локация:")
            print(f"  Home X: {state.get('home_x', 'N/A')}")
            print(f"  Home Y: {state.get('home_y', 'N/A')}")
            print(f"  Home Z: {state.get('home_z', 'N/A')}")
        else:
            print(f"[TELEMETRY ACCESS] Домашняя локация: НЕ УСТАНОВЛЕНА")
        print("="*80 + "\n")

    def _get_reply_topic(self, request: TelemetryRequest) -> str:
        """
        Proxy Function 5: Контроль назначения ответа.

        ЦПБ-5 Decision Point: Защита от утечки данных через некорректный reply_to

        Политика безопасности:
        - Приоритет reply_to из запроса (если указан)
        - Fallback на default response topic
        - TODO: Добавить whitelist разрешенных reply_to топиков
        """
        if request.reply_to:
            # В production здесь должна быть проверка whitelist
            self._infopanel.log_event(
                f"Using custom reply_to: {request.reply_to}",
                severity="info",
                event_type="telemetry_custom_reply"
            )
            return request.reply_to

        return self._response_topic

    @staticmethod
    def _get_transport_value(
        message: Dict[str, Any],
        payload: Dict[str, Any],
        field: str
    ) -> Any:
        """Извлечение значения из transport-уровня или payload."""
        if field in message:
            return message[field]
        return payload.get(field)


class SitlMessagingComponent(BaseAsyncComponent):
    """
    Компонент обработки запросов позиций дронов (ЦПБ-4, ЦПБ-5).

    Реализует шаблон Proxy для контроля доступа к телеметрии.
    Все запросы проходят через TelemetryProxy для валидации и аудита.

    Trust Level: HIGH
    Attack Surface: Средняя (принимает VERIFIED запросы, READ-ONLY доступ к Redis)
    Operation Type: READ-ONLY (не изменяет состояние дронов)
    """

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
        self._telemetry_proxy: Optional[TelemetryProxy] = None

        super().__init__(
            component_id=component_id,
            component_type="sitl_messaging",
            topic=topic,
            bus=bus,
        )

    async def _get_redis(self) -> redis.Redis:
        """Получение Redis клиента (READ-ONLY доступ)."""
        if self._redis is None:
            self._redis = redis.from_url(self._redis_url, decode_responses=True)

            # Инициализация TelemetryProxy после создания Redis клиента
            self._telemetry_proxy = TelemetryProxy(
                redis_client=self._redis,
                response_topic=self._response_topic,
                infopanel_client=self._infopanel,
                component_id=self.component_id,
            )

            self._infopanel.log_event(
                f"Redis connection established: {self._redis_url}",
                severity="info",
                event_type="redis_connected"
            )

        return self._redis

    def _register_handlers(self):
        """Регистрация обработчиков запросов телеметрии."""
        self.register_handler("request_position", self._handle_request_position)

    def start(self):
        """
        Запуск компонента и подписка на топик запросов.

        Подписывается на:
        - sitl.telemetry.request (запросы позиций)
        """
        self._infopanel.log_event(
            f"Starting telemetry component on topic: {self.topic}",
            severity="info",
            event_type="component_start"
        )

        super().start()

    async def _handle_request_position(
        self,
        message: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Обработка запроса позиции дрона через TelemetryProxy.

        Trust Boundary: REQUEST (validated by verifier) → TELEMETRY DATA → RESPONSE

        Политика безопасности:
        - Все запросы проходят через Proxy
        - READ-ONLY доступ к Redis
        - Контроль reply_to
        - Полный аудит через correlation_id
        """
        # Инициализация Redis и Proxy при первом запросе
        await self._get_redis()

        # Проксирование запроса через TelemetryProxy
        response = await self._telemetry_proxy.process_request(message)

        if not response.success:
            self._infopanel.log_event(
                f"Telemetry request failed: drone_id={response.drone_id}, "
                f"error={response.error}",
                severity="warning",
                event_type="telemetry_request_failed"
            )
            return {"error": response.error}

        # Формирование ответного сообщения через SDK
        payload = message.get("payload", message)
        response_message = create_response(
            correlation_id=self._telemetry_proxy._get_transport_value(
                message, payload, "correlation_id"
            ),
            payload=response.position,
            sender=self.component_id,
            success=True,
        )
        response_message["drone_id"] = response.drone_id

        # Публикация в контролируемый reply_topic (ЦПБ-5: изоляция)
        self.bus.publish(response.reply_topic, response_message)

        self._infopanel.log_event(
            f"Telemetry response published: drone_id={response.drone_id}, "
            f"topic={response.reply_topic}",
            severity="info",
            event_type="telemetry_response_published"
        )

        return response.position

    async def stop(self):
        """Остановка компонента и закрытие Redis соединения."""
        if self._redis:
            await self._redis.close()
            self._infopanel.log_event(
                "Redis connection closed",
                severity="info",
                event_type="redis_disconnected"
            )

        await super().stop()