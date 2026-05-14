"""
SITL Messaging — компонент запросов/ответов позиций дронов.

"""
import os
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

import redis.asyncio as redis

from sdk.base_async_component import BaseAsyncComponent
from sdk.messages import create_response
from broker.system_bus import SystemBus

from shared.contracts import (
    POSITION_REQUEST_TOPIC_DEFAULT,
    POSITION_RESPONSE_TOPIC_DEFAULT,
    POSITION_REQUEST_SCHEMA_NAME,
    validate_schema,
)
from shared.state import build_position_response, normalize_state
from shared.infopanel_client import create_infopanel_client_from_env


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
    state: Optional[Dict[str, Any]]
    error: Optional[str]
    reply_topic: str


class TelemetryProxy:
    """
    Proxy для контроля доступа к телеметрии дронов.

    ЦПБ-4: Центр безопасной телеметрии
    ЦПБ-5: Центр изоляции сообщений

    Функции:
    1. Валидация запроса по схеме
    2. Поиск состояния дрона в Redis (READ-ONLY)
    3. Трассировка через correlation_id
    4. Формирование ответа с координатами
    5. Контроль reply_to
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
        message: Dict[str, Any],
    ) -> TelemetryResponse:
        """
        Обработка запроса телеметрии через Proxy.

        Trust Boundary: REQUEST → VALIDATED → RESPONSE
        """
        # Step 1: Валидация структуры запроса
        telemetry_request = self._validate_request(message)
        if telemetry_request is None:
            return TelemetryResponse(
                success=False,
                drone_id="unknown",
                position=None,
                state=None,
                error="invalid request structure",
                reply_topic=self._response_topic,
            )

        # Step 2: Поиск состояния дрона в Redis (READ-ONLY)
        found, state = await self._fetch_drone_state(telemetry_request.drone_id)
        if not found:
            return TelemetryResponse(
                success=False,
                drone_id=telemetry_request.drone_id,
                position=None,
                state=None,
                error=f"drone '{telemetry_request.drone_id}' state not found",
                reply_topic=self._get_reply_topic(telemetry_request),
            )

        # Step 3: Трассировка запроса
        self._trace_request(telemetry_request)

        # Step 4: Формирование ответа с координатами
        position = self._build_position(state, telemetry_request.drone_id)
        if position is None:
            return TelemetryResponse(
                success=False,
                drone_id=telemetry_request.drone_id,
                position=None,
                state=None,
                error="invalid position in state",
                reply_topic=self._get_reply_topic(telemetry_request),
            )

        return TelemetryResponse(
            success=True,
            drone_id=telemetry_request.drone_id,
            position=position,
            state=state,
            error=None,
            reply_topic=self._get_reply_topic(telemetry_request),
        )

    def _validate_request(
        self,
        message: Dict[str, Any],
    ) -> Optional[TelemetryRequest]:
        """
        Proxy Function 1: Валидация структуры запроса по схеме.

        ЦПБ-4 Decision Point: Пропустить или отклонить запрос
        """
        payload = message.get("payload", message)

        ok, reason = validate_schema(payload, POSITION_REQUEST_SCHEMA_NAME)
        if not ok:
            self._infopanel.log_event(
                f"Rejected position request: {reason}",
                severity="warning",
                event_type="telemetry_request_rejected",
            )
            return None

        return TelemetryRequest(
            drone_id=payload["drone_id"],
            correlation_id=self._get_transport_value(message, payload, "correlation_id"),
            reply_to=self._get_transport_value(message, payload, "reply_to"),
            raw_message=message,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    async def _fetch_drone_state(
        self,
        drone_id: str,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Proxy Function 2: Поиск состояния дрона в Redis.

        ЦПБ-4 Decision Point: Возврат координат или отказ при отсутствии состояния
        Политика безопасности: READ-ONLY операция
        """
        raw_state = await self._redis.hgetall(f"drone:{drone_id}:state")
        if not raw_state:
            self._infopanel.log_event(
                f"drone '{drone_id}' state not found",
                severity="warning",
                event_type="telemetry_state_not_found",
            )
            return False, None

        return True, normalize_state(raw_state)

    def _trace_request(self, request: TelemetryRequest) -> None:
        """
        Proxy Function 3: Трассировка запроса через correlation_id.

        ЦПБ-5 Decision Point: Сохранение correlation_id для traceability
        """
        self._infopanel.log_event(
            f"Telemetry request: drone_id={request.drone_id}, "
            f"correlation_id={request.correlation_id}, "
            f"timestamp={request.timestamp}",
            severity="info",
            event_type="telemetry_traced",
        )

    def _build_position(
        self,
        state: Dict[str, Any],
        drone_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Proxy Function 4: Формирование ответа с координатами.

        ЦПБ-4 Decision Point: Формирование безопасного ответа
        Политика безопасности: READ-ONLY, только координаты
        """
        position = build_position_response(state)
        if position is None:
            self._infopanel.log_event(
                f"drone '{drone_id}' state does not contain a valid position",
                severity="warning",
                event_type="telemetry_invalid_position",
            )
        return position

    def _get_reply_topic(self, request: TelemetryRequest) -> str:
        """
        Proxy Function 5: Контроль назначения ответа.

        ЦПБ-5 Decision Point: Публикация в reply_to или default топик
        Риск: Зависимость от корректного reply_to
        """
        return request.reply_to or self._response_topic

    @staticmethod
    def _get_transport_value(
        message: Dict[str, Any],
        payload: Dict[str, Any],
        field: str,
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

    Trust Level: HIGH (80%)
    Attack Surface: Средняя (READ-ONLY доступ к Redis)
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
        """Ленивая инициализация Redis клиента и TelemetryProxy."""
        if self._redis is None:
            self._redis = redis.from_url(self._redis_url, decode_responses=True)
            self._telemetry_proxy = TelemetryProxy(
                redis_client=self._redis,
                response_topic=self._response_topic,
                infopanel_client=self._infopanel,
                component_id=self.component_id,
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
        super().start()

    async def _handle_request_position(
        self,
        message: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Обработка запроса позиции дрона через TelemetryProxy.

        Trust Boundary: REQUEST → TELEMETRY DATA → RESPONSE
        Политика безопасности: только чтение, ответ в reply_to или default топик
        """
        await self._get_redis()

        # Проксирование запроса через TelemetryProxy
        result = await self._telemetry_proxy.process_request(message)

        if not result.success:
            return {"error": result.error}

        # Вывод информации о дроне в консоль
        self._print_drone_info(result.drone_id, result.state, result.position)

        # Формирование и публикация ответа
        payload = message.get("payload", message)
        response_message = create_response(
            correlation_id=self._telemetry_proxy._get_transport_value(
                message, payload, "correlation_id"
            ),
            payload=result.position,
            sender=self.component_id,
            success=True,
        )
        response_message["drone_id"] = result.drone_id

        self.bus.publish(result.reply_topic, response_message)

        self._infopanel.log_event(
            f"Returned position for drone_id={result.drone_id}",
            severity="info",
            event_type="telemetry_response_published",
        )

        return result.position

    def _print_drone_info(
        self,
        drone_id: str,
        state: Dict[str, Any],
        response: Dict[str, Any],
    ) -> None:
        """Вывод информации о дроне в консоль при запросе позиции."""
        print("\n" + "="*80)
        print(f"[POSITION REQUEST] Запрос позиции дрона")
        print(f"[POSITION REQUEST] Дрон ID: {drone_id}")
        print(f"[POSITION REQUEST] Статус: {state.get('status', 'N/A')}")
        print("-"*80)
        print(f"[POSITION REQUEST] Текущие координаты:")
        print(f"  X: {response.get('x', 'N/A')}")
        print(f"  Y: {response.get('y', 'N/A')}")
        print(f"  Z: {response.get('z', 'N/A')}")
        print("-"*80)
        print(f"[POSITION REQUEST] Вектор направления (скорость):")
        print(f"  VX: {state.get('vx', 'N/A')}")
        print(f"  VY: {state.get('vy', 'N/A')}")
        print(f"  VZ: {state.get('vz', 'N/A')}")
        print("-"*80)
        if state.get("home_x") is not None:
            print(f"[POSITION REQUEST] Домашняя локация:")
            print(f"  Home X: {state.get('home_x', 'N/A')}")
            print(f"  Home Y: {state.get('home_y', 'N/A')}")
            print(f"  Home Z: {state.get('home_z', 'N/A')}")
        else:
            print(f"[POSITION REQUEST] Домашняя локация: НЕ УСТАНОВЛЕНА")
        print("="*80 + "\n")

    async def stop(self):
        """Остановка компонента и закрытие Redis соединения."""
        if self._redis:
            await self._redis.close()
        await super().stop()