"""
Базовый класс для компонентов, использующих SystemBus.

Аналогичен BaseSystem, но без health check и run_forever.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Optional

from broker.system_bus import SystemBus
from sdk.messages import create_response


class BaseComponent(ABC):
    """
    Абстрактный базовый класс для компонентов дрона.

    Компонент:
    - Подключается к SystemBus (единая шина с системами)
    - Подписывается на свой топик (components.{component_type})
    - Обрабатывает сообщения через маршрутизацию по action
    - Отвечает через reply_to (request/response) или publish
    """

    def __init__(
        self,
        component_id: str,
        component_type: str,
        topic: str,
        bus: SystemBus,
    ):
        self.component_id = component_id
        self.component_type = component_type
        self.topic = topic
        self.bus = bus

        self._handlers: Dict[str, Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = {}
        self._running = False

        self._setup_handlers()
        self._register_handlers()

    def _setup_handlers(self):
        """Базовые обработчики."""
        self.register_handler("ping", self._handle_ping)
        self.register_handler("get_status", self._handle_get_status)

    @abstractmethod
    def _register_handlers(self):
        """Регистрирует обработчики конкретного компонента."""
        pass

    def register_handler(
        self,
        action: str,
        handler: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    ):
        """Регистрирует обработчик для action."""
        self._handlers[action] = handler

    def _handle_message(self, message: Dict[str, Any]):
        """Маршрутизация входящего сообщения по action."""
        action = message.get("action")
        if not action:
            print(f"[{self.component_id}] Message without action: {message}")
            return

        handler = self._handlers.get(action)
        if not handler:
            print(f"[{self.component_id}] Unknown action: {action}")
            if message.get("reply_to"):
                self.bus.respond(message, {"error": f"Unknown action: {action}"}, action="error")
            return

        try:
            result = handler(message)
            if message.get("reply_to") and result is not None:
                response = create_response(
                    correlation_id=message.get("correlation_id"),
                    payload=result,
                    sender=self.component_id,
                    success=True,
                )
                self.bus.publish(message["reply_to"], response)
        except Exception as e:
            print(f"[{self.component_id}] Error handling {action}: {e}")
            if message.get("reply_to"):
                response = create_response(
                    correlation_id=message.get("correlation_id"),
                    payload={},
                    sender=self.component_id,
                    success=False,
                    error=str(e),
                )
                self.bus.publish(message["reply_to"], response)

    def _handle_ping(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {"pong": True, "component_id": self.component_id}

    def _handle_get_status(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "component_id": self.component_id,
            "component_type": self.component_type,
            "topic": self.topic,
            "running": self._running,
            "handlers": list(self._handlers.keys()),
        }

    def start(self):
        """Подписывается на топик и запускает шину."""
        self.bus.start()
        self.bus.subscribe(self.topic, self._handle_message)
        self._running = True
        print(f"[{self.component_id}] Started. Listening on topic: {self.topic}")

    def stop(self):
        """Отписывается и останавливает шину."""
        self._running = False
        self.bus.unsubscribe(self.topic)
        self.bus.stop()
        print(f"[{self.component_id}] Stopped")
