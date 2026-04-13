"""
Async-версия BaseComponent для компонентов с async-логикой.

Используется когда компонент работает с:
- redis.asyncio
- aiokafka / aiomqtt
- Другими async-библиотеками
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Optional, Awaitable

from broker.system_bus import SystemBus
from sdk.messages import create_response


class BaseAsyncComponent(ABC):
    """
    Абстрактный базовый класс для async-компонентов дрона.

    Компонент:
    - Подключается к SystemBus (единая шина с системами)
    - Подписывается на свой топик (components.{component_type})
    - Обрабатывает сообщения через маршрутизацию по action
    - Поддерживает async обработчики
    - Может запускать фоновые задачи (background tasks)
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

        self._handlers: Dict[str, Callable[[Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]]] = {}
        self._running = False
        self._background_tasks: list[asyncio.Task] = []

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
        handler: Callable[[Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]],
    ):
        """Регистрирует async обработчик для action."""
        self._handlers[action] = handler

    async def _handle_message(self, message: Dict[str, Any]):
        """Async маршрутизация входящего сообщения по action."""
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
            result = await handler(message)
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

    async def _handle_ping(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {"pong": True, "component_id": self.component_id}

    async def _handle_get_status(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "component_id": self.component_id,
            "component_type": self.component_type,
            "topic": self.topic,
            "running": self._running,
            "handlers": list(self._handlers.keys()),
            "background_tasks": len(self._background_tasks),
        }

    def add_background_task(self, coro):
        """Добавляет фоновую async-задачу."""
        task = asyncio.create_task(coro)
        self._background_tasks.append(task)
        return task

    def start(self):
        """Подписывается на топик и запускает шину."""
        self.bus.start()
        # Запоминаем loop для вызова корутин из callback-потоков (mqtt/kafka worker threads)
        self._loop = asyncio.get_event_loop()
        self.bus.subscribe(
            self.topic,
            lambda msg: asyncio.run_coroutine_threadsafe(self._handle_message(msg), self._loop),
        )
        self._running = True
        print(f"[{self.component_id}] Started. Listening on topic: {self.topic}")

    def stop(self):
        """Останавливает фоновые задачи и шину."""
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        self.bus.unsubscribe(self.topic)
        self.bus.stop()
        print(f"[{self.component_id}] Stopped")
