"""
Базовый класс для всех систем, использующих SystemBus.

Предоставляет унифицированный интерфейс для:
- Подписки на топик системы
- Обработки входящих сообщений
- Маршрутизации по action
- Health check endpoint
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Optional
import threading
import signal
import sys

from flask import Flask, jsonify

from broker.system_bus import SystemBus
from sdk.messages import create_response


class BaseSystem(ABC):
    """
    Абстрактный базовый класс для всех систем.

    Каждая система:
    - Подключается к SystemBus
    - Подписывается на свой топик
    - Обрабатывает сообщения через маршрутизацию по action
    - Имеет health check endpoint
    """

    def __init__(
        self,
        system_id: str,
        system_type: str,
        topic: str,
        bus: SystemBus,
        health_port: Optional[int] = None
    ):
        self.system_id = system_id
        self.system_type = system_type
        self.topic = topic
        self.bus = bus
        self.health_port = health_port

        self._handlers: Dict[str, Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = {}
        self._health_app: Optional[Flask] = None
        self._health_thread: Optional[threading.Thread] = None
        self._running = False

        self._setup_handlers()
        self._register_handlers()

    def _setup_handlers(self):
        """Регистрирует базовые обработчики."""
        self.register_handler("ping", self._handle_ping)
        self.register_handler("get_status", self._handle_get_status)

    @abstractmethod
    def _register_handlers(self):
        """Регистрирует обработчики сообщений для конкретной системы."""
        pass

    def register_handler(
        self,
        action: str,
        handler: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    ):
        """Регистрирует обработчик для действия."""
        self._handlers[action] = handler

    def _handle_message(self, message: Dict[str, Any]):
        """Маршрутизирует входящее сообщение по полю action."""
        action = message.get("action")

        if not action:
            print(f"[{self.system_id}] Message without action: {message}")
            return

        handler = self._handlers.get(action)

        if not handler:
            print(f"[{self.system_id}] Unknown action: {action}")
            if message.get("reply_to"):
                self.bus.respond(
                    message,
                    {"error": f"Unknown action: {action}"},
                    action="error"
                )
            return

        try:
            result = handler(message)
            if message.get("reply_to") and result is not None:
                response = create_response(
                    correlation_id=message.get("correlation_id"),
                    payload=result,
                    sender=self.system_id,
                    success=True
                )
                self.bus.publish(message["reply_to"], response)

        except Exception as e:
            print(f"[{self.system_id}] Error handling {action}: {e}")
            if message.get("reply_to"):
                response = create_response(
                    correlation_id=message.get("correlation_id"),
                    payload={},
                    sender=self.system_id,
                    success=False,
                    error=str(e)
                )
                self.bus.publish(message["reply_to"], response)

    def _handle_ping(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {"pong": True, "system_id": self.system_id}

    def _handle_get_status(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return self.get_status()

    def get_status(self) -> Dict[str, Any]:
        """Возвращает статус системы. Переопределяйте для добавления метрик."""
        return {
            "system_id": self.system_id,
            "system_type": self.system_type,
            "topic": self.topic,
            "running": self._running,
            "handlers": list(self._handlers.keys())
        }

    def _setup_health_check(self):
        """Настраивает Flask app для health check."""
        if not self.health_port:
            return

        self._health_app = Flask(f"{self.system_type}_health")

        import logging
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)

        @self._health_app.route('/health')
        def health():
            return jsonify({
                "status": "healthy" if self._running else "starting",
                "system_id": self.system_id,
                "system_type": self.system_type
            })

        @self._health_app.route('/status')
        def status():
            return jsonify(self.get_status())

    def _run_health_server(self):
        """Запускает health check сервер в отдельном потоке."""
        if self._health_app and self.health_port:
            self._health_app.run(
                host='0.0.0.0',
                port=self.health_port,
                threaded=True,
                use_reloader=False
            )

    def start(self):
        """Запускает систему: SystemBus, подписка на топик, health check."""
        print(f"[{self.system_id}] Starting {self.system_type}...")

        self.bus.start()
        self.bus.subscribe(self.topic, self._handle_message)
        self._running = True

        self._setup_health_check()
        if self._health_app and self.health_port:
            self._health_thread = threading.Thread(
                target=self._run_health_server,
                daemon=True,
                name=f"{self.system_type}-health"
            )
            self._health_thread.start()
            print(f"[{self.system_id}] Health check on port {self.health_port}")

        print(f"[{self.system_id}] Started. Listening on topic: {self.topic}")

    def stop(self):
        """Останавливает систему."""
        print(f"[{self.system_id}] Stopping...")
        self._running = False
        self.bus.unsubscribe(self.topic)
        self.bus.stop()
        print(f"[{self.system_id}] Stopped")

    def run_forever(self):
        """Запускает систему и блокирует до получения сигнала остановки."""
        def signal_handler(sig, frame):
            print(f"\n[{self.system_id}] Received signal {sig}, shutting down...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        self.start()
        print(f"[{self.system_id}] Running. Press Ctrl+C to stop.")

        try:
            while self._running:
                signal.pause()
        except AttributeError:
            import time
            while self._running:
                time.sleep(1)
