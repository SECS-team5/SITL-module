"""
Базовый интерфейс SystemBus для передачи сообщений между системами.
Поддерживает Kafka и MQTT реализации.
"""
from abc import ABC, abstractmethod
from typing import Callable, Dict, Any, Optional
import asyncio


class SystemBus(ABC):
    """
    Абстрактный базовый класс для системы передачи сообщений между системами.
    
    В отличие от EventBus (для модулей дрона), SystemBus предназначен
    для межсистемного взаимодействия (Certification, Fleet, Insurance и т.д.)
    
    Особенности:
    - Работает с dict сообщениями (не Event объекты)
    - Поддерживает request/response паттерн через correlation_id
    - Использует топики вида systems.{system_name}
    """

    @abstractmethod
    def publish(self, topic: str, message: Dict[str, Any]) -> bool:
        """
        Публикует сообщение в указанный топик.
        
        Args:
            topic: Имя топика (например, "systems.certification")
            message: Сообщение в формате dict с полями:
                     - action: str - действие для маршрутизации
                     - correlation_id: str (optional) - для request/response
                     - reply_to: str (optional) - топик для ответа
                     - sender: str - идентификатор отправителя
                     - payload: dict - данные сообщения
            
        Returns:
            bool: True если сообщение успешно отправлено
        """
        pass

    @abstractmethod
    def subscribe(self, topic: str, callback: Callable[[Dict[str, Any]], None]) -> bool:
        """
        Подписывается на получение сообщений из топика.
        
        Args:
            topic: Имя топика для подписки
            callback: Функция-обработчик, вызываемая при получении сообщения
            
        Returns:
            bool: True если подписка успешно создана
        """
        pass

    @abstractmethod
    def unsubscribe(self, topic: str) -> bool:
        """
        Отписывается от топика.
        
        Args:
            topic: Имя топика для отписки
            
        Returns:
            bool: True если отписка успешна
        """
        pass

    @abstractmethod
    def request(
        self, 
        topic: str, 
        message: Dict[str, Any], 
        timeout: float = 30.0
    ) -> Optional[Dict[str, Any]]:
        """
        Отправляет запрос и ожидает ответ (синхронный request/response).
        
        Использует correlation_id для связывания запроса и ответа.
        
        Args:
            topic: Топик для отправки запроса
            message: Сообщение запроса (correlation_id будет добавлен автоматически)
            timeout: Таймаут ожидания ответа в секундах
            
        Returns:
            Dict: Ответное сообщение или None при таймауте
        """
        pass

    @abstractmethod
    def request_async(
        self, 
        topic: str, 
        message: Dict[str, Any], 
        timeout: float = 30.0
    ) -> "asyncio.Future[Optional[Dict[str, Any]]]":
        """
        Асинхронная версия request().
        
        Args:
            topic: Топик для отправки запроса
            message: Сообщение запроса
            timeout: Таймаут ожидания ответа в секундах
            
        Returns:
            Future с ответным сообщением или None при таймауте
        """
        pass

    @abstractmethod
    def start(self) -> None:
        """Запускает обработку сообщений (если требуется)."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Останавливает обработку сообщений и освобождает ресурсы."""
        pass

    def respond(
        self, 
        original_message: Dict[str, Any], 
        response_payload: Dict[str, Any],
        action: str = "response"
    ) -> bool:
        """
        Отправляет ответ на запрос.
        
        Удобный метод для формирования и отправки ответа с правильным
        correlation_id и в правильный топик (reply_to).
        
        Args:
            original_message: Исходное сообщение запроса
            response_payload: Данные ответа
            action: Тип действия в ответе (по умолчанию "response")
            
        Returns:
            bool: True если ответ успешно отправлен
        """
        reply_to = original_message.get("reply_to")
        correlation_id = original_message.get("correlation_id")
        
        if not reply_to:
            print(f"Cannot respond: no reply_to in message")
            return False
        
        response = {
            "action": action,
            "correlation_id": correlation_id,
            "payload": response_payload
        }
        
        return self.publish(reply_to, response)
