"""
Протокол сообщений для SystemBus.

Все сообщения имеют единый формат с маршрутизацией по полю "action".
Этот формат является контрактом между всеми системами и компонентами.
"""
from dataclasses import dataclass, asdict, field
from typing import Dict, Any, Optional
from datetime import datetime, timezone


@dataclass
class Message:
    """Базовое сообщение для SystemBus."""
    action: str
    payload: Dict[str, Any] = field(default_factory=dict)
    sender: str = ""
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Конвертирует сообщение в dict для отправки."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """Создаёт Message из dict."""
        return cls(
            action=data.get("action", ""),
            payload=data.get("payload", {}),
            sender=data.get("sender", ""),
            correlation_id=data.get("correlation_id"),
            reply_to=data.get("reply_to"),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat())
        )


def create_response(
    correlation_id: str,
    payload: Dict[str, Any],
    sender: str,
    success: bool = True,
    error: Optional[str] = None
) -> Dict[str, Any]:
    """Создаёт ответ на запрос в формате протокола."""
    response = {
        "action": "response",
        "payload": payload,
        "sender": sender,
        "correlation_id": correlation_id,
        "success": success,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    if error:
        response["error"] = error
    return response
