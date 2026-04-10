"""
SDK пакет для разработки компонентов и систем дрона.

Содержит:
- Протокол сообщений (Message, create_response)
- Базовые классы (BaseComponent, BaseAsyncComponent)

Для создания нового компонента в отдельном репо:
    pip install -e path/to/common-repo
    from sdk import BaseComponent, BaseAsyncComponent, create_response
"""
from sdk.messages import Message, create_response
from sdk.base_component import BaseComponent
from sdk.base_async_component import BaseAsyncComponent

__all__ = ["Message", "create_response", "BaseComponent", "BaseAsyncComponent"]
