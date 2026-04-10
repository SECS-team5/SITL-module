import asyncio
import os
import time
from typing import Any

import aiohttp

DEFAULT_INFOPANEL_URL = "https://infopanel.csse.ru/api"
DEFAULT_SERVICE = "SITL"
DEFAULT_SERVICE_ID = 5
DEFAULT_API_VERSION = "1.0.0"
DEFAULT_BATCH_SIZE = 100
DEFAULT_FLUSH_INTERVAL = 5.0


class InfopanelClient:
    """Простой клиент для отправки логов в инфопанель через X-API-Key"""

    def __init__(
            self,
            base_url: str,
            api_key: str,
            service: str,
            service_id: int,
            api_version: str = DEFAULT_API_VERSION,
            batch_size: int = DEFAULT_BATCH_SIZE,
            flush_interval: float = DEFAULT_FLUSH_INTERVAL,
    ):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._service = service
        self._service_id = service_id
        self._api_version = api_version
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._session: aiohttp.ClientSession | None = None
        self._worker_task: asyncio.Task | None = None
        self._stopped = False

    async def start(self) -> None:
        """Запустить фоновую отправку логов"""
        if self._worker_task is not None:
            return
        self._session = aiohttp.ClientSession()
        self._worker_task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        """Остановить клиент и отправить оставшиеся логи"""
        self._stopped = True
        if self._worker_task:
            await self._queue.join()
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()

    def log_event(
            self,
            message: str,
            severity: str = "info",
            event_type: str = "event",
    ) -> None:
        """Добавить лог в очередь (неблокирующий вызов)"""
        if self._stopped:
            return

        entry = {
            "apiVersion": self._api_version,
            "timestamp": int(time.time() * 1000),
            "event_type": event_type,
            "service": self._service,
            "service_id": self._service_id,
            "severity": severity.lower(),
            "message": message[:1024],  # Обрезаем до максимальной длины
        }

        try:
            self._queue.put_nowait(entry)
        except asyncio.QueueFull:
            # Тихо игнорируем переполнение очереди
            pass

    async def _worker(self) -> None:
        """Фоновая задача для отправки логов батчами"""
        batch: list[dict[str, Any]] = []

        while not self._stopped:
            try:
                # Собираем батч в течение flush_interval или до заполнения
                deadline = asyncio.get_event_loop().time() + self._flush_interval

                while len(batch) < self._batch_size:
                    remaining_time = deadline - asyncio.get_event_loop().time()
                    if remaining_time <= 0:
                        break

                    try:
                        entry = await asyncio.wait_for(
                            self._queue.get(),
                            timeout=max(0.1, remaining_time),
                        )
                        batch.append(entry)
                    except asyncio.TimeoutError:
                        break

                # Отправляем батч, если есть что отправлять
                if batch:
                    await self._send_batch(batch)
                    batch = []

            except Exception:
                # При любой ошибке ждём секунду и продолжаем
                await asyncio.sleep(1.0)

    async def _send_batch(self, batch: list[dict[str, Any]]) -> None:
        """Отправить батч логов в инфопанель"""
        try:
            async with self._session.post(
                    f"{self._base_url}/log/event",
                    json=batch,
                    headers={"X-API-Key": self._api_key},
                    timeout=aiohttp.ClientTimeout(total=10.0),
            ) as resp:
                resp.raise_for_status()

                # Помечаем все элементы батча как обработанные
                for _ in batch:
                    self._queue.task_done()

        except Exception as exc:
            # Fallback: выводим ошибку в stderr
            print(f"[InfopanelClient] Failed to send {len(batch)} logs: {exc}")

            # Всё равно помечаем как обработанные, чтобы не зависнуть
            for _ in batch:
                self._queue.task_done()


def create_infopanel_client_from_env(env: dict[str, str] | None = None) -> InfopanelClient:
    """Создать клиент инфопанели из переменных окружения"""
    source = env if env is not None else os.environ

    base_url = source.get("INFOPANEL_URL", DEFAULT_INFOPANEL_URL)
    api_key = source.get("INFOPANEL_API_KEY", "")

    if not api_key:
        raise ValueError("INFOPANEL_API_KEY must be set")

    service = source.get("INFOPANEL_SERVICE", DEFAULT_SERVICE)
    service_id = int(source.get("INFOPANEL_SERVICE_ID", str(DEFAULT_SERVICE_ID)))

    return InfopanelClient(
        base_url=base_url,
        api_key=api_key,
        service=service,
        service_id=service_id,
    )