import pathlib
import sys
import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Импортируем сам класс клиента, а не фабрику, чтобы избежать конфликта с глобальным моком
from shared.infopanel_client import InfopanelClient  # noqa: E402


# ==============================================================================
# 1. Тесты методов клиента (создаем InfopanelClient напрямую)
# ==============================================================================
@pytest.mark.asyncio
async def test_log_event_adds_to_queue_and_truncates():
    client = InfopanelClient("url", "k", "s", 1)
    long_msg = "A" * 2000
    client.log_event(long_msg, severity="warn", event_type="test_event")
    
    assert client._queue.qsize() == 1
    entry = client._queue.get_nowait()
    assert len(entry["message"]) == 1024
    assert entry["severity"] == "warn"
    assert entry["event_type"] == "test_event"

@pytest.mark.asyncio
async def test_log_event_ignored_when_stopped():
    client = InfopanelClient("url", "k", "s", 1)
    client._stopped = True
    client.log_event("should_be_ignored")
    assert client._queue.qsize() == 0

@pytest.mark.asyncio
async def test_log_event_queue_full_silently_ignored():
    client = InfopanelClient("url", "k", "s", 1)
    client._queue = asyncio.Queue(maxsize=1)
    client._queue.put_nowait({"existing": True})
    # Не должно падать с asyncio.QueueFull
    client.log_event("overflow_msg")
    assert client._queue.qsize() == 1


# ==============================================================================
# 2. Метод _send_batch (мокаем aiohttp)
# ==============================================================================
@pytest.mark.asyncio
async def test_send_batch_success():
    client = InfopanelClient("https://api", "k", "s", 1)
    client._session = MagicMock()
    
    mock_resp = MagicMock()
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)
    mock_resp.raise_for_status = MagicMock()
    client._session.post = MagicMock(return_value=mock_resp)

    batch = [{"msg": "ok1"}, {"msg": "ok2"}]
    # Мокаем task_done, так как элементы batch не были взяты из очереди через get()
    with patch.object(client._queue, 'task_done', MagicMock()):
        await client._send_batch(batch)
    
    client._session.post.assert_called_once()

@pytest.mark.asyncio
async def test_send_batch_failure_does_not_crash():
    client = InfopanelClient("https://api", "k", "s", 1)
    client._session = MagicMock()
    client._session.post = MagicMock(side_effect=ConnectionError("timeout"))

    batch = [{"msg": "fail"}]
    with patch.object(client._queue, 'task_done', MagicMock()):
        await client._send_batch(batch)
    client._session.post.assert_called_once()


# ==============================================================================
# 3. Lifecycle: start, stop, _worker
# ==============================================================================
@pytest.mark.asyncio
async def test_start_is_idempotent():
    client = InfopanelClient("url", "k", "s", 1)
    with patch("asyncio.create_task", return_value=MagicMock()) as mock_create:
        await client.start()
        assert mock_create.call_count == 1
        # Второй вызов должен игнорироваться
        await client.start()
        assert mock_create.call_count == 1

@pytest.mark.asyncio
async def test_worker_flushes_batch_on_timer():
    client = InfopanelClient("url", "k", "s", 1, flush_interval=0.05)
    
    # Мок должен вызывать task_done(), иначе stop() зависнет на queue.join()
    async def mock_send_and_mark_done(batch):
        for _ in batch:
            client._queue.task_done()
            
    client._send_batch = AsyncMock(side_effect=mock_send_and_mark_done)
    
    client.log_event("event_1")
    client.log_event("event_2")
    
    await client.start()
    await asyncio.sleep(0.15)
    await client.stop()

    client._send_batch.assert_called_once()
    assert client._stopped is True

@pytest.mark.asyncio
async def test_worker_continues_after_send_error():
    client = InfopanelClient("url", "k", "s", 1, flush_interval=0.05)
    
    async def mock_fail_and_mark_done(batch):
        for _ in batch:
            client._queue.task_done()
        raise RuntimeError("Network fail")

    client._send_batch = AsyncMock(side_effect=mock_fail_and_mark_done)
    client.log_event("e1")
    
    await client.start()
    await asyncio.sleep(0.2)
    await client.stop()
    assert client._stopped is True