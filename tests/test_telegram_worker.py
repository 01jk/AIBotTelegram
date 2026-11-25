import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import aiosqlite
import pytest
from pydantic import SecretStr
from telethon import errors

from aibottelegram.config import Mode
from aibottelegram.storage.database import initialize
from aibottelegram.telegram import worker


@pytest.mark.asyncio
async def test_worker_missing_session_does_not_create_client(monkeypatch, tmp_path, caplog):
    factory = Mock()
    monkeypatch.setattr(worker, "make_client", factory)
    path = tmp_path / "missing.session"
    await worker.run_worker(path, tmp_path / "test.sqlite3", worker.Credentials(1, SecretStr("test"), 2))
    factory.assert_not_called()
    assert not path.exists()
    assert "run the login command" in caplog.text


@pytest.mark.asyncio
async def test_new_message_handler_gates_events_and_extracts_forum_thread(caplog):
    client = Mock()
    queue = asyncio.Queue(maxsize=1)
    accepting = asyncio.Event()
    worker.attach_reader(client, queue, accepting)
    handler = client.add_event_handler.call_args_list[0].args[0]
    reply = SimpleNamespace(forum_topic=True, reply_to_top_id=51, reply_to_msg_id=52)
    event = SimpleNamespace(chat_id=-1000000000042, id=55, message=SimpleNamespace(reply_to=reply), out=True)
    await handler(event)
    assert queue.empty()
    accepting.set()
    await handler(event)
    await handler(event)
    await handler(event)
    assert queue.get_nowait() == worker.ObservedMessage(-1000000000042, 55, 51, True)
    assert caplog.text.count("telegram_events_dropped") == 1


@pytest.mark.asyncio
async def test_event_reader_checks_chat_and_account_on_every_message(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    async with aiosqlite.connect(tmp_path / "test.sqlite3") as db:
        await initialize(db, Mode.OBSERVE)
        await db.executemany(
            "INSERT INTO chats (account_id, chat_id, mode) VALUES (?, ?, ?)",
            [(10, -21, "READ_ONLY"), (10, -22, "DENY"), (11, -23, "AUTO_REPLY")],
        )
        await db.commit()
        queue = asyncio.Queue()
        secret = worker.MemoryEvent(-21, 0, 40, 10, 100, "sk-or-v1-" + "a" * 64, outgoing=True)
        queue.put_nowait(worker.ObservedMessage(-21, 40, None, True, secret))
        for chat in (-21, -22, -23, -24):
            queue.put_nowait(worker.ObservedMessage(chat, 42, None, True))
        consumer = asyncio.create_task(worker.observe_events(db, 10, queue))
        try:
            await asyncio.wait_for(queue.join(), timeout=2)
            await db.execute("UPDATE chats SET mode = 'DENY' WHERE account_id = 10 AND chat_id = -21")
            await db.commit()
            queue.put_nowait(worker.ObservedMessage(-21, 43, None, True))
            await asyncio.wait_for(queue.join(), timeout=2)
        finally:
            consumer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consumer
    messages = [record.message for record in caplog.records]
    assert messages == ["telegram_message_observed chat_id=-21 message_id=42 thread_id=None direction=outgoing"]


@pytest.mark.asyncio
async def test_worker_never_subscribes_for_wrong_account(caplog):
    client = AsyncMock()
    client.get_me.return_value = SimpleNamespace(id=12, bot=False)
    accepting = asyncio.Event()
    await worker.listen(client, 13, accepting)
    client.set_receive_updates.assert_awaited_once_with(False)
    client.run_until_disconnected.assert_not_awaited()
    client.disconnect.assert_awaited_once()
    assert not accepting.is_set()
    assert "EXPECTED_ACCOUNT_ID" in caplog.text


@pytest.mark.asyncio
async def test_worker_retries_network_errors_with_bounded_backoff(monkeypatch, caplog):
    client = AsyncMock()
    client.get_me.return_value = SimpleNamespace(id=13, bot=False)
    client.connect.side_effect = [OSError("sensitive error detail")] * 8 + [None]
    client.run_until_disconnected.side_effect = errors.SessionRevokedError(request=None)
    sleep = AsyncMock()
    monkeypatch.setattr(worker.asyncio, "sleep", sleep)
    await worker.listen(client, 13, asyncio.Event())
    assert sleep.await_args_list == [call(seconds) for seconds in (1, 2, 4, 8, 15, 30, 60, 60)]
    assert client.connect.await_count == 9
    assert "session_unauthorized" in caplog.text
    assert "sensitive error detail" not in caplog.text


@pytest.mark.asyncio
async def test_revoked_session_stops_worker_without_retry(caplog):
    client = AsyncMock()
    client.get_me.return_value = SimpleNamespace(id=13, bot=False)
    client.run_until_disconnected.side_effect = errors.SessionRevokedError(request=None)
    accepting = asyncio.Event()
    await worker.listen(client, 13, accepting)
    assert client.set_receive_updates.await_args_list == [call(False), call(True)]
    client.connect.assert_awaited_once()
    assert not accepting.is_set()
    assert "session_unauthorized" in caplog.text
