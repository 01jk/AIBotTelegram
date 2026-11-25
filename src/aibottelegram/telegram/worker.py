import asyncio
import logging
import sqlite3
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from zoneinfo import ZoneInfo

import aiosqlite
from telethon import TelegramClient, errors, events, types

from ..config import Settings
from ..features.insights import run_insights
from ..features.memory import Memory, MemoryEvent
from ..features.mining import run_mining
from ..features.participation import ParticipationPlanner
from ..llm.credentials import KEY_TEXT
from ..llm.router import OpenRouter
from ..policy.outbound import OutboundGateway
from .outbound import TelethonOutbound, message_thread
from .session import (
    CONNECT_TIMEOUT,
    INVALID_AUTH,
    Credentials,
    SessionError,
    check_session_file,
    lock_session_directory,
    make_client,
    session_file,
    verify_account,
)

LOG = logging.getLogger(__name__)
RECONNECT_DELAYS = (1, 2, 4, 8, 15, 30, 60)
STABLE_CONNECTION_SECONDS = 60
EVENT_QUEUE_SIZE = 128
TEMPORARY_ERRORS = (OSError, errors.ServerError, errors.TimedOutError)


@dataclass
class WorkerStatus:
    state: str = "disabled"
    reason: str | None = None

    def set(self, state: str, reason: str | None = None) -> None:
        self.state, self.reason = state, reason


@dataclass(frozen=True, slots=True)
class ObservedMessage:
    chat_id: int
    message_id: int
    thread_id: int | None
    outgoing: bool
    memory: MemoryEvent | None = None


@dataclass(frozen=True, slots=True)
class DeletedMessages:
    chat_id: int | None
    message_ids: tuple[int, ...]


def attach_reader(
    client: TelegramClient,
    queue: asyncio.Queue[ObservedMessage | DeletedMessages],
    accepting: asyncio.Event,
    idle: asyncio.Event | None = None,
    overflow: asyncio.Event | None = None,
) -> None:
    overloaded = False

    def offer(update: ObservedMessage | DeletedMessages) -> None:
        nonlocal overloaded
        if idle is not None:
            idle.clear()
        try:
            queue.put_nowait(update)
            overloaded = False
        except asyncio.QueueFull:
            if overflow is not None:
                overflow.set()
            if not overloaded:
                LOG.warning("telegram_events_dropped reason=queue_full")
                overloaded = True

    async def on_message(event: events.NewMessage.Event, *, edited: bool = False) -> None:
        if not accepting.is_set() or event.chat_id is None:
            return
        message = event.message
        thread_id = message_thread(message) or None
        date = getattr(message, "date", None)
        edit_date = getattr(message, "edit_date", None)
        entity = getattr(event, "chat", None)
        sender = getattr(event, "sender", None)
        username = getattr(entity, "username", None) if isinstance(entity, types.Channel) else None
        memory = None
        if date is not None and (not edited or edit_date is not None):
            memory = MemoryEvent(
                event.chat_id, thread_id or 0, event.id, event.sender_id, int(date.timestamp()),
                (getattr(message, "message", None) or "")[:4096],
                getattr(message, "reply_to_msg_id", None), int(edit_date.timestamp()) if edited else None,
                event.out, bool(getattr(sender, "bot", False) or getattr(message, "via_bot_id", None)
                                or getattr(message, "fwd_from", None)),
                username, entity is not None, bool(getattr(message, "mentioned", False)),
                bool(getattr(entity, "send_paid_messages_stars", 0) or getattr(message, "paid_message_stars", 0)),
            )
        offer(ObservedMessage(event.chat_id, event.id, thread_id, event.out, memory))

    async def on_edit(event: events.MessageEdited.Event) -> None:
        await on_message(event, edited=True)

    async def on_delete(event: events.MessageDeleted.Event) -> None:
        if accepting.is_set():
            ids = tuple(event.deleted_ids)
            for start in range(0, len(ids), 100):
                offer(DeletedMessages(event.chat_id, ids[start:start + 100]))

    client.add_event_handler(on_message, events.NewMessage())
    client.add_event_handler(on_edit, events.MessageEdited())
    client.add_event_handler(on_delete, events.MessageDeleted())


async def observe_events(
    db: aiosqlite.Connection,
    account_id: int,
    queue: asyncio.Queue[ObservedMessage | DeletedMessages],
    gateway: OutboundGateway | None = None,
    idle: asyncio.Event | None = None,
    accepting: asyncio.Event | None = None,
    memory: Memory | None = None,
    planner: ParticipationPlanner | None = None,
    client: TelegramClient | None = None,
) -> None:
    peer_cache: OrderedDict[int, tuple[float, bool, str | None, bool]] = OrderedDict()
    while True:
        message = await queue.get()
        try:
            if isinstance(message, DeletedMessages):
                if memory is not None:
                    await memory.delete(message.chat_id, message.message_ids)
                continue
            if message.memory is not None and KEY_TEXT.search(message.memory.text):
                continue
            rows = await db.execute_fetchall(
                "SELECT 1 FROM chats WHERE account_id = ? AND chat_id = ? AND mode != 'DENY'",
                (account_id, message.chat_id),
            )
            if rows:
                event = message.memory
                if event is not None and not event.chat_known and event.chat < -1000000000000 and client is not None:
                    clock = asyncio.get_running_loop().time()
                    cached = peer_cache.get(event.chat)
                    if cached is None or cached[0] < clock:
                        try:
                            async with asyncio.timeout(3):
                                peer = await client.get_entity(event.chat)
                            cached = (clock + 60, True, peer.username if isinstance(peer, types.Channel) else None,
                                      bool(getattr(peer, "send_paid_messages_stars", 0)))
                        except (ValueError, TypeError, OSError, errors.RPCError):
                            cached = clock + 60, False, None, False
                        peer_cache[event.chat] = cached
                        peer_cache.move_to_end(event.chat)
                        if len(peer_cache) > 64:
                            peer_cache.popitem(last=False)
                    if cached[1]:
                        event = replace(event, chat_known=True, username=cached[2], paid_chat=event.paid_chat or cached[3])
                if gateway is not None and (accepting is None or accepting.is_set()) and (
                    event is None or event.edited_at is None
                ):
                    await gateway.observe(
                        message.chat_id, message.thread_id or 0, message.message_id, message.outgoing,
                    )
                if memory is not None and event is not None:
                    incoming = await memory.record(event)
                    if incoming and planner is not None and (accepting is None or accepting.is_set()):
                        planner.offer(event)
                LOG.info(
                    "telegram_message_observed chat_id=%s message_id=%s thread_id=%s direction=%s",
                    message.chat_id,
                    message.message_id,
                    message.thread_id,
                    "outgoing" if message.outgoing else "incoming",
                )
        finally:
            queue.task_done()
            if idle is not None and queue.empty():
                idle.set()


async def listen(
    client: TelegramClient, account_id: int, accepting: asyncio.Event,
    status: WorkerStatus | None = None,
    invalidate: Callable[[], Awaitable[None]] | None = None,
) -> None:
    status = status or WorkerStatus()
    retries = 0
    clock = asyncio.get_running_loop().time
    while True:
        connected_at = None
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT):
                await client.set_receive_updates(False)
                await client.connect()
                await verify_account(client, account_id)
                accepting.set()
                await client.set_receive_updates(True)
            connected_at = clock()
            status.set("connected")
            LOG.info("telegram_connected")
            await client.run_until_disconnected()
            reason = "disconnected"
        except TEMPORARY_ERRORS as error:
            reason = type(error).__name__
        except INVALID_AUTH:
            status.set("stopped", "session_unauthorized")
            LOG.error("telegram_worker_stopped reason=session_unauthorized")
            return
        except SessionError as error:
            status.set("stopped", str(error))
            LOG.error("telegram_worker_stopped reason=%s", error)
            return
        except errors.FloodWaitError as error:
            reason = "flood_wait"
            flood_delay = error.seconds
        except errors.RPCError as error:
            status.set("stopped", type(error).__name__)
            LOG.error("telegram_worker_stopped reason=%s", type(error).__name__)
            return
        finally:
            accepting.clear()
            if status.state == "connected":
                status.set("reconnecting")
            await client.disconnect()
            if invalidate is not None:
                await invalidate()

        if status.state == "stopped":
            return
        if connected_at is not None and clock() - connected_at >= STABLE_CONNECTION_SECONDS:
            retries = 0
        delay = RECONNECT_DELAYS[retries]
        if reason == "flood_wait":
            delay = max(delay, flood_delay)
        retries = min(retries + 1, len(RECONNECT_DELAYS) - 1)
        status.set("reconnecting", reason)
        LOG.warning("telegram_reconnect reason=%s delay=%s", reason, delay)
        await asyncio.sleep(delay)


async def dispatch_outbound(client: TelegramClient, gateway: OutboundGateway, status: WorkerStatus) -> None:
    while True:
        if gateway.ready():
            outcome = await gateway.dispatch_once()
            if outcome == "session_unauthorized":
                status.set("stopped", outcome)
                await client.disconnect()
                return
        await asyncio.sleep(1)


async def maintain_memory(memory: Memory, days: int) -> None:
    while True:
        await memory.maintain(days, int(time.time()))
        await asyncio.sleep(30)


async def run_worker(
    path: Path, database_path: Path, credentials: Credentials, status: WorkerStatus | None = None,
    *, dry_run: bool = True, timezone: str = "Europe/Kyiv", send_lock: asyncio.Lock | None = None,
    settings: Settings | None = None, router: OpenRouter | None = None,
) -> None:
    status = status or WorkerStatus()
    status.set("connecting")
    try:
        path = session_file(path)
        check_session_file(path)
        with lock_session_directory(path.parent):
            check_session_file(path)
            uri = database_path.resolve().as_uri() + "?mode=ro"
            async with (
                aiosqlite.connect(uri, uri=True, timeout=5) as db,
                aiosqlite.connect(database_path, timeout=5) as send_db,
                aiosqlite.connect(uri, uri=True, timeout=5) as plan_db,
            ):
                await send_db.execute("PRAGMA foreign_keys = ON")
                queue: asyncio.Queue[ObservedMessage | DeletedMessages] = asyncio.Queue(maxsize=EVENT_QUEUE_SIZE)
                accepting = asyncio.Event()
                idle = asyncio.Event()
                idle.set()
                overflow = asyncio.Event()
                client = make_client(path, credentials, request_retries=0)
                gateway = OutboundGateway(
                    send_db, credentials.account_id, TelethonOutbound(client), dry_run=dry_run,
                    zone=ZoneInfo(timezone), lock=send_lock or asyncio.Lock(),
                    ready=lambda: accepting.is_set() and idle.is_set() and not overflow.is_set()
                    and status.state == "connected",
                )
                settings = settings or Settings(_env_file=None)
                memory = Memory(send_db, credentials.account_id, gateway.lock, settings.opportunity_weights)
                planner = None if router is None else ParticipationPlanner(
                    plan_db, settings, credentials.account_id, router, gateway,
                )
                await gateway.recover()
                attach_reader(client, queue, accepting, idle, overflow)
                async with asyncio.TaskGroup() as tasks:
                    background = [
                        tasks.create_task(observe_events(db, credentials.account_id, queue, gateway, idle,
                                                         accepting, memory, planner, client)),
                        tasks.create_task(maintain_memory(memory, settings.memory_retention_days)),
                    ]
                    if not dry_run:
                        background.append(tasks.create_task(dispatch_outbound(client, gateway, status)))
                    if planner is not None:
                        background.append(tasks.create_task(planner.run()))
                    if router is not None:
                        background.append(tasks.create_task(run_mining(database_path, credentials.account_id, router)))
                        background.append(tasks.create_task(run_insights(database_path, credentials.account_id, router)))
                    try:
                        await listen(client, credentials.account_id, accepting, status, gateway.invalidate)
                    finally:
                        for task in background:
                            task.cancel()
    except SessionError as error:
        status.set("stopped", str(error))
        LOG.error("telegram_worker_stopped reason=%s", error)
    except (OSError, sqlite3.Error) as error:
        status.set("stopped", type(error).__name__)
        LOG.error("telegram_worker_stopped reason=%s", type(error).__name__)
