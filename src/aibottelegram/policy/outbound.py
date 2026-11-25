import asyncio
import re
import secrets
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Protocol
from zoneinfo import ZoneInfo

import aiosqlite

from .drafts import save_draft
from .limits import GLOBAL_LIMITS, is_quiet, load_limits, local_day
from .scheduler import ConversationScheduler

MAX_PENDING = 64
TRANSPORT_TIMEOUT = 12
HISTORY_TTL = 7 * 86400
URL = re.compile(r"(?i)(?:[a-z][a-z0-9+.-]*://|www\.|[\w-]+\.[a-z]{2,}(?=/|\b))")
REFERRAL = re.compile(r"(?i)[?&](?:ref|referral|affiliate|ref_code|aff|invite)=")
MARKUP = re.compile(r"(?m)^\s*(?:#{1,6}\s|\x60{3})|\*\*")


@dataclass(frozen=True)
class Candidate:
    chat_id: int
    context_id: int
    text: str
    dedup_key: str
    thread_id: int = 0
    reply_to_id: int | None = None
    proactive: bool = False
    memory_revision: int | None = None


@dataclass(frozen=True)
class Queued:
    id: int
    chat_id: int
    context_id: int
    text: str
    thread_id: int
    reply_to_id: int | None
    proactive: bool
    random_id: int
    expires_at: int
    memory_revision: int | None = None
    approved_draft_id: int | None = None


class TransportRejected(Exception):
    def __init__(self, reason: str, wait: int = 0):
        super().__init__(reason)
        self.reason, self.wait = reason, wait


class OutboundTransport(Protocol):
    async def prepare(self, candidate: Queued) -> object: ...
    async def send(self, request: object) -> int | None: ...


async def cancel_pending(
    db: aiosqlite.Connection, account_id: int, now: int, reason: str, chat_id: int = 0, *,
    automatic_only: bool = False,
) -> None:
    await db.execute(
        """
        UPDATE outbox SET status = 'failed', finished_at = ?, reason = ?, text = ''
        WHERE account_id = ? AND status = 'pending' AND (? = 0 OR chat_id = ?)
        AND (NOT ? OR approved_draft_id IS NULL)
        """,
        (now, reason, account_id, chat_id, chat_id, automatic_only),
    )


async def finish_conversation(db: aiosqlite.Connection, account_id: int, until: int) -> None:
    await db.execute(
        """
        UPDATE outbound_state SET active_chat_id = NULL, active_thread_id = 0,
            switch_after = MAX(switch_after, ?)
        WHERE account_id = ?
        """,
        (until, account_id),
    )


async def initiative_eligible(db: aiosqlite.Connection, account: int, chat: int, now: int) -> bool:
    rows = await db.execute_fetchall(
        """
        SELECT status, telegram_message_id, reserved_at FROM outbox WHERE account_id = ?
        AND chat_id = ? AND proactive = 1 AND status IN ('sending', 'sent', 'unknown')
        ORDER BY id DESC LIMIT 1
        """, (account, chat),
    )
    if not rows:
        return True
    status, message, when = rows[0]
    if status != "sent" or message is None or when is None or now - when < 1800:
        return False
    return bool(await db.execute_fetchall(
        """
        SELECT 1 FROM messages WHERE account_id = ? AND chat_id = ? AND direction = 'incoming'
        AND bot = 0 AND reply_to = ? AND sent_at >= ? LIMIT 1
        """, (account, chat, message, when),
    ))


async def budget_usage(
    db: aiosqlite.Connection, account_id: int, chat_id: int, now: int, zone: ZoneInfo,
) -> tuple[int, int, int, int | None]:
    start, end = local_day(now, zone)
    since = now - 86400
    [row] = await db.execute_fetchall(
        """
        SELECT COUNT(*) FILTER (WHERE reserved_at >= ? AND reserved_at < ?),
            COUNT(*) FILTER (WHERE reserved_at > ?),
            COUNT(*) FILTER (WHERE reserved_at >= ? AND reserved_at < ? AND proactive = 1),
            MAX(reserved_at)
        FROM outbox WHERE account_id = ? AND status IN ('sending', 'sent', 'unknown')
            AND reserved_at >= ? AND (? = 0 OR chat_id = ?)
        """,
        (start, end, since, start, end, account_id, min(start, since), chat_id, chat_id),
    )
    return tuple(row)


class OutboundGateway:
    def __init__(
        self, db: aiosqlite.Connection, account_id: int, transport: OutboundTransport,
        *, dry_run: bool, zone: ZoneInfo, lock: asyncio.Lock,
        clock: Callable[[], int] = lambda: int(time.time()),
        ready: Callable[[], bool] = lambda: True,
    ):
        self.db, self.account_id, self.transport = db, account_id, transport
        self.dry_run, self.zone, self.lock, self.clock, self.ready = dry_run, zone, lock, clock, ready
        self.dispatch_lock = asyncio.Lock()
        self.inflight: tuple[int, asyncio.Event] | None = None

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            await self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
                await self.db.commit()
            except BaseException:
                await self.db.rollback()
                raise

    async def recover(self) -> None:
        async with self.transaction():
            now = self.clock()
            await self.db.execute(
                """
                UPDATE outbox SET status = 'unknown', finished_at = ?, reason = 'interrupted', notify_owner = 1
                WHERE account_id = ? AND status = 'sending'
                """,
                (now, self.account_id),
            )
            await self._invalidate(now)

    async def _invalidate(self, now: int) -> None:
        await self.db.execute("DELETE FROM outbound_context WHERE account_id = ?", (self.account_id,))
        await cancel_pending(self.db, self.account_id, now, "context_reset")

    async def invalidate(self) -> None:
        async with self.transaction():
            await self._invalidate(self.clock())

    async def observe(self, chat_id: int, thread_id: int, message_id: int, outgoing: bool) -> None:
        inflight = self.inflight
        if outgoing and inflight is not None and chat_id == inflight[0]:
            # The update can arrive before the RPC receipt. Give it time to be recorded.
            await inflight[1].wait()
        async with self.transaction():
            allowed = await self.db.execute_fetchall(
                "SELECT 1 FROM chats WHERE account_id = ? AND chat_id = ? AND mode != 'DENY'",
                (self.account_id, chat_id),
            )
            if not allowed:
                return
            await self.db.execute(
                """
                INSERT INTO outbound_context VALUES (?, ?, ?, ?)
                ON CONFLICT(account_id, chat_id, thread_id) DO UPDATE SET
                    message_id = MAX(outbound_context.message_id, excluded.message_id)
                """,
                (self.account_id, chat_id, thread_id, message_id),
            )
            if not outgoing:
                return
            sent = await self.db.execute_fetchall(
                "SELECT 1 FROM outbox WHERE account_id = ? AND chat_id = ? AND telegram_message_id = ?",
                (self.account_id, chat_id, message_id),
            )
            if sent:
                return
            now = self.clock()
            limits = await load_limits(self.db, self.account_id)
            await self.db.execute(
                """
                INSERT INTO chat_send_state (account_id, chat_id, manual_until) VALUES (?, ?, ?)
                ON CONFLICT(account_id, chat_id) DO UPDATE SET
                    manual_until = MAX(manual_until, excluded.manual_until)
                """,
                (self.account_id, chat_id, now + limits["manual_cooldown"]),
            )
            await cancel_pending(self.db, self.account_id, now, "manual_reply", chat_id)

    async def enqueue(self, candidate: Candidate) -> bool:
        if (
            type(candidate.chat_id) is not int or not -(2**63) <= candidate.chat_id < 2**63
            or candidate.chat_id == 0
            or type(candidate.thread_id) is not int or not 0 <= candidate.thread_id < 2**31
            or type(candidate.context_id) is not int or not 0 < candidate.context_id < 2**31
            or (candidate.reply_to_id is not None and (
                type(candidate.reply_to_id) is not int or not 0 < candidate.reply_to_id < 2**31
            ))
            or type(candidate.proactive) is not bool
            or not isinstance(candidate.dedup_key, str) or not 1 <= len(candidate.dedup_key) <= 128
            or not isinstance(candidate.text, str) or not candidate.text.strip() or len(candidate.text) > 4096
        ):
            return False
        try:
            candidate.text.encode("utf-8")
            candidate.dedup_key.encode("utf-8")
        except UnicodeEncodeError:
            return False
        async with self.transaction():
            now = self.clock()
            await self._maintain(now)
            [(mode, paused)] = await self.db.execute_fetchall(
                "SELECT mode, paused FROM runtime_state WHERE id = 1"
            )
            [(pending,)] = await self.db.execute_fetchall(
                "SELECT COUNT(*) FROM outbox WHERE account_id = ? AND status = 'pending'", (self.account_id,)
            )
            limits = await load_limits(self.db, self.account_id)
            if self.dry_run or paused or mode != "AUTO" or not limits["candidate_ttl"] or pending >= MAX_PENDING:
                return False
            c = candidate
            rows = await self.db.execute_fetchall(
                """
                INSERT INTO outbox (account_id, chat_id, thread_id, dedup_key, random_id,
                    text, context_id, reply_to_id, proactive, created_at, expires_at, memory_revision)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE EXISTS (
                    SELECT 1 FROM chats WHERE account_id = ? AND chat_id = ? AND mode = 'AUTO_REPLY'
                        AND (NOT ? OR proactive_enabled)
                )
                AND NOT EXISTS (
                    SELECT 1 FROM outbox WHERE account_id = ? AND chat_id = ? AND thread_id = ?
                        AND context_id = ? AND text = ? AND status IN ('pending', 'sending', 'sent', 'unknown')
                )
                ON CONFLICT DO NOTHING RETURNING id
                """,
                (
                    self.account_id, c.chat_id, c.thread_id, c.dedup_key, secrets.randbits(63),
                    c.text.strip(), c.context_id, c.reply_to_id, c.proactive, now,
                    now + limits["candidate_ttl"], c.memory_revision, self.account_id, c.chat_id, c.proactive,
                    self.account_id, c.chat_id, c.thread_id, c.context_id, c.text.strip(),
                ),
            )
            return bool(rows)

    async def _maintain(self, now: int) -> None:
        await self.db.execute(
            """
            UPDATE outbox SET status = 'failed', finished_at = ?, reason = 'expired', text = ''
            WHERE account_id = ? AND status = 'pending' AND (
                expires_at <= ? OR created_at <= ? - COALESCE((
                    SELECT value FROM send_limits
                    WHERE account_id = outbox.account_id AND chat_id = 0 AND name = 'candidate_ttl'
                ), ?)
            )
            """,
            (now, self.account_id, now, now, GLOBAL_LIMITS["candidate_ttl"][0]),
        )
        await self.db.execute(
            "DELETE FROM outbox WHERE account_id = ? AND finished_at < ? AND notify_owner = 0",
            (self.account_id, now - HISTORY_TTL),
        )

    async def _decision(self, c: Queued, now: int) -> tuple[str, ConversationScheduler | None]:
        [(mode, paused)] = await self.db.execute_fetchall(
            "SELECT mode, paused FROM runtime_state WHERE id = 1"
        )
        rows = await self.db.execute_fetchall(
            """
            SELECT ch.mode, ch.proactive_enabled, cx.message_id, COALESCE(st.manual_until, 0),
                COALESCE(st.blocked_until, 0)
            FROM chats ch
            LEFT JOIN outbound_context cx ON cx.account_id = ch.account_id AND cx.chat_id = ch.chat_id
                AND cx.thread_id = ?
            LEFT JOIN chat_send_state st ON st.account_id = ch.account_id AND st.chat_id = ch.chat_id
            WHERE ch.account_id = ? AND ch.chat_id = ?
            """,
            (c.thread_id, self.account_id, c.chat_id),
        )
        approved = c.approved_draft_id is not None
        if self.dry_run or paused or mode != ("DRAFT" if approved else "AUTO"):
            return "disabled", None
        if approved and (c.proactive or c.memory_revision is None or c.reply_to_id != c.context_id):
            return "invalid_draft", None
        permissions = {"DRAFT_ONLY", "AUTO_REPLY"} if approved else {"AUTO_REPLY"}
        if not rows or rows[0][0] not in permissions or (c.proactive and not rows[0][1]):
            return "chat_denied", None
        if c.memory_revision is not None:
            verified = await self.db.execute_fetchall(
                """
                SELECT 1 FROM chat_revisions r JOIN chats ch
                ON ch.account_id = r.account_id AND ch.chat_id = r.chat_id
                WHERE r.account_id = ? AND r.chat_id = ? AND r.revision = ?
                AND ch.chat_id < 0 AND ch.external_context = 1
                AND (ch.public_username IS NOT NULL OR ch.external_nonpublic = 1)
                """, (self.account_id, c.chat_id, c.memory_revision),
            )
            if not verified:
                return "stale_context", None
            if c.proactive and not await initiative_eligible(self.db, self.account_id, c.chat_id, now):
                return "initiative_unanswered", None
        _, _, latest, manual_until, chat_block = rows[0]
        if now < manual_until:
            return "manual_reply", None
        if latest != c.context_id:
            return "stale_context", None
        global_limits = await load_limits(self.db, self.account_id)
        chat_limits = await load_limits(self.db, self.account_id, c.chat_id)
        if (
            len(c.text.encode("utf-16-le")) // 2 > global_limits["max_length"]
            or any(char in c.text for char in "\u2013\u2014")
            or MARKUP.search(c.text) or REFERRAL.search(c.text)
            or (not chat_limits["allow_links"] and URL.search(c.text))
        ):
            return "invalid_text", None
        if (
            not self.ready() or now < chat_block
            or is_quiet(now, self.zone, global_limits["quiet_start"], global_limits["quiet_end"])
        ):
            return "wait", None
        state = await self.db.execute_fetchall(
            """
            SELECT active_chat_id, active_thread_id, active_started, active_last, switch_after, blocked_until
            FROM outbound_state WHERE account_id = ?
            """,
            (self.account_id,),
        )
        active, thread, started, last, switch, block = state[0] if state else (None, 0, 0, 0, 0, 0)
        scheduler = ConversationScheduler(active, thread, started, last, switch)
        if now < block or not scheduler.permits(c.chat_id, c.thread_id, now, global_limits):
            return "wait", None
        used, rolling, proactive, last = await budget_usage(self.db, self.account_id, 0, now, self.zone)
        used_chat, rolling_chat, proactive_chat, last_chat = await budget_usage(
            self.db, self.account_id, c.chat_id, now, self.zone,
        )
        if (
            used >= global_limits["global_daily"] or rolling >= global_limits["global_rolling"]
            or used_chat >= chat_limits["daily"] or rolling_chat >= chat_limits["rolling"]
            or (c.proactive and (
                proactive >= global_limits["proactive_daily"] or proactive_chat >= chat_limits["proactive_daily"]
            ))
            or (last is not None and now < last + global_limits["min_interval"])
            or (last_chat is not None and now < last_chat + chat_limits["min_interval"])
        ):
            return "wait", None
        return "ready", scheduler if scheduler.is_active(now, global_limits) else None

    async def _candidate(self, candidate_id: int | None = None) -> Queued | None:
        async with self.transaction():
            now = self.clock()
            await self._maintain(now)
            if await self.db.execute_fetchall(
                "SELECT 1 FROM outbox WHERE account_id = ? AND status = 'sending'", (self.account_id,)
            ):
                return None
            rows = await self.db.execute_fetchall(
                """
                SELECT id, chat_id, context_id, text, thread_id, reply_to_id, proactive, random_id, expires_at,
                    memory_revision, approved_draft_id
                FROM outbox WHERE account_id = ? AND status = 'pending' AND (? IS NULL OR id = ?)
                ORDER BY id LIMIT ?
                """,
                (self.account_id, candidate_id, candidate_id, MAX_PENDING),
            )
            for row in rows:
                candidate = Queued(*row)
                decision, active = await self._decision(candidate, now)
                if decision == "wait":
                    continue
                if decision != "ready":
                    await self._finish(candidate.id, "failed", now, decision)
                    continue
                if candidate_id is not None:
                    await self.db.execute(
                        "UPDATE outbox SET status = 'sending', reserved_at = ? WHERE id = ?", (now, candidate.id)
                    )
                    await self.db.execute(
                        """
                        INSERT INTO outbound_state (account_id, active_chat_id, active_thread_id,
                            active_started, active_last) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(account_id) DO UPDATE SET active_chat_id = excluded.active_chat_id,
                            active_thread_id = excluded.active_thread_id, active_started = excluded.active_started,
                            active_last = excluded.active_last
                        """,
                        (self.account_id, candidate.chat_id, candidate.thread_id, active.started_at if active else now, now),
                    )
                return candidate
        return None

    async def eligible(self, chat: int, thread: int, anchor: int, revision: int, proactive: bool) -> bool:
        placeholder = Queued(0, chat, anchor, "…", thread, None, proactive, 0, 0, revision)
        async with self.lock:
            decision, _ = await self._decision(placeholder, self.clock())
        return decision == "ready"

    async def end_conversation(self, chat: int, thread: int) -> None:
        async with self.transaction():
            active = await self.db.execute_fetchall(
                "SELECT 1 FROM outbound_state WHERE account_id = ? AND active_chat_id = ? AND active_thread_id = ?",
                (self.account_id, chat, thread),
            )
            if active:
                limits = await load_limits(self.db, self.account_id)
                await finish_conversation(self.db, self.account_id, self.clock() + limits["switch_cooldown"])

    async def _finish(
        self, candidate_id: int, status: str, now: int, reason: str | None,
        message_id: int | None = None, notify: bool = False,
    ) -> None:
        await self.db.execute(
            """
            UPDATE outbox SET status = ?, finished_at = ?, reason = ?, telegram_message_id = ?,
                notify_owner = ?, text = CASE WHEN ? = 'failed' THEN '' ELSE text END
            WHERE id = ?
            """,
            (status, now, reason, message_id, notify, status, candidate_id),
        )

    async def _rejected(self, candidate: Queued, error: TransportRejected) -> str:
        async with self.transaction():
            now = self.clock()
            reason = error.reason
            notify = reason in {"peer_flood", "session_unauthorized"}
            if reason == "paid_messages":
                active = await self.db.execute_fetchall(
                    "SELECT 1 FROM outbox WHERE id = ? AND status IN ('pending', 'sending')", (candidate.id,),
                )
                if not active:
                    return reason
                if candidate.approved_draft_id is not None:
                    await self.db.execute(
                        "UPDATE drafts SET paid_chat = 1, state = 'pending' WHERE id = ? AND account_id = ?",
                        (candidate.approved_draft_id, self.account_id),
                    )
                elif candidate.memory_revision is not None:
                    await save_draft(
                        self.db, self.account_id, candidate.chat_id, candidate.thread_id, candidate.context_id,
                        candidate.memory_revision, candidate.text, now, paid_chat=True,
                    )
            elif reason == "flood_wait":
                rows = await self.db.execute_fetchall(
                    "SELECT last_flood_at FROM outbound_state WHERE account_id = ?", (self.account_id,),
                )
                repeated = bool(rows and rows[0][0] is not None and now - rows[0][0] < 86400)
                await self.db.execute(
                    """
                    INSERT INTO outbound_state (account_id, blocked_until, last_flood_at) VALUES (?, ?, ?)
                    ON CONFLICT(account_id) DO UPDATE SET blocked_until = MAX(blocked_until, excluded.blocked_until),
                        last_flood_at = excluded.last_flood_at
                    """,
                    (self.account_id, now + error.wait, now),
                )
                if repeated:
                    reason, notify = "repeated_flood_wait", True
            elif reason == "slow_mode":
                await self.db.execute(
                    """
                    INSERT INTO chat_send_state (account_id, chat_id, blocked_until) VALUES (?, ?, ?)
                    ON CONFLICT(account_id, chat_id) DO UPDATE SET blocked_until = MAX(blocked_until, excluded.blocked_until)
                    """,
                    (self.account_id, candidate.chat_id, now + error.wait),
                )
            elif reason == "write_forbidden":
                await self.db.execute(
                    """
                    UPDATE chats SET mode = 'READ_ONLY', proactive_enabled = 0
                    WHERE account_id = ? AND chat_id = ? AND mode = 'AUTO_REPLY'
                    """,
                    (self.account_id, candidate.chat_id),
                )
                await cancel_pending(self.db, self.account_id, now, reason, candidate.chat_id)
            if reason in {"peer_flood", "repeated_flood_wait"}:
                await self.db.execute("UPDATE runtime_state SET paused = 1, pause_reason = ? WHERE id = 1", (reason,))
                await cancel_pending(self.db, self.account_id, now, reason)
            await self._finish(candidate.id, "failed", now, reason, notify=notify)
            return reason

    async def dispatch_once(self) -> str | None:
        async with self.dispatch_lock:
            candidate = await self._candidate()
            if candidate is None:
                return None
            try:
                async with asyncio.timeout(TRANSPORT_TIMEOUT):
                    request = await self.transport.prepare(candidate)
            except TransportRejected as error:
                return await self._rejected(candidate, error)
            except (TimeoutError, OSError, ValueError):
                return await self._rejected(candidate, TransportRejected("preflight_unavailable"))

            reserved = await self._candidate(candidate.id)
            if reserved is None:
                return None
            if not self.ready():
                return await self._rejected(candidate, TransportRejected("context_changed_during_reservation"))
            completed = asyncio.Event()
            self.inflight = candidate.chat_id, completed
            try:
                try:
                    async with asyncio.timeout(TRANSPORT_TIMEOUT):
                        message_id = await self.transport.send(request)
                except TransportRejected as error:
                    return await self._rejected(candidate, error)
                except (TimeoutError, OSError):
                    message_id = None
                # Cancellation leaves 'sending'; restart conservatively recovers it as 'unknown'.
                async with self.transaction():
                    if message_id is None:
                        await self._finish(candidate.id, "unknown", self.clock(), "delivery_unconfirmed", notify=True)
                        return "unknown"
                    await self._finish(candidate.id, "sent", self.clock(), None, message_id)
                    return "sent"
            finally:
                self.inflight = None
                completed.set()
