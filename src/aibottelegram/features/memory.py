import asyncio
import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

import aiosqlite

from ..policy.outbound import cancel_pending
from .opportunities import capture, clean_sources, source_link
from .topics import TopicManager, keywords


@dataclass(frozen=True, slots=True)
class MemoryEvent:
    chat: int
    thread: int
    message: int
    sender: int | None
    timestamp: int
    text: str
    reply_to: int | None = None
    edited_at: int | None = None
    outgoing: bool = False
    bot: bool = False
    username: str | None = None
    chat_known: bool = False
    addressed: bool = False
    paid_chat: bool = False


async def bump_revision(db: aiosqlite.Connection, account: int, chat: int) -> None:
    await db.execute(
        """
        INSERT INTO chat_revisions (account_id, chat_id, revision) VALUES (?, ?, 1)
        ON CONFLICT(account_id, chat_id) DO UPDATE SET revision = revision + 1
        """, (account, chat),
    )


async def external_current(db: aiosqlite.Connection, account: int, chat: int, revision: int) -> bool:
    return bool(await db.execute_fetchall(
        """
        SELECT 1 FROM chat_revisions r JOIN chats c ON c.account_id = r.account_id AND c.chat_id = r.chat_id
        WHERE r.account_id = ? AND r.chat_id = ? AND r.revision = ? AND c.mode != 'DENY'
        AND c.external_context = 1 AND c.public_username IS NOT NULL
        """, (account, chat, revision),
    ))


async def forget_chat(db: aiosqlite.Connection, account: int, chat: int, now: int) -> None:
    await bump_revision(db, account, chat)
    await db.execute("UPDATE chat_revisions SET forgotten_before = ? WHERE account_id = ? AND chat_id = ?",
                     (now, account, chat))
    await cancel_pending(db, account, now, "memory_forgotten", chat)
    await db.execute("DELETE FROM outbound_context WHERE account_id = ? AND chat_id = ?", (account, chat))
    await db.execute("DELETE FROM drafts WHERE account_id = ? AND chat_id = ?", (account, chat))
    await db.execute(
        "UPDATE outbox SET text = '', memory_recorded = 1 WHERE account_id = ? AND chat_id = ?", (account, chat),
    )
    await db.execute(
        """
        UPDATE opportunities SET model_confidence = NULL, assessed_at = NULL WHERE id IN (
            SELECT s.opportunity_id FROM opportunity_sources s JOIN messages m ON m.id = s.message_row
            WHERE m.account_id = ? AND m.chat_id = ?
        )
        """, (account, chat),
    )
    await db.execute("DELETE FROM messages WHERE account_id = ? AND chat_id = ?", (account, chat))
    await db.execute("DELETE FROM topics WHERE account_id = ? AND chat_id = ?", (account, chat))
    await db.execute("DELETE FROM memory_tombstones WHERE account_id = ? AND chat_id = ?", (account, chat))
    await clean_sources(db, account)


class Memory:
    def __init__(self, db: aiosqlite.Connection, account: int, lock: asyncio.Lock,
                 weights: tuple[int, int, int, int, int]):
        self.db, self.account, self.lock, self.weights = db, account, lock, weights
        self.topics = TopicManager(db)

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

    async def record(self, event: MemoryEvent) -> bool:
        async with self.transaction():
            return await self._record(event)

    async def _record(self, event: MemoryEvent, *, confirmed_auto: bool = False) -> bool:
        account, db = self.account, self.db
        allowed = await db.execute_fetchall(
            "SELECT 1 FROM chats WHERE account_id = ? AND chat_id = ? AND mode != 'DENY'",
            (account, event.chat),
        )
        if not allowed:
            return False
        if await db.execute_fetchall(
            """
            SELECT 1 FROM chat_revisions WHERE account_id = ? AND chat_id = ? AND forgotten_before >= ?
            UNION ALL SELECT 1 FROM memory_tombstones WHERE account_id = ? AND chat_id = ? AND message_id = ?
            """, (account, event.chat, event.timestamp, account, event.chat, event.message),
        ):
            return False
        if event.chat_known:
            await db.execute(
                "UPDATE chats SET public_username = ? WHERE account_id = ? AND chat_id = ?",
                (event.username, account, event.chat),
            )
        previous = await db.execute_fetchall(
            "SELECT id, edited_at FROM messages WHERE account_id = ? AND chat_id = ? AND message_id = ?",
            (account, event.chat, event.message),
        )
        if previous and (event.edited_at is None or (previous[0][1] or 0) >= event.edited_at):
            return False
        if event.edited_at is not None and not previous:
            return False
        if event.outgoing and not confirmed_auto:
            confirmed_auto = bool(await db.execute_fetchall(
                "SELECT 1 FROM outbox WHERE account_id = ? AND chat_id = ? AND telegram_message_id = ? AND status = 'sent'",
                (account, event.chat, event.message),
            ))
        uncertain = False
        if event.outgoing and not confirmed_auto:
            uncertain = bool(await db.execute_fetchall(
                """
                SELECT 1 FROM outbox WHERE account_id = ? AND chat_id = ? AND thread_id = ?
                AND status IN ('unknown', 'sending') AND text = ? AND reserved_at BETWEEN ? AND ?
                """, (account, event.chat, event.thread, event.text, event.timestamp - 120, event.timestamp + 30),
            ))
        origin = "account_auto" if confirmed_auto or uncertain else "account_manual" if event.outgoing else "human"
        affected = []
        if previous:
            row = previous[0][0]
            affected = await db.execute_fetchall("SELECT topic_id FROM topic_messages WHERE message_row = ?", (row,))
            await db.execute("DELETE FROM topic_messages WHERE message_row = ?", (row,))
            await self._invalidate_card(row)
            await db.execute("DELETE FROM opportunity_sources WHERE message_row = ?", (row,))
            await db.execute("UPDATE messages SET text = ?, edited_at = ? WHERE id = ?",
                             (event.text[:4096], event.edited_at, row))
            await db.execute(
                "UPDATE outbox SET text = '', memory_recorded = 1 WHERE account_id = ? AND chat_id = ? AND telegram_message_id = ?",
                (account, event.chat, event.message),
            )
        else:
            [(row,)] = await db.execute_fetchall(
                """
                INSERT INTO messages (account_id, chat_id, thread_id, message_id, sender_id, sent_at, text,
                    reply_to, edited_at, direction, origin, bot, public, delivery_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id
                """, (account, event.chat, event.thread, event.message, event.sender, event.timestamp,
                      event.text[:4096], event.reply_to, event.edited_at,
                      "outgoing" if event.outgoing else "incoming", origin, event.bot, bool(event.username),
                      "unconfirmed" if uncertain else "confirmed"),
            )
        if uncertain:
            await bump_revision(db, account, event.chat)
            return False
        topic = await self.topics.assign(
            account, event.chat, event.thread, row, event.text[:4096], event.reply_to, event.timestamp,
        )
        for identifier in {topic, *(value for (value,) in affected)}:
            await self.topics.rebuild(identifier)
        if origin == "human" and not event.bot:
            await capture(
                db, account, row, event.text[:4096], event.timestamp,
                source_link(event.chat, event.thread, event.message, event.username), self.weights,
            )
        if previous:
            await clean_sources(db, account)
        await bump_revision(db, account, event.chat)
        return not previous and origin == "human" and not event.bot

    async def delete(self, chat: int | None, message_ids: tuple[int, ...]) -> None:
        async with self.transaction():
            allowed = chat is not None and bool(await self.db.execute_fetchall(
                "SELECT 1 FROM chats WHERE account_id = ? AND chat_id = ? AND mode != 'DENY'",
                (self.account, chat),
            ))
            rows = []
            for message in message_ids[:100]:
                if chat is not None:
                    where, arguments = "chat_id = ?", (chat,)
                else:
                    # MTProto omits the peer for deletes outside channels.
                    where, arguments = "chat_id > -1000000000000", ()
                found = await self.db.execute_fetchall(
                    f"SELECT id, chat_id, message_id FROM messages WHERE account_id = ? AND message_id = ? AND {where} LIMIT 2",
                    (self.account, message, *arguments),
                )
                if len(found) == 1:
                    rows.append(found[0])
                peer = found[0][1] if len(found) == 1 else chat if allowed else None
                if peer is not None:
                    await self.db.execute(
                        "INSERT OR IGNORE INTO memory_tombstones VALUES (?, ?, ?, ?)",
                        (self.account, peer, message, int(time.time())),
                    )
                if not found and chat is not None:
                    await self.db.execute(
                        "UPDATE outbox SET text = '', memory_recorded = 1 WHERE account_id = ? AND chat_id = ? AND telegram_message_id = ?",
                        (self.account, chat, message),
                    )
            await self._delete_rows(rows)

    async def _invalidate_card(self, row: int) -> None:
        await self.db.execute(
            """
            UPDATE opportunities SET model_confidence = NULL, assessed_at = NULL WHERE id IN (
                SELECT opportunity_id FROM opportunity_sources WHERE message_row = ?
            )
            """, (row,),
        )

    async def _delete_rows(self, rows: list[tuple[int, int, int]]) -> None:
        if not rows:
            return
        topics, chats = set(), set()
        for row, chat, message in rows:
            chats.add(chat)
            topics.update(identifier for (identifier,) in await self.db.execute_fetchall(
                "SELECT topic_id FROM topic_messages WHERE message_row = ?", (row,),
            ))
            await self._invalidate_card(row)
            await self.db.execute("DELETE FROM messages WHERE id = ?", (row,))
            await self.db.execute(
                "UPDATE outbox SET text = '', memory_recorded = 1 WHERE account_id = ? AND chat_id = ? AND telegram_message_id = ?",
                (self.account, chat, message),
            )
        for identifier in topics:
            await self.topics.rebuild(identifier)
        for chat in chats:
            await bump_revision(self.db, self.account, chat)
            await cancel_pending(self.db, self.account, int(time.time()), "memory_changed", chat)
        await clean_sources(self.db, self.account)

    async def maintain(self, retention: int, now: int) -> None:
        async with self.transaction():
            rows = await self.db.execute_fetchall(
                """
                SELECT id, chat_id, thread_id, telegram_message_id, text, finished_at, reply_to_id FROM outbox
                WHERE account_id = ? AND status = 'sent' AND memory_recorded = 0 ORDER BY id LIMIT 20
                """, (self.account,),
            )
            for row, chat, thread, message, text, timestamp, reply_to in rows:
                if message and text and timestamp:
                    await self._record(
                        MemoryEvent(chat, thread, message, self.account, timestamp, text, reply_to, outgoing=True),
                        confirmed_auto=True,
                    )
                await self.db.execute("UPDATE outbox SET memory_recorded = 1 WHERE id = ?", (row,))
            old = await self.db.execute_fetchall(
                """
                SELECT id, chat_id, message_id FROM messages WHERE account_id = ? AND sent_at < ?
                ORDER BY sent_at LIMIT 200
                """, (self.account, now - retention * 86400),
            )
            await self._delete_rows(old)
            await self.db.execute(
                "DELETE FROM topics WHERE account_id = ? AND status = 'closed' AND last_at < ?",
                (self.account, now - retention * 86400),
            )
            await self.db.execute("DELETE FROM memory_tombstones WHERE account_id = ? AND deleted_at < ?",
                                  (self.account, now - retention * 86400))
            await self.db.execute("DELETE FROM drafts WHERE account_id = ? AND created_at < ?",
                                  (self.account, now - retention * 86400))


async def get_context(db: aiosqlite.Connection, account: int, chat: int, thread: int, anchor: int) -> dict | None:
    scope = (account, chat, thread)
    events = await db.execute_fetchall(
        """
        SELECT m.message_id, m.sender_id, m.text, m.reply_to, m.direction, m.origin, m.bot,
               m.sent_at, m.public, tm.topic_id
        FROM messages m LEFT JOIN topic_messages tm ON tm.message_row = m.id
        WHERE m.account_id = ? AND m.chat_id = ? AND m.thread_id = ? AND m.delivery_status = 'confirmed'
        ORDER BY m.message_id DESC LIMIT 8
        """, scope,
    )
    if not events or events[0][0] != anchor:
        return None
    head = events[0]
    [(revision, external, username, nonpublic)] = await db.execute_fetchall(
        """
        SELECT COALESCE(r.revision, 0), c.external_context, c.public_username, c.external_nonpublic FROM chats c
        LEFT JOIN chat_revisions r ON r.account_id = c.account_id AND r.chat_id = c.chat_id
        WHERE c.account_id = ? AND c.chat_id = ?
        """, (account, chat),
    )
    query = " OR ".join(f'"{word}"' for word in keywords(head[2])[:6])
    related = await db.execute_fetchall(
        """
        SELECT m.message_id, m.text, m.origin, m.public FROM message_search
        JOIN messages m ON m.id = message_search.rowid WHERE message_search MATCH ?
        AND m.account_id = ? AND m.chat_id = ? AND m.thread_id = ? AND m.message_id != ?
        AND m.delivery_status = 'confirmed'
        ORDER BY rank LIMIT 6
        """, (query, *scope, anchor),
    ) if query else []
    chain = []
    next_reply = head[3]
    for _ in range(4):
        if not next_reply:
            break
        parent = await db.execute_fetchall(
            """
            SELECT message_id, sender_id, text, reply_to, direction, origin, bot, sent_at, public
            FROM messages WHERE account_id = ? AND chat_id = ? AND thread_id = ? AND message_id = ?
                AND delivery_status = 'confirmed'
            """, (*scope, next_reply),
        )
        if not parent:
            break
        chain.append(parent[0])
        next_reply = parent[0][3]
    summary = await db.execute_fetchall(
        "SELECT summary, model_summary, analysis_kind FROM chat_summaries WHERE topic_id = ?", (head[-1],),
    )
    return {
        "head": head, "recent": events, "chain": chain, "related": related,
        "summary": json.loads(summary[0][0]) if summary else [],
        "public_summary": summary[0][1] if summary else None,
        "topic_kind": summary[0][2] if summary else None,
        "revision": revision, "external": bool(chat < 0 and external and (nonpublic or (username and head[8]))),
        "external_nonpublic": bool(nonpublic),
    }
