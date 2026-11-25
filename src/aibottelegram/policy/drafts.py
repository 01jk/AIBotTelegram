import secrets

import aiosqlite

from ..features.opportunities import source_link
from ..style.profile import classify
from .limits import load_limits

DRAFT_TTL = 600
MAX_DRAFTS = 32
MAX_DRAFT_LENGTH = 3500
DRAFT_PAGE_SIZE = 5


async def can_preview(
    db: aiosqlite.Connection, account: int, chat: int, thread: int, context: int, revision: int, now: int,
    *, paid_chat: bool = False,
) -> bool:
    return bool(await db.execute_fetchall(
        """
        SELECT 1 FROM runtime_state AS rt
        JOIN chats AS c ON c.account_id = ? AND c.chat_id = ?
        JOIN chat_revisions AS r ON r.account_id = c.account_id AND r.chat_id = c.chat_id
        JOIN outbound_context AS ctx ON ctx.account_id = c.account_id AND ctx.chat_id = c.chat_id AND ctx.thread_id = ?
        LEFT JOIN chat_send_state AS st ON st.account_id = c.account_id AND st.chat_id = c.chat_id
        WHERE rt.id = 1 AND (rt.mode = 'DRAFT' OR (? AND rt.mode = 'AUTO' AND c.mode = 'AUTO_REPLY'))
        AND rt.paused = 0
        AND c.mode IN ('DRAFT_ONLY', 'AUTO_REPLY') AND c.chat_id < 0 AND c.external_context = 1
        AND (c.public_username IS NOT NULL OR c.external_nonpublic = 1)
        AND r.revision = ? AND ctx.message_id = ? AND COALESCE(st.manual_until, 0) <= ?
        """, (account, chat, thread, paid_chat, revision, context, now),
    ))


async def save_draft(
    db: aiosqlite.Connection, account: int, chat: int, thread: int, context: int,
    revision: int, text: str, now: int, *, paid_chat: bool = False,
) -> bool:
    if not 1 <= len(text.encode("utf-16-le")) // 2 <= MAX_DRAFT_LENGTH:
        return False
    if not await can_preview(db, account, chat, thread, context, revision, now, paid_chat=paid_chat):
        return False
    rows = await db.execute_fetchall(
        """
        INSERT INTO drafts (account_id, chat_id, thread_id, context_id, memory_revision, text, created_at, expires_at, paid_chat)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING id
        """, (account, chat, thread, context, revision, text, now, now + DRAFT_TTL, paid_chat),
    )
    if not rows:
        return False
    await db.execute(
        """
        DELETE FROM drafts WHERE account_id = ? AND id IN (
            SELECT id FROM drafts WHERE account_id = ? ORDER BY id DESC LIMIT -1 OFFSET ?
        )
        """, (account, account, MAX_DRAFTS),
    )
    return True


async def draft_rows(
    db: aiosqlite.Connection, account: int, identifier: int | None = None, *, page: int = 1,
) -> list:
    return await db.execute_fetchall(
        """
        SELECT d.id, d.chat_id, d.thread_id, d.context_id, d.memory_revision, d.text, d.expires_at, d.state,
               c.public_username, d.paid_chat,
               (SELECT status FROM outbox WHERE approved_draft_id = d.id AND account_id = d.account_id)
        FROM drafts d JOIN chats c ON c.account_id = d.account_id AND c.chat_id = d.chat_id
        WHERE d.account_id = ? AND c.mode != 'DENY' AND (? IS NULL OR d.id = ?)
        ORDER BY d.id DESC LIMIT ? OFFSET ?
        """, (account, identifier, identifier, 1 if identifier is not None else DRAFT_PAGE_SIZE + 1,
              0 if identifier is not None else (page - 1) * DRAFT_PAGE_SIZE),
    )


async def show_drafts(
    db: aiosqlite.Connection, account: int, now: int, identifier: int | None, *, page: int = 1,
) -> str:
    rows = await draft_rows(db, account, identifier, page=page)
    if not rows:
        if identifier is not None:
            return "Черновик не найден."
        return "Черновиков пока нет." if page == 1 else "На этой странице черновиков нет."
    lines = []
    for item, chat, thread, context, revision, text, expires, state, username, paid_chat, send_status in rows[:DRAFT_PAGE_SIZE]:
        if state == "queued":
            label = {
                "sent": "отправлен", "failed": "отправка отменена", "unknown": "доставка неизвестна",
                "pending": "одобрен, в очереди", "sending": "отправляется",
            }.get(send_status, "одобрен")
        elif state == "rejected":
            label = "отклонён"
        elif paid_chat:
            label = "чат с платными сообщениями · только черновик"
        elif expires <= now or not await can_preview(db, account, chat, thread, context, revision, now):
            label = "устарел или недоступен"
        else:
            label = "ждёт решения"
        preview = text if identifier is not None or len(text) <= 300 else text[:300] + "…"
        source = source_link(chat, thread, context, username) or f"№{context}"
        lines.append(f"№{item} · чат {chat} · {label}\n{preview}\nИсходное сообщение: {source}")
    heading = f"Черновики · страница {page}\n\n" if identifier is None else ""
    return (
        heading + "\n\n".join(lines) + "\n\nПросмотр: /drafts ID · Одобрить: /approve ID · Отклонить: /reject ID"
        "\nЗапомнить свой вариант: /prefer ID текст"
    )


async def save_draft_preference(
    db: aiosqlite.Connection, account: int, owner: int, identifier: int, text: str, now: int,
) -> bool:
    rows = await db.execute_fetchall(
        """
        SELECT COALESCE(m.text, d.text)
        FROM drafts d JOIN chats c ON c.account_id = d.account_id AND c.chat_id = d.chat_id
        LEFT JOIN messages m ON m.account_id = d.account_id AND m.chat_id = d.chat_id
            AND m.thread_id = d.thread_id AND m.message_id = d.context_id
        WHERE d.account_id = ? AND d.id = ? AND c.mode != 'DENY'
        """, (account, identifier),
    )
    if not rows:
        return False
    [(source,)] = rows
    language, situation, _ = classify(source)
    await db.execute(
        """
        INSERT INTO style_preferences (owner_id, draft_id, language, situation, text, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(owner_id, draft_id) DO UPDATE SET
            language = excluded.language, situation = excluded.situation,
            text = excluded.text, updated_at = excluded.updated_at
        """, (owner, identifier, language, situation, text, now),
    )
    return True


async def approve_draft(
    db: aiosqlite.Connection, account: int, identifier: int, now: int, *, dry_run: bool, max_pending: int,
) -> str:
    if dry_run:
        return "DRY_RUN включён. Черновик можно посмотреть, отправка выключена."
    rows = await db.execute_fetchall(
        """
        SELECT chat_id, thread_id, context_id, memory_revision, text, expires_at, state, paid_chat
        FROM drafts WHERE account_id = ? AND id = ?
        """, (account, identifier),
    )
    if not rows:
        return "Черновик не найден."
    chat, thread, context, revision, text, expires, state, paid_chat = rows[0]
    if paid_chat:
        return "В чате платные сообщения. Этот черновик можно скопировать и отправить вручную."
    if state != "pending":
        return "Этот черновик уже одобрен или отклонён."
    if expires <= now or not await can_preview(db, account, chat, thread, context, revision, now):
        return "Черновик устарел или отправка сейчас недоступна. Проверь /mode, /chats и /status."
    global_limits = await load_limits(db, account)
    local = await load_limits(db, account, chat)
    if not all((global_limits["global_daily"], global_limits["global_rolling"], local["daily"], local["rolling"])):
        return "Бюджет отправки равен нулю. Проверь /limits."
    expires = min(expires, now + global_limits["candidate_ttl"])
    if expires <= now:
        return "TTL отправки равен нулю. Проверь /limits."
    [(pending,)] = await db.execute_fetchall(
        "SELECT COUNT(*) FROM outbox WHERE account_id = ? AND status = 'pending' AND expires_at > ?",
        (account, now),
    )
    if pending >= max_pending:
        return "Очередь заполнена. Черновик пока не одобрен."
    rows = await db.execute_fetchall(
        """
        INSERT INTO outbox (account_id, chat_id, thread_id, dedup_key, random_id, text, context_id,
            reply_to_id, proactive, created_at, expires_at, memory_revision, approved_draft_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING RETURNING id
        """, (account, chat, thread, f"approved-draft:{identifier}", secrets.randbits(63), text,
              context, context, now, expires, revision, identifier),
    )
    if not rows:
        return "Черновик не добавлен в очередь. Повтори команду, если он ещё актуален."
    await db.execute("UPDATE drafts SET state = 'queued' WHERE account_id = ? AND id = ?", (account, identifier))
    return f"Черновик №{identifier} одобрен и поставлен в очередь. Шлюз ещё проверит отправку."


async def reject_draft(db: aiosqlite.Connection, account: int, identifier: int) -> str:
    rows = await db.execute_fetchall(
        "UPDATE drafts SET state = 'rejected' WHERE account_id = ? AND id = ? AND state = 'pending' RETURNING id",
        (account, identifier),
    )
    return f"Черновик №{identifier} отклонён." if rows else "Черновик не найден или уже обработан."
