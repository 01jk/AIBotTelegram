import hashlib
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import aiosqlite

from .topics import keywords

CATEGORIES = (
    ("telegram", re.compile(r"(?i)\b(?:telegram|телеграм\w*|бот\w*|bot\w*)\b"), "Интеграция Telegram", "Python, Telegram API"),
    ("automation", re.compile(r"(?i)\b(?:автоматиз\w*|парс\w*|інтеграц\w*|интеграц\w*|api)\b"), "Автоматизация процесса", "Python, API, анализ процесса"),
    ("funding", re.compile(r"(?i)\b(?:грант\w*|конкурс\w*|хакатон\w*)\b"), "Конкурс или грант", "Проверка условий и подготовка заявки"),
    ("development", re.compile(r"(?i)\b(?:разработ\w*|розроб\w*|python|програм\w*|сайт\w*)\b"), "Запрос на разработку", "Python, оценка требований"),
)
DEMAND = re.compile(r"(?i)\b(?:ищ\w*|шука\w*|нуж\w*|треба|потріб\w*|заказ\w*|замов\w*|найм\w*|підряд\w*|бюджет|дедлайн|грант\w*)\b")
EXCLUDED_MARKET = re.compile(r"(?i)₽|\b(?:рубл\w*|руб\.?|rub|росси\w*|москв\w*|рф)\b")
BUDGET = re.compile(r"(?i)\b(?:бюджет|budget)\s*[:=]?\s*([0-9][0-9 .]{0,11})\s*(UAH|USD|EUR|грн)\b")
DEADLINE = re.compile(r"(?i)\b(?:срок|термін|дедлайн|deadline)\s*[:=]?\s*((?:до\s+)?[0-9]{1,2}[./][0-9]{1,2}(?:[./][0-9]{2,4})?)")
STATES = {"new", "seen", "interesting", "rejected", "in_progress"}


def source_link(chat: int, thread: int, message: int, username: str | None) -> str | None:
    if username and re.fullmatch(r"[A-Za-z0-9_]{4,32}", username):
        base = f"https://t.me/{username}"
    elif chat < -1000000000000:
        base = f"https://t.me/c/{-chat - 1000000000000}"
    else:
        return None
    return f"{base}/{thread}/{message}" if thread > 1 else f"{base}/{message}"


async def capture(
    db: aiosqlite.Connection, account: int, row: int, text: str, timestamp: int,
    link: str | None, weights: tuple[int, int, int, int, int],
) -> None:
    if not DEMAND.search(text) or EXCLUDED_MARKET.search(text):
        return
    match = next((item for item in CATEGORIES if item[1].search(text)), None)
    if match is None:
        return
    category, _, need, skills = match
    terms = sorted(set(keywords(text)))[:8]
    fingerprint = hashlib.sha256((" ".join([category, *terms])).encode("utf-8")).hexdigest()
    factors = (85, 75, 100, 70, 75) if BUDGET.search(text) else (85, 60, 100, 65, 70)
    score = sum(weight * factor for weight, factor in zip(weights, factors)) // 100
    details = json.dumps({"need": need, "skills": skills, "factors": factors}, ensure_ascii=False)
    [(identifier,)] = await db.execute_fetchall(
        """
        INSERT INTO opportunities (account_id, fingerprint, category, score, details, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_id, fingerprint) DO UPDATE SET
            assessed_at = CASE WHEN excluded.updated_at > opportunities.updated_at THEN NULL ELSE assessed_at END,
            model_confidence = CASE WHEN excluded.updated_at > opportunities.updated_at THEN NULL ELSE model_confidence END,
            updated_at = MAX(opportunities.updated_at, excluded.updated_at)
        RETURNING id
        """, (account, fingerprint, category, score, details, timestamp, timestamp),
    )
    await db.execute("INSERT OR IGNORE INTO opportunity_sources VALUES (?, ?, ?, ?)", (identifier, row, link, score))


async def clean_sources(db: aiosqlite.Connection, account: int) -> None:
    await db.execute(
        """
        DELETE FROM opportunities WHERE account_id = ?
        AND NOT EXISTS (SELECT 1 FROM opportunity_sources WHERE opportunity_id = opportunities.id)
        """, (account,),
    )


async def format_opportunities(
    db: aiosqlite.Connection, account: int, now: int, *, digest: bool = False,
) -> str:
    since = now - (86400 if digest else 7 * 86400)
    rows = await db.execute_fetchall(
        """
        SELECT o.id, s.score, o.details, o.state, s.link, m.text, m.chat_id, m.message_id, m.sent_at,
               (SELECT COUNT(*) FROM opportunity_sources WHERE opportunity_id = o.id), o.model_confidence
        FROM opportunities o
        JOIN opportunity_sources s ON s.message_row = (
            SELECT os.message_row FROM opportunity_sources os JOIN messages mm ON mm.id = os.message_row
            JOIN chats cc ON cc.account_id = mm.account_id AND cc.chat_id = mm.chat_id AND cc.mode != 'DENY'
            WHERE os.opportunity_id = o.id AND mm.sent_at >= ?
            ORDER BY mm.sent_at DESC, mm.id DESC LIMIT 1
        )
        JOIN messages m ON m.id = s.message_row
        WHERE o.account_id = ? AND s.opportunity_id = o.id AND o.state != 'rejected'
        ORDER BY s.score DESC, m.sent_at DESC LIMIT 5
        """, (since, account),
    )
    cards = []
    for ident, score, details, state, link, text, chat, message, sent_at, sources, confidence in rows:
        info = json.loads(details)
        budget = BUDGET.search(text)
        deadline = DEADLINE.search(text)
        money = f"{budget[1].strip()} {budget[2].upper()}" if budget else "не указан"
        due = deadline[1] if deadline else "не указан"
        date = datetime.fromtimestamp(sent_at, ZoneInfo("Europe/Kyiv")).strftime("%d.%m")
        source = link or f"чат {chat}, сообщение {message}"
        cards.append(
            f"№{ident} · {info['need']} · {score}/100 · {state}\n"
            f"Сигнал от автора публикации. Навыки: {info['skills']}. "
            f"Бюджет: {money}. Срок: {due}. Источников: {sources}.\n"
            f"Первый шаг: уточнить требования и трудоёмкость. Затраты и конкуренция пока неизвестны. "
            f"Уверенность: {f'модель {confidence}%' if confidence is not None else 'предварительная'}. "
            f"Источник {date}: {source}"
        )
    if not cards:
        return ""
    prefix = "Новые и обновлённые возможности за сутки" if digest else "Возможности за последние 7 дней"
    return prefix + "\n\n" + "\n\n".join(cards) + "\n\nПриватные ссылки доступны участникам источника."


async def reserve_digest(db: aiosqlite.Connection, account: int, zone: ZoneInfo, schedule: str, now: int):
    local = datetime.fromtimestamp(now, zone)
    if local.strftime("%H:%M") < schedule:
        return None
    day = local.date().isoformat()
    existing = await db.execute_fetchall("SELECT 1 FROM digest_runs WHERE account_id = ? AND local_date = ?", (account, day))
    if existing:
        return None
    text = await format_opportunities(db, account, now, digest=True)
    if not text:
        return None
    rows = await db.execute_fetchall(
        "INSERT OR IGNORE INTO digest_runs VALUES (?, ?, 'reserved') RETURNING local_date", (account, day),
    )
    await db.commit()
    return (day, text) if rows else None
