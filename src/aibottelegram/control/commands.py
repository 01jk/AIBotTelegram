import asyncio
import re
import time
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

import aiosqlite
from pydantic import SecretStr

from ..config import Settings
from ..features.memory import bump_revision, forget_chat
from ..features.opportunities import STATES, format_opportunities
from ..llm.drafting import SENSITIVE_PATTERN
from ..llm.router import OpenRouter, reset_provider_quota
from ..policy.drafts import approve_draft, reject_draft, save_draft_preference, show_drafts
from ..policy.limits import format_limit, load_limits, parse_limit
from ..policy.outbound import MAX_PENDING, budget_usage, cancel_pending, finish_conversation
from ..policy.scheduler import ConversationScheduler
from ..style.runtime import style_issue, style_status
from ..telegram.worker import WorkerStatus
from .menu import ADD_CHAT, MENU_TEXT, PREFER, callback_action, reply_action

UPDATE_TTL = 48 * 3600
PAGE_SIZE = 30
CHAT_MODES = {"read_only", "draft_only", "auto_reply"}
CHAT_LINK = re.compile(r"(?:@|(?:https?://)?t\.me/)([A-Za-z][A-Za-z0-9_]{0,31})/?", re.I | re.ASCII)
ChatResolver = Callable[[str], Awaitable[tuple[int, str] | None]]
MODE_DESCRIPTIONS = {
    "OBSERVE": "Читает разрешённые чаты, пополняет память и ищет возможности. Личный аккаунт молчит.",
    "DRAFT": "Готовит черновики ответов в разрешённых группах с /external CHAT_ID on. "
             "/drafts показывает черновики. Для отправки нужны /approve ID, DRY_RUN=false и доступный бюджет.",
    "AUTO": "Может сам отвечать и задавать вопросы. Для отправки в группу нужны AUTO_REPLY, "
            "/external CHAT_ID on, DRY_RUN=false, снятая пауза и доступный бюджет.",
}
CHAT_HELP = (
    "Права отдельного чата\n"
    "READ_ONLY — чтение и память. Это значение по умолчанию.\n"
    "DRAFT_ONLY — чтение, черновики и отправка по /approve ID в режиме DRAFT.\n"
    "AUTO_REPLY — права DRAFT_ONLY и автоматические ответы в режиме AUTO. "
    "Ключ proactive отдельно разрешает инициативные сообщения.\n"
    "DENY — блокирует чтение новых событий и отправку."
)
MODE_HELP = (
    "Общий режим бота\n"
    + "\n".join(f"{mode} — {description}" for mode, description in MODE_DESCRIPTIONS.items())
    + "\nПауза действует отдельно. /resume снимает паузу и сохраняет режим."
)
HELP = (
    "/menu — открыть меню с кнопками\n"
    "/status — состояние\n"
    "/stop или /pause — остановить генерацию и отправки. /resume — продолжить\n"
    "/mode observe|draft|auto — режим\n"
    "/login — подключить личный аккаунт через QR\n"
    "/openrouter_key — заменить ключ OpenRouter\n"
    "/drafts [ID] — посмотреть черновики\n"
    "/approve ID и /reject ID — одобрить или отклонить черновик\n"
    "/prefer ID текст — учесть свой вариант в следующих ответах OpenRouter\n"
    "/chats [страница] — разрешения чатов\n"
    "/allow CHAT_ID [read_only|draft_only|auto_reply [proactive]] — разрешить чат\n"
    "/limits [chat CHAT_ID] [параметр значение] — лимиты отправки\n"
    "/style — локальный профиль стиля\n"
    "/models и /providers — модели, провайдеры и квоты\n"
    "/reasoning [on|off] — рассуждения DeepSeek при написании ответа\n"
    "/reset_quota — сбросить локальные счётчики OpenRouter\n"
    "/external CHAT_ID on|off — отфильтрованный контекст группы для OpenRouter\n"
    "/forget CHAT_ID — удалить локальную память чата\n"
    "/opportunities и /digest — возможности и дайджест\n"
    "/deny CHAT_ID — запретить чат\n\n"
    "Вместо CHAT_ID можно указать @username или публичную ссылку https://t.me/username.\n\n"
    + MODE_HELP + "\n\n" + CHAT_HELP
)


class ControlAction(Enum):
    LOGIN = "login"
    OPENROUTER_KEY = "openrouter_key"


FORWARDED = {
    "forward_origin", "forward_from", "forward_from_chat", "forward_sender_name",
    "forward_date", "is_automatic_forward", "via_bot", "sender_chat",
}


class ControlConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ControlConfig:
    token: SecretStr
    owner_id: int
    account_id: int
    dry_run: bool
    timezone: str = "Europe/Kyiv"
    digest_enabled: bool = True
    digest_time: str = "19:00"


def control_config(settings: Settings) -> ControlConfig | None:
    token = settings.control_bot_token
    if token is None or not token.get_secret_value().strip():
        return None
    if not re.fullmatch(r"[1-9][0-9]*:[A-Za-z0-9_-]+", token.get_secret_value()):
        raise ControlConfigError("CONTROL_BOT_TOKEN has an invalid format")
    account_id = settings.expected_account_id
    if account_id is None:
        raise ControlConfigError("set EXPECTED_ACCOUNT_ID to use the control bot")
    return ControlConfig(
        token, settings.owner_telegram_id or account_id, account_id, settings.dry_run, settings.timezone,
        settings.digest_enabled, settings.digest_time,
    )


def owner_message(update: dict, owner_id: int) -> dict | None:
    message = update.get("message")
    if not isinstance(message, dict) or FORWARDED.intersection(message):
        return None
    sender, chat = message.get("from"), message.get("chat")
    if not isinstance(sender, dict) or not isinstance(chat, dict):
        return None
    if (
        chat.get("type") != "private" or sender.get("is_bot") is not False
        or type(chat.get("id")) is not int or chat["id"] != owner_id
        or type(sender.get("id")) is not int or sender["id"] != owner_id
    ):
        return None
    return message


def owner_command(update: dict, owner_id: int, bot_username: str) -> tuple[str, list[str]] | None:
    message = owner_message(update, owner_id)
    if message is None:
        return None
    text = message.get("text")
    if not isinstance(text, str) or not text.startswith("/") or len(text) > 4096:
        return None
    command, *args = text.split()
    name, separator, target = command[1:].partition("@")
    if separator and target.casefold() != bot_username.casefold():
        return None
    name = name.casefold()
    if name in {"chat", "prefer_input", "confirm_approve", "confirm_external", "draft_page"}:
        return None
    return name, args


def parse_chat_id(value: str) -> int | None:
    if not re.fullmatch(r"-?[1-9][0-9]{0,18}", value):
        return None
    chat_id = int(value)
    return chat_id if -(2**63) <= chat_id < 2**63 else None


async def limits_command(db: aiosqlite.Connection, account_id: int, args: list[str]) -> str:
    chat_id = 0
    if args and args[0] == "chat":
        parsed = parse_chat_id(args[1]) if len(args) >= 2 else None
        if parsed is None:
            return "Формат: /limits chat CHAT_ID [параметр значение]"
        chat_id, args = parsed, args[2:]
    if not args:
        limits = await load_limits(db, account_id, chat_id)
        return "\n".join(f"{name} = {format_limit(name, value)}" for name, value in limits.items())
    value = parse_limit(args[0], args[1], chat_id) if len(args) == 2 else None
    if value is None:
        return "Некорректный параметр или значение. Доступные параметры: /limits или /limits chat CHAT_ID."
    await db.execute(
        """
        INSERT INTO send_limits VALUES (?, ?, ?, ?)
        ON CONFLICT(account_id, chat_id, name) DO UPDATE SET value = excluded.value
        """,
        (account_id, chat_id, args[0], value),
    )
    return f"Лимит сохранён: {args[0]} = {format_limit(args[0], value)}."


async def outbound_status(db: aiosqlite.Connection, config: ControlConfig, now: int) -> str:
    account = config.account_id
    zone = ZoneInfo(config.timezone)
    limits = await load_limits(db, account)
    daily, rolling, _, _ = await budget_usage(db, account, 0, now, zone)
    states = dict(await db.execute_fetchall(
        "SELECT status, COUNT(*) FROM outbox WHERE account_id = ? GROUP BY status", (account,)
    ))
    rows = await db.execute_fetchall(
        """
        SELECT active_chat_id, active_thread_id, active_started, active_last, switch_after, blocked_until
        FROM outbound_state WHERE account_id = ?
        """,
        (account,),
    )
    active, block = "нет", "нет"
    if rows:
        scheduler = ConversationScheduler(*rows[0][:5])
        if scheduler.is_active(now, limits):
            active = str(scheduler.chat_id)
            if scheduler.thread_id:
                active += f" / тема {scheduler.thread_id}"
        if rows[0][5] > now:
            block = datetime.fromtimestamp(rows[0][5], zone).isoformat(timespec="seconds")
    return (
        f"Активный разговор: {active}\nОчередь: {states.get('pending', 0)}"
        f" · отправляется: {states.get('sending', 0)} · доставка неизвестна: {states.get('unknown', 0)}\n"
        f"Бюджет за день: {daily}/{limits['global_daily']} · за 24 ч: {rolling}/{limits['global_rolling']}\n"
        f"Блокировка Telegram до: {block}"
    )


async def apply_command(
    db: aiosqlite.Connection,
    config: ControlConfig,
    worker: WorkerStatus,
    name: str,
    args: list[str],
    router: OpenRouter | None = None,
    *,
    public_username: str | None = None,
) -> str:
    now = int(time.time())
    if name == "menu":
        return MENU_TEXT
    if name == "settings":
        return "Настройки бота"
    if name == "reasoning":
        if args not in ([], ["on"], ["off"]):
            return "Формат: /reasoning on|off"
        if args:
            await db.execute("UPDATE runtime_state SET reasoning_enabled = ? WHERE id = 1", (args == ["on"],))
        [(enabled,)] = await db.execute_fetchall("SELECT reasoning_enabled FROM runtime_state WHERE id = 1")
        return f"Reasoning DeepSeek: {'включён' if enabled else 'выключен'}. Применяется к новым ответам."
    if name == "draft_page":
        return await show_drafts(db, config.account_id, now, None, page=int(args[0]))
    if name == "add_chat":
        return ADD_CHAT
    if name == "prefer_input":
        return f"{PREFER}{args[0]}."
    if name == "confirm_auto":
        return "Включить автоматические ответы в чатах, которым разрешена отправка?"
    if name == "confirm_approve":
        return f"Поставить черновик №{args[0]} в очередь отправки?"
    if name == "confirm_external":
        return f"Разрешить отправлять отфильтрованный контекст чата {args[0]} в OpenRouter?"
    if name == "confirm_reset":
        return "Сбросить локальные счётчики бесплатных запросов?"
    if name == "chat":
        chat_id = parse_chat_id(args[0])
        rows = await db.execute_fetchall(
            "SELECT mode, proactive_enabled, external_context, public_username FROM chats WHERE account_id = ? AND chat_id = ?",
            (config.account_id, chat_id),
        )
        if not rows:
            return "Чат не найден."
        [(mode, proactive, external, username)] = rows
        label = f"@{username}" if username else str(chat_id)
        return (f"{label}\nРежим: {mode}\nOpenRouter: {'включён' if external else 'выключен'}"
                f"\nИнициативы: {'включены' if proactive else 'выключены'}")
    if name in {"start", "help"}:
        return HELP
    if name == "status" and not args:
        [(mode, paused, reason)] = await db.execute_fetchall(
            "SELECT mode, paused, pause_reason FROM runtime_state WHERE id = 1"
        )
        [(count,)] = await db.execute_fetchall(
            "SELECT COUNT(*) FROM chats WHERE account_id = ? AND mode != 'DENY'",
            (config.account_id,),
        )
        return (
            f"Режим: {mode}\nПауза: {'да' if paused else 'нет'}"
            f"{f' ({reason})' if paused and reason else ''}\n"
            f"DRY_RUN: {'включён' if config.dry_run else 'выключен'}\n"
            f"Сессия Telegram: {worker.state}"
            f"{f' ({worker.reason})' if worker.reason else ''}\n"
            f"Разрешено чатов: {count}\n"
            f"{await outbound_status(db, config, now)}"
        )
    if name in {"stop", "pause", "resume"} and not args:
        paused = name != "resume"
        await db.execute(
            "UPDATE runtime_state SET paused = ?, pause_reason = ? WHERE id = 1",
            (paused, "owner" if paused else None),
        )
        if paused:
            limits = await load_limits(db, config.account_id)
            await cancel_pending(db, config.account_id, now, "paused")
            await finish_conversation(db, config.account_id, now + limits["switch_cooldown"])
        if name == "stop":
            return "Работа остановлена. Генерация и отправки на паузе. Для продолжения: /resume."
        return "Пауза включена." if paused else "Пауза снята. Режим сохранён."
    if name == "mode":
        if not args:
            [(mode,)] = await db.execute_fetchall("SELECT mode FROM runtime_state WHERE id = 1")
            return f"Текущий режим: {mode}.\n\n{MODE_HELP}\n\nВыбор режима: /mode observe|draft|auto"
        if len(args) != 1 or args[0].upper() not in MODE_DESCRIPTIONS:
            return f"Выбор режима: /mode observe|draft|auto\n\n{MODE_HELP}"
        mode = args[0].upper()
        changed = await db.execute_fetchall(
            "UPDATE runtime_state SET mode = ? WHERE id = 1 AND mode != ? RETURNING id", (mode, mode),
        )
        if changed:
            await cancel_pending(db, config.account_id, now, "mode_changed")
        return f"Режим сохранён: {mode}. {MODE_DESCRIPTIONS[mode]}"
    if name == "prefer":
        identifier = parse_chat_id(args[0]) if len(args) >= 2 else None
        if identifier is None or identifier <= 0:
            return "Формат: /prefer ID текст. Пример: /prefer 3 ок, мир"
        text = " ".join(args[1:])
        if SENSITIVE_PATTERN.search(text) or style_issue(text, (), 120):
            return "Укажи обычную реплику до 120 символов без ссылок, контактов и секретов."
        if not await save_draft_preference(db, config.account_id, config.owner_id, identifier, text, now):
            return "Черновик не найден."
        return f"Вариант для черновика №{identifier} сохранён для похожих ответов."
    if name in {"drafts", "approve", "reject"}:
        if name == "drafts" and not args:
            return await show_drafts(db, config.account_id, now, None)
        identifier = parse_chat_id(args[0]) if len(args) == 1 or (name == "drafts" and len(args) == 2) else None
        if identifier is None or identifier <= 0:
            return f"Укажи положительный номер черновика. Пример /{name} 1."
        if name == "drafts":
            return await show_drafts(db, config.account_id, now, identifier)
        if name == "approve":
            return await approve_draft(
                db, config.account_id, identifier, now, dry_run=config.dry_run, max_pending=MAX_PENDING,
            )
        return await reject_draft(db, config.account_id, identifier)
    if name == "limits":
        return await limits_command(db, config.account_id, args)
    if name == "style" and not args:
        return await style_status(db, config.owner_id)
    if name in {"models", "providers"} and not args:
        if router is None:
            return "OpenRouter ещё не настроен."
        return router.model_status() if name == "models" else await router.provider_status(db)
    if name == "reset_quota" and not args:
        await reset_provider_quota(db)
        return "Локальные счётчики бесплатных запросов сброшены. Квота OpenRouter действует отдельно."
    if name == "external":
        chat_id = parse_chat_id(args[0]) if args else None
        if chat_id is None or len(args) != 2 or args[1] not in {"on", "off"}:
            return "Формат: /external CHAT_ID on|off"
        enabled = args[1] == "on"
        if enabled and chat_id >= 0:
            return "Для OpenRouter укажи отрицательный ID группы. Личные диалоги не поддерживаются."
        rows = await db.execute_fetchall(
            """
            UPDATE chats SET external_context = ?, external_nonpublic = (? AND COALESCE(?, public_username) IS NULL),
                public_username = COALESCE(?, public_username)
            WHERE account_id = ? AND chat_id = ? AND (NOT ? OR mode != 'DENY')
            RETURNING external_nonpublic
            """, (enabled, enabled, public_username, public_username, config.account_id, chat_id, enabled),
        )
        if not rows:
            return "Сначала разреши группу командой /allow CHAT_ID draft_only."
        await bump_revision(db, config.account_id, chat_id)
        if not enabled:
            await cancel_pending(db, config.account_id, now, "external_disabled", chat_id)
        if not enabled:
            return "Внешний контекст отключён."
        if rows[0][0]:
            return "OpenRouter включён для группы без публичного @username. Модели получат отфильтрованные фрагменты."
        return "Отфильтрованный контекст для OpenRouter разрешён."
    if name == "forget":
        chat_id = parse_chat_id(args[0]) if len(args) == 1 else None
        if chat_id is None:
            return "Формат: /forget CHAT_ID"
        await forget_chat(db, config.account_id, chat_id, now)
        return f"Локальная память чата {chat_id} очищена."
    if name in {"opportunities", "digest"} and not args:
        text = await format_opportunities(db, config.account_id, now, digest=name == "digest")
        return text or "Пока нет подходящих находок."
    if name == "opportunities":
        identifier = parse_chat_id(args[0]) if args else None
        if identifier is None or identifier <= 0 or len(args) != 2 or args[1] not in STATES:
            return "Формат: /opportunities ID new|seen|interesting|rejected|in_progress"
        rows = await db.execute_fetchall(
            "UPDATE opportunities SET state = ? WHERE account_id = ? AND id = ? RETURNING id",
            (args[1], config.account_id, identifier),
        )
        return f"Возможность №{identifier}: {args[1]}." if rows else "Карточка не найдена."
    if name == "chats":
        if len(args) > 1 or (args and not re.fullmatch(r"[1-9][0-9]{0,5}", args[0])):
            return "Формат: /chats [страница]"
        page = int(args[0]) if args else 1
        rows = await db.execute_fetchall(
            """
            SELECT chat_id, mode, proactive_enabled,
                chat_id < 0 AND external_context = 1 AND (public_username IS NOT NULL OR external_nonpublic = 1),
                public_username
            FROM chats WHERE account_id = ? ORDER BY chat_id LIMIT ? OFFSET ?
            """,
            (config.account_id, PAGE_SIZE + 1, (page - 1) * PAGE_SIZE),
        )
        if not rows:
            return "На этой странице чатов нет. Добавить чат можно командой /allow CHAT_ID."
        reply = "\n".join(
            f"{chat_id} · {mode}{f' · @{username}' if username else ''}"
            f"{' · OpenRouter' if external else ''}{' · инициативы' if proactive else ''}"
            for chat_id, mode, proactive, external, username in rows[:PAGE_SIZE]
        )
        if len(rows) > PAGE_SIZE:
            reply += f"\nСледующая страница: /chats {page + 1}"
        return reply
    if name in {"allow", "deny"}:
        if name == "allow" and not args:
            return f"Добавить чат: /allow CHAT_ID [read_only|draft_only|auto_reply [proactive]]\n\n{CHAT_HELP}"
        mode = args[1].casefold() if len(args) >= 2 else "read_only"
        chat_id = parse_chat_id(args[0]) if args else None
        if chat_id is None or (name == "deny" and len(args) != 1):
            return "Укажи ID чата, @username или ссылку t.me/username. Пример /allow https://t.me/username draft_only."
        proactive = mode == "auto_reply" and args[2:] == ["proactive"]
        if name == "allow" and (mode not in CHAT_MODES or (len(args) > 2 and not proactive)):
            return "Формат: /allow CHAT_ID [read_only|draft_only|auto_reply [proactive]]"
        permission = mode.upper() if name == "allow" else "DENY"
        await db.execute(
            """
            INSERT INTO chats (account_id, chat_id, mode, proactive_enabled, public_username) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(account_id, chat_id) DO UPDATE SET mode = excluded.mode,
                proactive_enabled = excluded.proactive_enabled,
                public_username = COALESCE(excluded.public_username, chats.public_username)
            """,
            (config.account_id, chat_id, permission, proactive, public_username),
        )
        if permission != "AUTO_REPLY":
            await cancel_pending(
                db, config.account_id, now, "chat_denied", chat_id, automatic_only=permission == "DRAFT_ONLY",
            )
        if permission == "DENY":
            await db.execute(
                "UPDATE chats SET external_context = 0, external_nonpublic = 0 WHERE account_id = ? AND chat_id = ?",
                (config.account_id, chat_id),
            )
            await bump_revision(db, config.account_id, chat_id)
        return f"Чат {chat_id}: {permission}."
    return HELP


async def handle_update(
    db: aiosqlite.Connection,
    config: ControlConfig,
    worker: WorkerStatus,
    bot_id: int,
    bot_username: str,
    update: dict,
    router: OpenRouter | None = None,
    *,
    resolve_chat: ChatResolver | None = None,
    send_lock: asyncio.Lock | None = None,
) -> str | ControlAction | None:
    command = owner_command(update, config.owner_id, bot_username)
    if command is None:
        callback = callback_action(update, config.owner_id, bot_id)
        message = owner_message(update, config.owner_id)
        command = callback[1] if callback else reply_action(message, bot_id) if message else None
    if command is None:
        return None
    name, args = command
    index = None
    if name in {"allow", "deny", "external", "forget"} and args:
        index = 0
    elif name == "limits" and len(args) >= 2 and args[0] == "chat":
        index = 1
    link = CHAT_LINK.fullmatch(args[index]) if index is not None else None
    public_username = None
    resolution_error = None
    if link is not None:
        resolved = await resolve_chat(link[1]) if resolve_chat else None
        if resolved is None:
            resolution_error = "Telegram не смог найти публичную группу по этой ссылке. Проверь @username или укажи ID."
        else:
            chat_id, public_username = resolved
            args[index] = str(chat_id)

    now = int(time.time())
    update_id = update["update_id"]
    async with send_lock or nullcontext():
        await db.execute("BEGIN IMMEDIATE")
        try:
            rows = await db.execute_fetchall(
                "SELECT update_id FROM control_updates WHERE bot_id = ? AND processed_at >= ?",
                (bot_id, now - UPDATE_TTL),
            )
            if rows and update_id <= rows[0][0]:
                await db.rollback()
                return None
            if resolution_error is not None:
                reply = resolution_error
            elif command == ("login", []):
                reply = ControlAction.LOGIN
            elif command == ("openrouter_key", []):
                reply = ControlAction.OPENROUTER_KEY
            else:
                reply = await apply_command(db, config, worker, name, args, router, public_username=public_username)
            await db.execute(
                """
                INSERT INTO control_updates (bot_id, update_id, processed_at) VALUES (?, ?, ?)
                ON CONFLICT(bot_id) DO UPDATE SET
                    update_id = excluded.update_id, processed_at = excluded.processed_at
                """,
                (bot_id, update_id, now),
            )
            await db.commit()
            return reply
        except BaseException:
            await db.rollback()
            raise
