import re

import aiosqlite

from ..policy.drafts import DRAFT_PAGE_SIZE, draft_rows

MENU_TEXT = "Управление ботом"
ID = r"-?[1-9][0-9]{0,18}"
PAGE = r"[1-9][0-9]{0,5}"
CALLBACK = re.compile(
    rf"(?:menu|status|pause|resume|mode(?: (?:observe|draft|auto))?|confirm_auto|"
    rf"drafts(?: {ID}(?: {PAGE})?)?|draft_page {PAGE}|(?:confirm_approve|approve|reject|prefer_input) {ID}|"
    rf"chats(?: {PAGE})?|chat {ID}|allow {ID} (?:read_only|draft_only|auto_reply(?: proactive)?)|"
    rf"deny {ID}|(?:external {ID} (?:on|off)|confirm_external {ID})|"
    r"add_chat|settings|models|reasoning(?: on| off)?|providers|style|limits|reset_quota|confirm_reset|login|openrouter_key|digest|opportunities|help)"
)
ADD_CHAT = "Пришли в ответ ID, @username или ссылку t.me/username. Чат получит режим чтения."
PREFER = "Пришли в ответ свой вариант для черновика №"


def reply_action(message: dict, bot: int) -> tuple[str, list[str]] | None:
    parent = message.get("reply_to_message")
    if not isinstance(parent, dict) or not isinstance(parent.get("from"), dict):
        return None
    if parent["from"].get("id") != bot or parent["from"].get("is_bot") is not True:
        return None
    prompt, text = parent.get("text"), message.get("text")
    if not isinstance(prompt, str) or not isinstance(text, str) or len(text) > 4096 or text.startswith("/"):
        return None
    args = text.split()
    if prompt == ADD_CHAT and len(args) == 1:
        return "allow", args
    match = re.fullmatch(re.escape(PREFER) + rf"({ID})\.", prompt)
    if match and args:
        return "prefer", [match[1], *args]
    return None


def callback_action(update: dict, owner: int, bot: int) -> tuple[str, tuple[str, list[str]]] | None:
    query = update.get("callback_query")
    if not isinstance(query, dict):
        return None
    sender, message = query.get("from"), query.get("message")
    if not isinstance(sender, dict) or not isinstance(message, dict):
        return None
    chat, author = message.get("chat"), message.get("from")
    if (
        sender.get("id") != owner or sender.get("is_bot") is not False
        or not isinstance(chat, dict) or chat.get("id") != owner or chat.get("type") != "private"
        or not isinstance(author, dict) or author.get("id") != bot or author.get("is_bot") is not True
        or not message.get("date")
    ):
        return None
    identifier, data = query.get("id"), query.get("data")
    if not isinstance(identifier, str) or not identifier or not isinstance(data, str) or not data.startswith("ui:"):
        return None
    data = data[3:]
    if not CALLBACK.fullmatch(data):
        return None
    name, *args = data.split()
    return identifier, (name, args)


def button(label: str, action: str) -> dict:
    return {"text": label, "callback_data": "ui:" + action}


def main_rows() -> list[list[dict]]:
    return [
        [button("Статус", "status"), button("Режим", "mode")],
        [button("Черновики", "drafts"), button("Чаты", "chats")],
        [button("Пауза", "pause"), button("Продолжить", "resume")],
        [button("Настройки", "settings"), button("Дайджест", "digest")],
    ]


async def menu_rows(db: aiosqlite.Connection, account: int, name: str, args: list[str]) -> list[list[dict]]:
    back = [button("Главное меню", "menu")]
    if name in {"menu", "start"}:
        return main_rows()
    if name == "mode":
        return [[button("Наблюдение", "mode observe"), button("Черновики", "mode draft"),
                 button("Автоответы", "confirm_auto")], back]
    if name == "confirm_auto":
        return [[button("Включить автоответы", "mode auto")], [button("Назад", "mode")], back]
    if name == "draft_page" or (name == "drafts" and not args):
        page = int(args[0]) if args else 1
        drafts = await draft_rows(db, account, page=page)
        items = [button(f"№{row[0]}", f"drafts {row[0]} {page}") for row in drafts[:DRAFT_PAGE_SIZE]]
        navigation = []
        if page > 1:
            navigation.append(button("‹ Назад", f"draft_page {page - 1}"))
        if len(drafts) > DRAFT_PAGE_SIZE:
            navigation.append(button("Дальше ›", f"draft_page {page + 1}"))
        return ([items] if items else []) + ([navigation] if navigation else []) + [back]
    if name == "drafts" and args:
        item = args[0]
        if not re.fullmatch(ID, item):
            return [back]
        visible = await db.execute_fetchall(
            """
            SELECT d.paid_chat FROM drafts d JOIN chats c ON c.account_id = d.account_id AND c.chat_id = d.chat_id
            WHERE d.account_id = ? AND CAST(d.id AS TEXT) = ? AND d.state = 'pending' AND c.mode != 'DENY'
            """, (account, item),
        )
        actions = []
        if visible:
            actions.append([button("Отклонить", f"reject {item}")])
            if not visible[0][0]:
                actions[0].insert(0, button("Одобрить", f"confirm_approve {item}"))
        page = args[1] if len(args) > 1 else "1"
        return actions + [[button("Запомнить свой вариант", f"prefer_input {item}"),
                           button("К черновикам", f"draft_page {page}")], back]
    if name == "confirm_approve":
        item = args[0]
        return [[button("Подтвердить отправку", f"approve {item}")], [button("Назад", f"drafts {item}")], back]
    if name in {"approve", "reject"}:
        return [[button("К черновикам", "drafts")], back]
    if name == "chats":
        page = int(args[0]) if args else 1
        chats = await db.execute_fetchall(
            "SELECT chat_id, public_username FROM chats WHERE account_id = ? ORDER BY chat_id LIMIT 31 OFFSET ?",
            (account, (page - 1) * 30),
        )
        buttons = [button(f"@{username}" if username else str(chat), f"chat {chat}") for chat, username in chats[:30]]
        rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
        navigation = []
        if page > 1:
            navigation.append(button("‹ Назад", f"chats {page-1}"))
        if len(chats) > 30:
            navigation.append(button("Дальше ›", f"chats {page+1}"))
        return rows + ([navigation] if navigation else []) + [[button("Добавить чат", "add_chat")], back]
    if name in {"chat", "allow", "external", "deny"} and args and re.fullmatch(ID, args[0]):
        item = args[0]
        return [
            [button("Чтение", f"allow {item} read_only"), button("Черновики", f"allow {item} draft_only")],
            [button("Автоответы", f"allow {item} auto_reply"), button("С инициативой", f"allow {item} auto_reply proactive")],
            [button("OpenRouter вкл.", f"confirm_external {item}"), button("OpenRouter выкл.", f"external {item} off")],
            [button("Запретить чат", f"deny {item}"), button("Список чатов", "chats")], back,
        ]
    if name == "confirm_external":
        item = args[0]
        return [[button("Разрешить OpenRouter", f"external {item} on"), button("Назад", f"chat {item}")], back]
    if name == "confirm_reset":
        return [[button("Сбросить счётчики бота", "reset_quota"), button("Назад", "providers")], back]
    if name == "providers":
        return [[button("Обновить", "providers"), button("Сбросить локально", "confirm_reset")], back]
    if name == "settings":
        return [
            [button("Модели", "models"), button("Квоты", "providers"), button("Лимиты", "limits")],
            [button("Стиль", "style"), button("Reasoning", "reasoning"), button("Вход QR", "login")],
            [button("Ключ OpenRouter", "openrouter_key"), button("Справка", "help")], back,
        ]
    if name == "reasoning":
        return [[button("Включить", "reasoning on"), button("Выключить", "reasoning off")],
                [button("Настройки", "settings")], back]
    return [back]
