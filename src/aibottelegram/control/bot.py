import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
from pydantic import SecretStr
from telethon import errors

from ..features.opportunities import reserve_digest
from ..llm.credentials import KEY_TEXT, KeyFileError, install_key
from ..llm.router import OpenRouter
from ..telegram.session import SessionError
from ..telegram.worker import WorkerStatus
from .commands import UPDATE_TTL, ControlAction, ControlConfig, handle_update, owner_command, owner_message
from .menu import MENU_TEXT, callback_action, menu_rows, reply_action

LOG = logging.getLogger(__name__)
BOT_API_URL = "https://api.telegram.org"
POLL_TIMEOUT = 20
PERMANENT_ERRORS = {400, 401, 403, 404, 409}
QRLogin = Callable[[Callable[[str], Awaitable[None]]], Awaitable[None]]
KEY_PROMPT_TTL = 180


class BotAPIError(RuntimeError):
    def __init__(self, code: int, retry_after: int = 0):
        super().__init__(f"Bot API error {code}")
        self.code = code
        self.retry_after = retry_after


class BotAPI:
    def __init__(self, http: aiohttp.ClientSession, token: str, base_url: str):
        self.http = http
        self._url = f"{base_url.rstrip('/')}/bot{token}"

    async def call(self, method: str, payload: dict | aiohttp.FormData) -> object:
        options = {"data": payload} if isinstance(payload, aiohttp.FormData) else {"json": payload}
        if method in {"sendMessage", "sendPhoto", "deleteMessage", "getChat", "answerCallbackQuery"}:
            options["timeout"] = aiohttp.ClientTimeout(total=5, connect=3)
        try:
            async with self.http.post(
                f"{self._url}/{method}", allow_redirects=False, **options
            ) as response:
                status = response.status
                if status in PERMANENT_ERRORS:
                    raise BotAPIError(status)
                try:
                    body = await response.json(content_type=None)
                except (ValueError, UnicodeError):
                    raise BotAPIError(status if status != 200 else 502) from None
        except (aiohttp.ClientError, TimeoutError):
            raise BotAPIError(503) from None
        if not isinstance(body, dict):
            raise BotAPIError(status if status != 200 else 502)
        if status == 200 and body.get("ok") is True and "result" in body:
            return body["result"]
        code = body.get("error_code")
        if status != 200 or type(code) is not int or not 400 <= code < 600:
            code = status if status != 200 else 502
        parameters = body.get("parameters")
        wait = parameters.get("retry_after") if isinstance(parameters, dict) else None
        raise BotAPIError(code, wait if code == 429 and type(wait) is int and wait > 0 else 0)

    async def resolve_chat(self, username: str) -> tuple[int, str] | None:
        try:
            result = await self.call("getChat", {"chat_id": f"@{username}"})
        except BotAPIError:
            return None
        if not isinstance(result, dict) or result.get("type") not in ("supergroup", "channel"):
            return None
        chat_id = result.get("id")
        if type(chat_id) is not int or not -(2**63) <= chat_id < 0:
            return None
        canonical = result.get("username")
        return chat_id, canonical if isinstance(canonical, str) and canonical else username


class BotLogin:
    def __init__(self, api: BotAPI, owner_id: int, worker: WorkerStatus, login: QRLogin):
        self.api, self.owner_id, self.worker, self.login = api, owner_id, worker, login
        self.task: asyncio.Task | None = None

    def request(self) -> str | None:
        if self.task is not None and not self.task.done():
            return "Вход уже начат. Проверь QR в этом чате."
        if self.worker.state == "connected":
            return "Личный аккаунт уже подключён."
        if self.worker.state != "stopped":
            return "Бот подключается к Telegram. Проверь /status и повтори /login, если сессия остановлена."
        self.task = asyncio.create_task(self.run())
        return None

    async def close(self) -> None:
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task

    async def run(self) -> None:
        photo_id = None

        async def publish(url: str) -> None:
            nonlocal photo_id
            import qrcode
            from qrcode.image.pure import PyPNGImage

            image = BytesIO()
            qrcode.make(url, image_factory=PyPNGImage).save(image)
            data = aiohttp.FormData()
            data.add_field("chat_id", str(self.owner_id))
            data.add_field("protect_content", "true")
            data.add_field("caption", "Открой этот QR на другом экране. На телефоне: Telegram → Настройки → "
                                      "Устройства → Подключить устройство. Если QR истёк, отправь /login снова.")
            data.add_field("photo", image.getvalue(), filename="login.png", content_type="image/png")
            result = await self.api.call("sendPhoto", data)
            if not isinstance(result, dict) or type(result.get("message_id")) is not int:
                raise BotAPIError(502)
            photo_id = result["message_id"]

        try:
            await self.login(publish)
            message = "Сессия Telegram сохранена. Подключаю аккаунт. Проверь /status."
        except errors.SessionPasswordNeededError:
            message = "Telegram требует пароль 2FA. Заверши вход через SSH командой login --qr --replace."
        except (TimeoutError, errors.AuthTokenExpiredError):
            message = "Время входа истекло. Отправь /login, когда будешь готов отсканировать QR."
        except errors.FloodWaitError as error:
            message = f"Telegram просит подождать {error.seconds} с перед повтором /login."
        except (BotAPIError, SessionError, ImportError, OSError, errors.RPCError, TypeError) as error:
            LOG.warning("control_login_failed reason=%s", type(error).__name__)
            message = "Вход не завершён. Убедись, что сканируешь QR нужным аккаунтом, и проверь /status."
        finally:
            if photo_id is not None:
                with suppress(BotAPIError):
                    await self.api.call("deleteMessage", {"chat_id": self.owner_id, "message_id": photo_id})
        with suppress(BotAPIError):
            await self.api.call("sendMessage", {"chat_id": self.owner_id, "text": message})


class BotKeyChange:
    def __init__(self, api: BotAPI, owner_id: int, router: OpenRouter | None, clock: Callable[[], float]):
        self.api, self.owner_id, self.router, self.clock = api, owner_id, router, clock
        self.prompt: tuple[int, float] | None = None

    async def start(self) -> str | None:
        if self.router is None or self.router.key_file is None:
            return "Замена ключа недоступна. Сначала настрой файл ключа на сервере."
        if self.prompt and self.clock() < self.prompt[1]:
            return "Ответь новым ключом на предыдущий запрос бота."
        self.prompt = None
        try:
            result = await self.api.call("sendMessage", {
                "chat_id": self.owner_id,
                "text": "Ответь на это сообщение одним новым ключом OpenRouter. "
                        "Запрос действует 3 минуты. Сообщение с ключом будет удалено.",
                "protect_content": True,
                "reply_markup": {"force_reply": True, "input_field_placeholder": "Новый ключ OpenRouter"},
            })
        except BotAPIError:
            return "Не удалось открыть замену ключа. Повтори /openrouter_key."
        if not isinstance(result, dict) or type(result.get("message_id")) is not int:
            return "Не удалось открыть замену ключа. Повтори /openrouter_key."
        self.prompt = result["message_id"], self.clock() + KEY_PROMPT_TTL
        return None

    async def accept(self, update: dict) -> str | None:
        message = owner_message(update, self.owner_id)
        if message is None or not isinstance(message.get("text"), str):
            return None
        text = message["text"].strip()
        reply_to = message.get("reply_to_message")
        is_reply = bool(self.prompt and isinstance(reply_to, dict) and reply_to.get("message_id") == self.prompt[0])
        if not KEY_TEXT.search(text):
            if not is_reply or text.startswith("/"):
                return None
            if "sk-or-v1-" not in text:
                return "Ответь на запрос бота одним новым ключом OpenRouter."
        message_id = message.get("message_id")
        if type(message_id) is not int:
            return "Ключ не изменён. Удали сообщение с ключом вручную."
        try:
            deleted = await self.api.call("deleteMessage", {"chat_id": self.owner_id, "message_id": message_id})
        except BotAPIError:
            deleted = False
        if deleted is not True:
            return "Ключ не изменён. Telegram не подтвердил удаление сообщения. Удали его вручную."
        if not is_reply or self.prompt is None or self.clock() >= self.prompt[1]:
            return "Сообщение удалено. Для замены начни с /openrouter_key и ответь на запрос бота."
        if not KEY_TEXT.fullmatch(text):
            return "Сообщение удалено. Ответь на запрос бота только новым ключом."
        prompt_id = self.prompt[0]
        self.prompt = None
        with suppress(BotAPIError):
            await self.api.call("deleteMessage", {"chat_id": self.owner_id, "message_id": prompt_id})
        if self.router is None or self.router.key_file is None:
            return "Замена ключа недоступна. Проверь настройки сервера."
        try:
            await install_key(self.router.http, self.router.url, self.router.key_file, SecretStr(text))
        except KeyFileError:
            return "Не удалось проверить или сохранить ключ. Действующий ключ сохранён."
        return "Ключ OpenRouter заменён. Следующий запрос возьмёт новый ключ. Квота сохраняется."


def update_id(update: object) -> int:
    if isinstance(update, dict):
        value = update.get("update_id")
        if type(value) is int and 0 < value < 2**63 - 1:
            return value
    return 0


async def poll(
    api: BotAPI, db: aiosqlite.Connection, config: ControlConfig, worker: WorkerStatus,
    send_lock: asyncio.Lock,
    router: OpenRouter | None = None,
    login: BotLogin | None = None,
) -> None:
    clock = asyncio.get_running_loop().time
    offset = None
    last_update = 0.0
    backoff = 1
    reply_after = 0.0
    notice_attempted = False
    next_outbound_notice = 0.0
    last_models_notice: tuple[str, ...] = ()
    bot_id = None
    username = ""
    zone = ZoneInfo(config.timezone)
    key_change = BotKeyChange(api, config.owner_id, router, clock)

    async def reply(text: str, markup: dict | None = None) -> bool:
        nonlocal reply_after
        if clock() < reply_after:
            return False
        try:
            payload = {"chat_id": config.owner_id, "text": text}
            if markup is not None:
                payload["reply_markup"] = markup
            await api.call("sendMessage", payload)
        except BotAPIError as error:
            if error.code == 429:
                reply_after = clock() + (error.retry_after or 1)
            LOG.warning("control_reply_failed code=%s", error.code)
            return False
        return True

    while True:
        try:
            if bot_id is None:
                identity = await api.call("getMe", {})
                if (
                    not isinstance(identity, dict) or type(identity.get("id")) is not int
                    or not 0 < identity["id"] < 2**63 or identity.get("is_bot") is not True
                    or not isinstance(identity.get("username"), str)
                ):
                    raise BotAPIError(502)
                bot_id, username = identity["id"], identity["username"]
                LOG.info("control_bot_connected")

            if worker.state != "stopped":
                notice_attempted = False
            elif worker.reason and not notice_attempted:
                notice_attempted = await reply(
                    f"Подключение личного аккаунта остановилось ({worker.reason}). Подробнее в /status."
                )

            if router is not None:
                missing = router.unconfirmed_models()
                if missing and missing != last_models_notice:
                    last_models_notice = missing
                    await reply("OpenRouter не подтвердил маршруты: " + ", ".join(missing) + ". /models")
                elif not missing:
                    last_models_notice = ()

            if config.digest_enabled:
                async with send_lock:
                    [(paused,)] = await db.execute_fetchall("SELECT paused FROM runtime_state WHERE id = 1")
                    digest = None if paused else await reserve_digest(
                        db, config.account_id, zone, config.digest_time, int(time.time()),
                    )
                if digest is not None:
                    day, text = digest
                    sent = await reply(text)
                    await db.execute(
                        "UPDATE digest_runs SET status = ? WHERE account_id = ? AND local_date = ?",
                        ("sent" if sent else "unconfirmed", config.account_id, day),
                    )
                    await db.commit()

            if clock() >= next_outbound_notice:
                rows = await db.execute_fetchall(
                    """
                    SELECT id, chat_id, status, reason FROM outbox
                    WHERE account_id = ? AND notify_owner = 1 ORDER BY id LIMIT 1
                    """,
                    (config.account_id,),
                )
                if rows:
                    item, chat, state, reason = rows[0]
                    if await reply(f"Отправка №{item}, чат {chat}: {state} ({reason}). Подробности в /status."):
                        await db.execute("UPDATE outbox SET notify_owner = 0 WHERE id = ?", (item,))
                        await db.commit()
                    else:
                        next_outbound_notice = clock() + 60

            if offset is not None and clock() - last_update > UPDATE_TTL:
                offset = None
            payload: dict = {"timeout": POLL_TIMEOUT, "limit": 50, "allowed_updates": ["message", "callback_query"]}
            if offset is not None:
                payload["offset"] = offset
            updates = await api.call("getUpdates", payload)
            if not isinstance(updates, list):
                raise BotAPIError(502)
        except BotAPIError as error:
            if error.code in PERMANENT_ERRORS:
                LOG.error("control_bot_stopped code=%s", error.code)
                return
            delay = max(backoff, error.retry_after)
            LOG.warning("control_bot_retry code=%s delay=%s", error.code, delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, 30)
            continue

        backoff = 1
        replies = []
        for update in sorted(updates, key=update_id):
            identifier = update_id(update)
            if not identifier or (offset is not None and identifier < offset):
                continue
            callback = callback_action(update, config.owner_id, bot_id)
            command = owner_command(update, config.owner_id, username)
            message = owner_message(update, config.owner_id)
            action = callback[1] if callback else command
            if action is None and message:
                action = reply_action(message, bot_id)
            if callback:
                with suppress(BotAPIError):
                    await api.call("answerCallbackQuery", {"callback_query_id": callback[0]})
            response = await key_change.accept(update)
            if response is None:
                response = await handle_update(
                    db, config, worker, bot_id, username, update, router=router,
                    resolve_chat=api.resolve_chat, send_lock=send_lock,
                )
            offset = identifier + 1
            last_update = clock()
            if response is ControlAction.LOGIN:
                response = login.request() if login else "Вход через QR недоступен. Проверь настройки Telegram."
            elif response is ControlAction.OPENROUTER_KEY:
                response = await key_change.start()
            if response:
                name, args = action if action else ("", [])
                markup = None
                if callback and name in {"add_chat", "prefer_input"}:
                    markup = {"force_reply": True}
                elif callback or name in {"start", "menu"} or (message and command is None and action):
                    if name == "start":
                        response = MENU_TEXT
                    markup = {"inline_keyboard": await menu_rows(db, config.account_id, name, args)}
                replies.append((response, markup))
        for response, markup in replies:
            await reply(response, markup)


async def run_control(
    database_path: Path, config: ControlConfig, worker: WorkerStatus, *, base_url: str = BOT_API_URL,
    send_lock: asyncio.Lock | None = None,
    router: OpenRouter | None = None,
    qr_login: QRLogin | None = None,
) -> None:
    timeout = aiohttp.ClientTimeout(total=POLL_TIMEOUT + 10, connect=5)
    async with aiosqlite.connect(database_path, timeout=5) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        async with aiohttp.ClientSession(timeout=timeout) as http:
            api = BotAPI(http, config.token.get_secret_value(), base_url)
            login = BotLogin(api, config.owner_id, worker, qr_login) if qr_login else None
            try:
                await poll(api, db, config, worker, send_lock or asyncio.Lock(), router, login)
            finally:
                if login is not None:
                    await login.close()
