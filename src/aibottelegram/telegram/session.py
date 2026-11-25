import asyncio
import errno
import getpass
import logging
import os
import re
import stat
import sys
import tempfile
import warnings
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path

from pydantic import SecretStr
from telethon import TelegramClient, errors, functions, types
from telethon.sessions import SQLiteSession

from ..config import Settings

CONNECT_TIMEOUT = 25
QR_ATTEMPTS = 3
INVALID_AUTH = (errors.UnauthorizedError, errors.AuthKeyError, errors.AuthKeyNotFound)
CODE_DESTINATIONS = {
    types.auth.SentCodeTypeApp: "the verified Telegram chat on a device already signed in to this account",
    types.auth.SentCodeTypeSms: "your SMS inbox",
    types.auth.SentCodeTypeSmsWord: "your SMS inbox",
    types.auth.SentCodeTypeSmsPhrase: "your SMS inbox",
    types.auth.SentCodeTypeCall: "a phone call",
    types.auth.SentCodeTypeEmailCode: "your Telegram login email inbox",
}


class SessionError(RuntimeError):
    pass


@dataclass(frozen=True)
class Credentials:
    api_id: int
    api_hash: SecretStr
    account_id: int


def telegram_credentials(settings: Settings) -> Credentials | None:
    api_id = settings.api_id
    api_hash = settings.api_hash
    account_id = settings.expected_account_id
    if api_id is None and api_hash is None and account_id is None:
        return None
    if (
        api_id is None
        or api_hash is None
        or not api_hash.get_secret_value().strip()
        or account_id is None
    ):
        raise SessionError("set API_ID, API_HASH and EXPECTED_ACCOUNT_ID together")
    return Credentials(api_id, api_hash, account_id)


def session_file(path: Path) -> Path:
    if not path.name or path.name == "..":
        raise SessionError("TELEGRAM_SESSION_PATH must name a session file")
    if not path.name.endswith(".session"):
        path = path.with_name(path.name + ".session")
    path = path.resolve()
    if path.suffix != ".session":
        raise SessionError("session file must resolve to a .session path")
    return path


@contextmanager
def lock_session_directory(directory: Path) -> Iterator[None]:
    lock = directory / ".aibottelegram.session.lock"
    fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise SessionError(
                    "another process is using this Telegram session directory"
                ) from None
            raise
        yield
    finally:
        os.close(fd)


def check_session_file(path: Path) -> None:
    if not path.is_file():
        raise SessionError("Telegram session is missing; run the login command in a terminal")
    if os.name != "nt" and stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise SessionError("Telegram session file permissions are too broad; set chmod 600")


def make_client(
    path: Path, credentials: Credentials, *, receive_updates: bool = False, request_retries: int = 1
) -> TelegramClient:
    session = SQLiteSession(str(path))
    session.save_entities = False
    # Library diagnostics may include raw protocol data. Log only outcomes at our boundary.
    transport_log = logging.getLogger("aibottelegram.telegram.transport")
    transport_log.setLevel(logging.CRITICAL + 1)
    transport_log.propagate = False
    return TelegramClient(
        session,
        credentials.api_id,
        credentials.api_hash.get_secret_value(),
        base_logger=transport_log,
        receive_updates=receive_updates,
        sequential_updates=True,
        auto_reconnect=False,
        connection_retries=0,
        request_retries=request_retries,
        flood_sleep_threshold=0,
        raise_last_call_error=True,
        entity_cache_limit=1000,
    )


async def verify_account(client: TelegramClient, expected_id: int) -> None:
    me = await client.get_me()
    if me is None:
        raise SessionError("Telegram session is unauthorized or revoked")
    if me.bot or me.id != expected_id:
        raise SessionError("Telegram account does not match EXPECTED_ACCOUNT_ID")


def read_phone() -> str:
    try:
        phone = input("Telegram phone (+country code): ").strip()
    except EOFError:
        raise SessionError("login requires an interactive terminal") from None
    phone = re.sub(r"[ ()-]", "", phone)
    if not re.fullmatch(r"\+[1-9][0-9]{6,14}", phone):
        raise SessionError("enter the phone number in international format, starting with +")
    return phone


def read_hidden(prompt: str, *, allow_empty: bool = False) -> str:
    if not sys.stdin.isatty():
        raise SessionError("login requires a terminal with hidden input")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            value = getpass.getpass(prompt)
        except (getpass.GetPassWarning, EOFError):
            raise SessionError("login requires a terminal with hidden input") from None
    if not allow_empty and not value.strip():
        raise SessionError("login input cannot be empty")
    return value


def display_qr(url: str) -> None:
    from qrcode import QRCode

    if os.name == "nt":
        from colorama import just_fix_windows_console

        just_fix_windows_console()
    qr = QRCode()
    qr.add_data(url)
    qr.print_ascii(tty=True)


async def sign_in_with_qr(client: TelegramClient) -> None:
    if not sys.stdout.isatty():
        raise SessionError("QR login requires an interactive terminal")
    print("On your phone, open Telegram > Settings > Devices > Link Desktop Device.", flush=True)
    for attempt in range(QR_ATTEMPTS):
        async with asyncio.timeout(CONNECT_TIMEOUT):
            qr = await client.qr_login()
        waiter = asyncio.create_task(qr.wait())
        try:
            # Telethon must register its login-token handler before the user scans.
            await asyncio.sleep(0)
            display_qr(qr.url)
            async with asyncio.timeout(60):
                await waiter
            return
        except (TimeoutError, errors.AuthTokenExpiredError):
            if attempt + 1 < QR_ATTEMPTS:
                print("QR expired. Scan the new code below.", flush=True)
        except TypeError:
            raise SessionError("Telegram returned an unexpected QR login response") from None
        finally:
            if not waiter.done():
                waiter.cancel()
                with suppress(asyncio.CancelledError):
                    await waiter
    raise SessionError("QR login timed out; run login --qr again when ready to scan")


async def replace_session_with_qr(
    path: Path, credentials: Credentials, publish: Callable[[str], Awaitable[None]],
) -> None:
    path = session_file(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with lock_session_directory(path.parent), tempfile.TemporaryDirectory(prefix=".qr-", dir=path.parent) as folder:
        temporary = Path(folder) / "telegram.session"
        temporary.touch(mode=0o600)
        client = make_client(temporary, credentials, receive_updates=True)
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT):
                await client.connect()
                qr = await client.qr_login()
            waiter = asyncio.create_task(qr.wait())
            try:
                await asyncio.sleep(0)
                await publish(qr.url)
                async with asyncio.timeout(60):
                    await waiter
            finally:
                if not waiter.done():
                    waiter.cancel()
                # Retrieve a failure even if publishing the image failed first.
                await asyncio.gather(waiter, return_exceptions=True)
            async with asyncio.timeout(CONNECT_TIMEOUT):
                try:
                    await verify_account(client, credentials.account_id)
                except SessionError:
                    with suppress(TimeoutError, OSError, errors.RPCError):
                        await client.log_out()
                    raise
        finally:
            await client.disconnect()
        if any(path.with_name(path.name + suffix).exists() for suffix in ("-wal", "-shm", "-journal")):
            raise SessionError("Служебные файлы старой сессии ещё заняты. Проверь /status.")
        os.replace(temporary, path)


async def sign_in_with_code(client: TelegramClient, credentials: Credentials) -> None:
    phone = read_phone()
    async with asyncio.timeout(CONNECT_TIMEOUT):
        sent = await client(functions.auth.SendCodeRequest(
            phone, credentials.api_id, credentials.api_hash.get_secret_value(), types.CodeSettings()
        ))
    if isinstance(sent, types.auth.SentCodeSuccess):
        return
    destination = CODE_DESTINATIONS.get(type(sent.type)) if isinstance(sent, types.auth.SentCode) else None
    if not destination or not sent.phone_code_hash:
        print("Telegram did not offer a supported code delivery method. Switching to QR login.", flush=True)
        await sign_in_with_qr(client)
        return
    print(f"Telegram selected code delivery via {destination}.", flush=True)
    code = read_hidden("Telegram login code (Enter for QR): ", allow_empty=True).strip()
    if not code:
        await sign_in_with_qr(client)
        return
    async with asyncio.timeout(CONNECT_TIMEOUT):
        await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)


async def verify_or_login(
    path: Path, credentials: Credentials, interactive: bool, *, qr: bool = False
) -> None:
    if interactive and not sys.stdin.isatty():
        raise SessionError("login requires an interactive terminal")
    client = make_client(path, credentials, receive_updates=interactive)
    try:
        async with asyncio.timeout(CONNECT_TIMEOUT):
            await client.connect()
        if interactive:
            try:
                if qr:
                    await sign_in_with_qr(client)
                else:
                    await sign_in_with_code(client, credentials)
            except errors.SessionPasswordNeededError:
                password = read_hidden("Telegram 2FA password: ")
                async with asyncio.timeout(CONNECT_TIMEOUT):
                    await client.sign_in(password=password)
        async with asyncio.timeout(CONNECT_TIMEOUT):
            await verify_account(client, credentials.account_id)
    finally:
        await client.disconnect()


async def login(path: Path, credentials: Credentials, *, qr: bool = False, replace: bool = False) -> None:
    path = session_file(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        with lock_session_directory(path.parent):
            if path.exists() and not replace:
                check_session_file(path)
                await verify_or_login(path, credentials, interactive=False)
                return

            fd, filename = tempfile.mkstemp(prefix=".login-", suffix=".session", dir=path.parent)
            os.close(fd)
            temporary_session = Path(filename)
            try:
                await verify_or_login(temporary_session, credentials, interactive=True, qr=qr)
                os.replace(temporary_session, path)
            finally:
                temporary_session.unlink(missing_ok=True)
    except errors.FloodWaitError as error:
        raise SessionError(f"Telegram asked to wait {error.seconds}s before retrying login") from None
    except errors.PhoneNumberFloodError:
        raise SessionError("Telegram limited code requests for this number. Try login --qr.") from None
    except errors.PhoneNumberInvalidError:
        raise SessionError("Telegram rejected the number. Check the country code and digits.") from None
    except (errors.PhoneCodeInvalidError, errors.PhoneCodeExpiredError):
        raise SessionError("Telegram rejected the code. Use the newest code or run login --qr.") from None
    except (*INVALID_AUTH, errors.RPCError, OSError) as error:
        raise SessionError(f"Telegram login failed ({type(error).__name__})") from None
