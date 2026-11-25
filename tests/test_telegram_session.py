import asyncio
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon import errors, functions, types

from aibottelegram.config import Settings
from aibottelegram.telegram import session


@pytest.fixture
def credentials():
    return session.telegram_credentials(
        Settings(api_id=123, api_hash="test", expected_account_id=2**33, _env_file=None)
    )


def test_partial_credentials_are_rejected(monkeypatch):
    for key in ("API_ID", "API_HASH", "EXPECTED_ACCOUNT_ID"):
        monkeypatch.delenv(key, raising=False)
    assert session.telegram_credentials(Settings(_env_file=None)) is None
    with pytest.raises(session.SessionError, match="together"):
        session.telegram_credentials(Settings(api_id=123, _env_file=None))


def test_phone_input_normalizes_separators_and_rejects_invalid_numbers(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "+380 (00) 000-00-00")
    assert session.read_phone() == "+380000000000"
    monkeypatch.setattr("builtins.input", lambda _: "test")
    with pytest.raises(session.SessionError, match="international format"):
        session.read_phone()


@pytest.mark.asyncio
@pytest.mark.parametrize("correct_account", [True, False])
async def test_bot_qr_replaces_session_only_after_account_verification(monkeypatch, tmp_path, credentials, correct_account):
    path = tmp_path / "telegram.session"
    path.write_bytes(b"old")
    client = AsyncMock()
    client.get_me.return_value = SimpleNamespace(id=credentials.account_id + (not correct_account), bot=False)
    client.qr_login.return_value = SimpleNamespace(url="tg://login?token=mock", wait=AsyncMock())

    def make_client(temporary, *args, **kwargs):
        assert temporary != path
        temporary.write_bytes(b"new")
        return client

    monkeypatch.setattr(session, "make_client", make_client)
    publish = AsyncMock()
    if correct_account:
        await session.replace_session_with_qr(path, credentials, publish)
        assert path.read_bytes() == b"new"
    else:
        with pytest.raises(session.SessionError, match="EXPECTED_ACCOUNT_ID"):
            await session.replace_session_with_qr(path, credentials, publish)
        assert path.read_bytes() == b"old"
        client.log_out.assert_awaited_once()
    publish.assert_awaited_once_with("tg://login?token=mock")
    client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_telethon_constructs_local_session_without_connecting(tmp_path, credentials):
    path = tmp_path / "test.session"
    client = session.make_client(path, credentials)
    try:
        assert path.is_file()
        assert not client.is_connected()
        assert not client.session.save_entities
    finally:
        await client.disconnect()


def test_directory_lock_across_processes(tmp_path):
    command = """
import sys
from pathlib import Path
from aibottelegram.telegram.session import SessionError, lock_session_directory

try:
    with lock_session_directory(Path(sys.argv[1])):
        pass
except SessionError:
    sys.exit(17)
"""
    source = Path(session.__file__).resolve().parents[2]
    args = [sys.executable, "-B", "-c", command, str(tmp_path)]
    env = {**os.environ, "PYTHONPATH": str(source)}
    with session.lock_session_directory(tmp_path):
        assert subprocess.run(args, env=env, timeout=8, check=False).returncode == 17
    assert subprocess.run(args, env=env, timeout=8, check=False).returncode == 0


@pytest.mark.asyncio
async def test_login_supports_two_factor_and_saves_only_verified_session(
    monkeypatch, tmp_path, credentials, capsys,
):
    client = AsyncMock()
    client.get_me.return_value = SimpleNamespace(id=credentials.account_id, bot=False)
    client.return_value = types.auth.SentCode(types.auth.SentCodeTypeApp(5), "test-hash")
    client.sign_in.side_effect = [errors.SessionPasswordNeededError(request=None), None]
    inputs = iter(("12345", "password"))
    monkeypatch.setattr(session.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(session, "make_client", lambda *_, **__: client)
    monkeypatch.setattr(session, "read_phone", lambda: "+380000000000")
    monkeypatch.setattr(session, "read_hidden", lambda _, **__: next(inputs))
    path = tmp_path / "test"
    await session.login(path, credentials)
    assert path.with_suffix(".session").is_file()
    assert isinstance(client.await_args.args[0], functions.auth.SendCodeRequest)
    assert client.sign_in.await_args_list[0].kwargs == {
        "phone": "+380000000000", "code": "12345", "phone_code_hash": "test-hash",
    }
    assert client.sign_in.await_args_list[1].kwargs == {"password": "password"}
    assert "verified Telegram chat" in capsys.readouterr().out
    client.get_me.assert_awaited_once()
    client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_wrong_account_does_not_publish_session(monkeypatch, tmp_path, credentials):
    client = AsyncMock()
    client.get_me.return_value = SimpleNamespace(id=42, bot=False)
    monkeypatch.setattr(session.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(session, "make_client", lambda *_, **__: client)
    monkeypatch.setattr(session, "sign_in_with_qr", AsyncMock())
    with pytest.raises(session.SessionError, match="EXPECTED_ACCOUNT_ID"):
        await session.login(tmp_path / "test.session", credentials, qr=True)
    client.assert_not_awaited()
    assert list(tmp_path.glob("*.session")) == []


@pytest.mark.asyncio
async def test_existing_session_is_only_verified(monkeypatch, tmp_path, credentials):
    path = tmp_path / "test.session"
    path.touch(mode=0o600)
    client = AsyncMock()
    client.get_me.return_value = None
    monkeypatch.setattr(session, "make_client", lambda *_, **__: client)
    with pytest.raises(session.SessionError, match="unauthorized"):
        await session.login(path, credentials)
    assert path.exists()
    client.assert_not_awaited()
    client.sign_in.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_code_switches_to_qr_without_requesting_another_code(monkeypatch, credentials):
    client = AsyncMock()
    client.return_value = types.auth.SentCode(types.auth.SentCodeTypeApp(5), "test-hash")
    qr_sign_in = AsyncMock()
    monkeypatch.setattr(session, "read_phone", lambda: "+380000000000")
    monkeypatch.setattr(session, "read_hidden", lambda _, **__: "")
    monkeypatch.setattr(session, "sign_in_with_qr", qr_sign_in)
    await session.sign_in_with_code(client, credentials)
    client.assert_awaited_once()
    qr_sign_in.assert_awaited_once_with(client)
    client.sign_in.assert_not_awaited()


@pytest.mark.asyncio
async def test_qr_waits_before_display_and_replaces_an_expired_code(monkeypatch):
    client = AsyncMock()
    waiting = asyncio.Event()
    scanned = asyncio.Event()
    displays = []

    async def wait_for_scan():
        waiting.set()
        await scanned.wait()

    def display(url):
        displays.append(url)
        if url == "current":
            assert waiting.is_set()
            scanned.set()

    client.qr_login.side_effect = [
        SimpleNamespace(url="expired", wait=AsyncMock(side_effect=TimeoutError)),
        SimpleNamespace(url="current", wait=wait_for_scan),
    ]
    monkeypatch.setattr(session.sys.stdout, "isatty", lambda: True)
    monkeypatch.setattr(session, "display_qr", display)
    await session.sign_in_with_qr(client)
    assert displays == ["expired", "current"]
