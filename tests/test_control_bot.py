import asyncio
import sqlite3
from contextlib import closing

import aiosqlite
import pytest
import aiohttp
from aiohttp import web
from pydantic import SecretStr

from aibottelegram.config import Mode
from aibottelegram.control.bot import run_control
from aibottelegram.control.commands import ControlConfig, handle_update
from aibottelegram.control.menu import ADD_CHAT, menu_rows
from aibottelegram.llm.config import ModelsConfig
from aibottelegram.llm.credentials import StoredKey, load_key, write_key
from aibottelegram.llm.router import OpenRouter
from aibottelegram.storage.database import initialize
from aibottelegram.telegram.worker import WorkerStatus

OWNER = 4_294_967_311
CHAT = -1001234567890
CONFIG = ControlConfig(SecretStr("123:test-token"), OWNER, OWNER, True)


def update(identifier, command, *, sender=OWNER, chat_id=OWNER, chat_type="private", **fields):
    return {
        "update_id": identifier,
        "message": {
            "from": {"id": sender, "is_bot": False},
            "chat": {"id": chat_id, "type": chat_type},
            "text": command,
            **fields,
        },
    }


@pytest.mark.asyncio
async def test_commands_are_atomic_and_idempotent_across_connections(tmp_path):
    path = tmp_path / "test.sqlite3"
    status = WorkerStatus("connected")
    async with aiosqlite.connect(path) as db:
        await initialize(db, Mode.OBSERVE)
        for identifier, command in enumerate(
            (f"/allow {CHAT}", "/mode auto", "/pause", "/resume", "/stop"), start=1
        ):
            assert await handle_update(db, CONFIG, status, 123, "testbot", update(identifier, command))
    async with aiosqlite.connect(path) as db:
        await initialize(db, Mode.OBSERVE)
        assert not await handle_update(db, CONFIG, status, 123, "testbot", update(4, "/resume"))
        assert await db.execute_fetchall("SELECT mode, paused FROM runtime_state") == [("AUTO", 1)]
        assert await db.execute_fetchall("SELECT mode FROM chats") == [("READ_ONLY",)]
        assert "Чат" in await handle_update(
            db, CONFIG, status, 123, "testbot", update(6, f"/deny {CHAT}")
        )
        assert await db.execute_fetchall("SELECT mode FROM chats") == [("DENY",)]
        assert "23:00" in await handle_update(
            db, CONFIG, status, 123, "testbot", update(7, "/limits quiet_start 23:00")
        )
        assert "Некорректный" in await handle_update(
            db, CONFIG, status, 123, "testbot", update(8, "/limits global_daily -1")
        )
        assert await db.execute_fetchall("SELECT name, value FROM send_limits") == [("quiet_start", 1380)]


@pytest.mark.asyncio
async def test_menu_owner_can_add_chat_and_manage_it_with_buttons(tmp_path):
    path = tmp_path / "menu.sqlite3"
    worker = WorkerStatus("connected")

    def click(identifier, data, *, sender=OWNER, chat_id=OWNER):
        return {"update_id": identifier, "callback_query": {
            "id": str(identifier), "from": {"id": sender, "is_bot": False}, "data": "ui:" + data,
            "message": {"date": 1794100000, "message_id": 40, "from": {"id": 123, "is_bot": True},
                        "chat": {"id": chat_id, "type": "private"}},
        }}

    async with aiosqlite.connect(path) as db:
        await initialize(db, Mode.DRAFT)
        assert await handle_update(db, CONFIG, worker, 123, "testbot", click(1, "add_chat")) == ADD_CHAT
        parent = {"message_id": 41, "text": ADD_CHAT, "from": {"id": 123, "is_bot": True}}
        assert "READ_ONLY" in await handle_update(
            db, CONFIG, worker, 123, "testbot", update(2, str(CHAT), reply_to_message=parent),
        )
        buttons = [button for row in await menu_rows(db, OWNER, "chats", []) for button in row]
        assert any(button["callback_data"] == f"ui:chat {CHAT}" for button in buttons)
        data = f"allow {CHAT} draft_only"
        assert not await handle_update(db, CONFIG, worker, 123, "testbot", click(3, data, sender=7))
        assert not await handle_update(db, CONFIG, worker, 123, "testbot", click(4, data, chat_id=7))
        assert "DRAFT_ONLY" in await handle_update(db, CONFIG, worker, 123, "testbot", click(5, data))
        assert not await handle_update(db, CONFIG, worker, 123, "testbot", click(5, f"deny {CHAT}"))
        assert await db.execute_fetchall("SELECT mode FROM chats") == [("DRAFT_ONLY",)]

        await db.executemany(
            """
            INSERT INTO drafts (account_id, chat_id, thread_id, context_id, memory_revision, text,
                                created_at, expires_at, paid_chat)
            VALUES (?, ?, 0, ?, 0, ?, 0, 1, ?)
            """,
            [(OWNER, CHAT, item, f"Тест {item}", item == 1) for item in range(1, 7)],
        )
        await db.commit()
        page = await menu_rows(db, OWNER, "drafts", [])
        assert "ui:draft_page 2" in [key["callback_data"] for row in page for key in row]
        assert "Тест 1\n" in await handle_update(db, CONFIG, worker, 123, "testbot", click(6, "draft_page 2"))
        assert "Тест 6\n" not in await handle_update(db, CONFIG, worker, 123, "testbot", click(7, "draft_page 2"))
        detail = await menu_rows(db, OWNER, "drafts", ["1", "2"])
        actions = [key["callback_data"] for row in detail for key in row]
        assert "ui:draft_page 2" in actions and "ui:confirm_approve 1" not in actions
        assert "включён" in await handle_update(db, CONFIG, worker, 123, "testbot", click(8, "reasoning on"))


@pytest.mark.asyncio
async def test_mode_help_shows_current_mode_and_chat_permissions(tmp_path):
    async with aiosqlite.connect(tmp_path / "help.sqlite3") as db:
        await initialize(db, Mode.OBSERVE)
        status = WorkerStatus("connected")
        mode = await handle_update(db, CONFIG, status, 123, "testbot", update(1, "/mode"))
        start = await handle_update(db, CONFIG, status, 123, "testbot", update(2, "/start"))
        allow = await handle_update(db, CONFIG, status, 123, "testbot", update(3, "/allow"))
        assert "Текущий режим: OBSERVE" in mode
        assert "/drafts" in mode and "/approve ID" in mode
        assert "DRY_RUN=false" in mode and "доступный бюджет" in mode
        assert "Общий режим бота" in start and "Права отдельного чата" in start
        assert "READ_ONLY" in allow and "DRAFT_ONLY" in allow and "AUTO_REPLY" in allow and "DENY" in allow
        assert await db.execute_fetchall("SELECT mode, paused FROM runtime_state") == [("OBSERVE", 0)]
        assert await db.execute_fetchall("SELECT COUNT(*) FROM chats") == [(0,)]


@pytest.mark.asyncio
async def test_qr_login_is_owner_only_deduplicated_and_photo_is_deleted(tmp_path):
    path = tmp_path / "qr.sqlite3"
    async with aiosqlite.connect(path) as db:
        await initialize(db, Mode.OBSERVE)
    completed = asyncio.Event()
    calls = []
    attempts = 0

    async def login(publish):
        nonlocal attempts
        attempts += 1
        await publish("tg://login?token=mock")

    async def telegram(request):
        method = request.match_info["method"]
        if method == "sendPhoto":
            parts = await request.multipart()
            fields = {}
            async for part in parts:
                fields[part.name] = await part.read()
            assert fields["chat_id"] == str(OWNER).encode()
            assert fields["protect_content"] == b"true"
            assert fields["photo"].startswith(b"\x89PNG\r\n\x1a\n")
            assert b"tg://login" not in fields["caption"]
            calls.append("photo")
            result = {"message_id": 50}
        else:
            payload = await request.json()
            if method == "getMe":
                result = {"id": 123, "is_bot": True, "username": "testbot"}
            elif method == "getUpdates":
                if "offset" in payload:
                    await asyncio.wait_for(completed.wait(), 2)
                    return web.json_response({"ok": False, "error_code": 409}, status=409)
                result = [
                    update(1, "/login", sender=1234), update(2, "/login", chat_id=-42, chat_type="group"),
                    update(3, "/login"), update(3, "/login"),
                ]
            elif method == "deleteMessage":
                assert payload == {"chat_id": OWNER, "message_id": 50}
                calls.append("delete")
                result = True
            else:
                assert method == "sendMessage" and payload["chat_id"] == OWNER
                assert "сохранена" in payload["text"]
                calls.append("success")
                completed.set()
                result = {"message_id": 51}
        return web.json_response({"ok": True, "result": result})

    app = web.Application()
    app.router.add_post("/bot123:test-token/{method}", telegram)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = runner.addresses[0][1]
        async with asyncio.timeout(8):
            await run_control(path, CONFIG, WorkerStatus("stopped"), base_url=f"http://127.0.0.1:{port}", qr_login=login)
    finally:
        await runner.cleanup()
    assert attempts == 1
    assert calls == ["photo", "delete", "success"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("owner", "delete_ok"), [("same-user", True), ("different-user", True), ("same-user", False)])
async def test_key_replacement_requires_owner_prompt_and_successful_deletion(tmp_path, owner, delete_ok):
    path = tmp_path / "control.sqlite3"
    key_file = tmp_path / "openrouter-key.json"
    old = SecretStr("old-test-key")
    new = "sk-or-v1-" + "a" * 64
    write_key(key_file, StoredKey(old, '["same-user",null]'), initialize=True)
    calls, replies = [], []

    async def telegram(request):
        method = request.match_info["method"]
        payload = await request.json()
        assert new not in str(payload)
        if method == "getMe":
            result = {"id": 123, "is_bot": True, "username": "testbot"}
        elif method == "getUpdates":
            offset = payload.get("offset")
            if offset is None:
                result = [update(1, "/openrouter_key", sender=44), update(2, "/openrouter_key")]
            elif offset == 3:
                result = [
                    update(3, new, chat_id=-42, chat_type="group", message_id=60,
                           reply_to_message={"message_id": 50}),
                    update(4, new, message_id=61, reply_to_message={"message_id": 49}),
                    update(5, new, message_id=62, reply_to_message={"message_id": 50}),
                ]
            else:
                return web.json_response({"ok": False, "error_code": 409}, status=409)
        elif method == "sendMessage":
            assert payload["chat_id"] == OWNER
            if "reply_markup" in payload:
                assert payload["reply_markup"]["force_reply"] is True
                assert payload["protect_content"] is True
                calls.append("prompt")
                result = {"message_id": 50}
            else:
                replies.append(payload["text"])
                result = {"message_id": 70}
        else:
            assert method == "deleteMessage" and payload["chat_id"] == OWNER
            message_id = payload["message_id"]
            calls.append(("delete", message_id))
            result = delete_ok or message_id != 62
        return web.json_response({"ok": True, "result": result})

    async def openrouter(request):
        assert request.headers["Authorization"] == "Bearer " + new
        assert calls[-2:] == [("delete", 62), ("delete", 50)]
        calls.append("verify")
        return web.json_response({"data": {"creator_user_id": owner}})

    app = web.Application()
    app.router.add_post("/bot123:test-token/{method}", telegram)
    app.router.add_get("/or/key", openrouter)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        base = f"http://127.0.0.1:{runner.addresses[0][1]}"
        async with aiosqlite.connect(path) as db, aiohttp.ClientSession() as http:
            await initialize(db, Mode.OBSERVE)
            router = OpenRouter(http, db, ModelsConfig(), old, key_file=key_file, base_url=base + "/or")
            async with asyncio.timeout(8):
                await run_control(path, CONFIG, WorkerStatus("connected"), base_url=base, router=router)
    finally:
        await runner.cleanup()
    assert calls[:3] == ["prompt", ("delete", 61), ("delete", 62)]
    if not delete_ok:
        assert "не подтвердил удаление" in replies[-1]
        assert "verify" not in calls
    else:
        assert "Ключ OpenRouter заменён" in replies[-1]
    assert load_key(key_file).secret == (SecretStr(new) if delete_ok else old)
    assert new.encode() not in path.read_bytes()


@pytest.mark.asyncio
async def test_public_chat_references_use_verified_numeric_id(tmp_path):
    path = tmp_path / "links.sqlite3"
    async with aiosqlite.connect(path) as db:
        await initialize(db, Mode.OBSERVE)
    requested, replies = [], []

    async def telegram(request):
        method = request.match_info["method"]
        payload = await request.json()
        if method == "getMe":
            result = {"id": 123, "is_bot": True, "username": "testbot"}
        elif method == "getUpdates":
            if "offset" in payload:
                return web.json_response({"ok": False, "error_code": 409}, status=409)
            result = [
                update(1, "/allow @unauthorized", sender=44),
                update(2, "/allow https://t.me/examplegroup draft_only"),
                update(3, "/external @examplegroup on"),
                update(4, "/limits chat t.me/examplegroup daily 4"),
                update(5, "/forget https://t.me/examplegroup/"),
                update(6, "/allow @privateuser"),
                update(7, "/deny https://t.me/+privateinvite"),
            ]
        elif method == "getChat":
            username = payload["chat_id"]
            requested.append(username)
            result = ({"id": CHAT, "type": "supergroup", "username": "ExampleGroup"} if username == "@examplegroup"
                      else {"id": 987, "type": "private", "username": "privateuser"})
        else:
            assert method == "sendMessage" and payload["chat_id"] == OWNER
            replies.append(payload["text"])
            result = {"message_id": 60}
        return web.json_response({"ok": True, "result": result})

    app = web.Application()
    app.router.add_post("/bot123:test-token/{method}", telegram)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        async with asyncio.timeout(8):
            await run_control(path, CONFIG, WorkerStatus("connected"),
                              base_url=f"http://127.0.0.1:{runner.addresses[0][1]}")
    finally:
        await runner.cleanup()
    assert requested == ["@examplegroup"] * 4 + ["@privateuser"]
    assert any("Отфильтрованный контекст" in reply for reply in replies)
    assert any("не смог найти публичную группу" in reply for reply in replies)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT chat_id, mode, public_username, external_context, external_nonpublic FROM chats").fetchall() == [
            (CHAT, "DRAFT_ONLY", "ExampleGroup", 1, 0),
        ]
        assert db.execute("SELECT chat_id, name, value FROM send_limits").fetchall() == [(CHAT, "daily", 4)]


@pytest.mark.asyncio
async def test_real_http_polling_recovers_from_429_and_rejects_non_owner(tmp_path):
    path = tmp_path / "test.sqlite3"
    async with aiosqlite.connect(path) as db:
        await initialize(db, Mode.OBSERVE)
    polls, messages = [], []

    async def telegram(request):
        payload = await request.json()
        method = request.match_info["method"]
        if method == "getMe":
            return web.json_response({"ok": True, "result": {"id": 123, "username": "testbot", "is_bot": True}})
        if method == "sendMessage":
            messages.append(payload)
            return web.json_response({"ok": True, "result": {"message_id": 1}})
        polls.append(payload)
        if len(polls) == 1:
            return web.json_response({"ok": False, "error_code": 429, "parameters": {"retry_after": 1}}, status=429)
        if len(polls) == 2:
            return web.json_response({"ok": True, "result": [
                update(7, "/status@testbot"),
                update(1, "/mode auto", sender=1),
                update(2, "/mode auto", chat_id=-10, chat_type="group"),
                update(3, "/mode auto", forward_origin={"type": "user"}),
                update(4, "/mode auto", chat_id=1),
                update(5, "/mode@otherbot auto"),
                update(6, "/pause"),
            ]})
        return web.json_response({"ok": False, "error_code": 409}, status=409)

    app = web.Application()
    app.router.add_post("/bot123:test-token/{method}", telegram)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        await run_control(path, CONFIG, WorkerStatus("connected"), base_url=f"http://127.0.0.1:{port}")
    finally:
        await runner.cleanup()

    assert len(messages) == 2
    assert all(message["chat_id"] == OWNER for message in messages)
    assert "Пауза включена" in messages[0]["text"]
    assert "Режим: OBSERVE" in messages[1]["text"]
    assert "DRY_RUN: включён" in messages[1]["text"]
    assert polls[-1]["offset"] == 8
    assert polls[-1]["allowed_updates"] == ["message", "callback_query"]
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT mode, paused FROM runtime_state").fetchone() == ("OBSERVE", 1)
