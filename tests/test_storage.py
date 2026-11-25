import asyncio
import sqlite3
from contextlib import closing

import aiosqlite
import pytest

from aibottelegram.config import Mode
from aibottelegram.main import run_heartbeat
from aibottelegram.storage import database


@pytest.mark.asyncio
async def test_reopening_preserves_runtime_state(tmp_path):
    path = tmp_path / "companion.sqlite3"
    async with aiosqlite.connect(path) as db:
        await database.initialize(db, Mode.OBSERVE)
        await db.execute("UPDATE runtime_state SET mode = 'DRAFT', paused = 1 WHERE id = 1")
        await db.commit()

    async with aiosqlite.connect(path) as db:
        await database.initialize(db, Mode.AUTO)
        assert await db.execute_fetchall("SELECT version FROM schema_migrations ORDER BY version") == [
            (version,) for version in range(1, len(database.MIGRATIONS) + 1)
        ]
        assert await db.execute_fetchall("SELECT mode, paused FROM runtime_state") == [
            ("DRAFT", 1)
        ]


@pytest.mark.asyncio
async def test_concurrent_initialization(tmp_path):
    path = tmp_path / "companion.sqlite3"

    async def start():
        async with aiosqlite.connect(path) as db:
            await database.initialize(db, Mode.OBSERVE)

    await asyncio.gather(*(start() for _ in range(4)))
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT COUNT(*) FROM schema_migrations").fetchone() == (len(database.MIGRATIONS),)
        assert db.execute("SELECT COUNT(*) FROM runtime_state").fetchone() == (1,)


@pytest.mark.asyncio
async def test_outbound_migration_preserves_version_two_state(tmp_path, monkeypatch):
    path = tmp_path / "existing.sqlite3"
    async with aiosqlite.connect(path) as db:
        with monkeypatch.context() as previous_version:
            previous_version.setattr(database, "MIGRATIONS", database.MIGRATIONS[:2])
            await database.initialize(db, Mode.DRAFT)
        await db.execute("UPDATE runtime_state SET paused = 1")
        await db.execute("INSERT INTO chats (account_id, chat_id, mode) VALUES (1, -42, 'READ_ONLY')")
        await db.commit()

    async with aiosqlite.connect(path) as db:
        await database.initialize(db, Mode.OBSERVE)
        assert await db.execute_fetchall("SELECT mode, paused FROM runtime_state") == [("DRAFT", 1)]
        assert await db.execute_fetchall("SELECT chat_id, mode FROM chats") == [(-42, "READ_ONLY")]
        assert await db.execute_fetchall("SELECT COUNT(*) FROM outbox") == [(0,)]


@pytest.mark.asyncio
async def test_failed_migration_rolls_back_schema(tmp_path, monkeypatch):
    path = tmp_path / "companion.sqlite3"
    monkeypatch.setattr(database, "MIGRATIONS", (*database.MIGRATIONS, ("BROKEN SQL",)))
    async with aiosqlite.connect(path) as db:
        with pytest.raises(sqlite3.OperationalError):
            await database.initialize(db, Mode.OBSERVE)
        assert await db.execute_fetchall("SELECT name FROM sqlite_master") == []


@pytest.mark.asyncio
async def test_newer_schema_is_rejected(tmp_path):
    path = tmp_path / "companion.sqlite3"
    async with aiosqlite.connect(path) as db:
        await database.initialize(db, Mode.OBSERVE)
        await db.execute("INSERT INTO schema_migrations (version) VALUES (?)", (len(database.MIGRATIONS) + 1,))
        await db.commit()
        with pytest.raises(database.StorageError, match="incompatible"):
            await database.initialize(db, Mode.OBSERVE)


@pytest.mark.asyncio
async def test_chat_defaults_to_deny(tmp_path):
    async with aiosqlite.connect(tmp_path / "companion.sqlite3") as db:
        await database.initialize(db, Mode.OBSERVE)
        await db.execute("INSERT INTO chats (account_id, chat_id) VALUES (?, ?)", (1, -10**12))
        assert await db.execute_fetchall("SELECT mode, proactive_enabled FROM chats") == [
            ("DENY", 0)
        ]
        with pytest.raises(sqlite3.IntegrityError):
            await db.execute("UPDATE chats SET proactive_enabled = 1")


@pytest.mark.asyncio
async def test_healthcheck_does_not_create_database(tmp_path):
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(sqlite3.OperationalError):
        await database.healthcheck(path, now=100, max_age=20)
    assert not path.exists()


@pytest.mark.asyncio
async def test_heartbeat_health_and_stop(tmp_path, monkeypatch):
    path = tmp_path / "companion space.sqlite3"
    monkeypatch.setattr("aibottelegram.main.time", type("Clock", (), {"time": lambda: 100}))
    async with aiosqlite.connect(path) as db:
        await database.initialize(db, Mode.OBSERVE)
        assert not await database.healthcheck(path, now=100, max_age=20)
        stop = asyncio.Event()
        task = asyncio.create_task(run_heartbeat(db, stop))
        try:
            async with asyncio.timeout(2):
                while not await database.healthcheck(path, now=100, max_age=20):
                    await asyncio.sleep(0.01)
            assert await database.healthcheck(path, now=120, max_age=20)
            assert not await database.healthcheck(path, now=121, max_age=20)
            assert not await database.healthcheck(path, now=99, max_age=20)
        finally:
            stop.set()
            await task
        assert not await database.healthcheck(path, now=100, max_age=20)
