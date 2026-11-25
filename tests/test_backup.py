import sqlite3
from contextlib import closing

import aiosqlite
import pytest

from aibottelegram.config import Mode
from aibottelegram.storage.backup import backup, restore
from aibottelegram.storage.database import StorageError, initialize


@pytest.mark.asyncio
async def test_backup_reads_committed_wal_and_restore_preserves_data(tmp_path):
    live = tmp_path / "companion.sqlite3"
    async with aiosqlite.connect(live) as db:
        await db.execute("PRAGMA journal_mode = WAL")
        await initialize(db, Mode.DRAFT)
        await db.execute("UPDATE runtime_state SET paused = 1, heartbeat_at = 99, heartbeat_owner = 'live'")
        await db.commit()
        await db.execute("UPDATE runtime_state SET mode = 'AUTO'")
        snapshot = backup(live, tmp_path / "backups")
        await db.rollback()

    restored = restore(snapshot, tmp_path / "recovered" / "companion.sqlite3")
    with closing(sqlite3.connect(restored)) as db:
        assert db.execute("SELECT mode, paused, heartbeat_at, heartbeat_owner FROM runtime_state").fetchone() == (
            "DRAFT", 1, None, None,
        )
        assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    with pytest.raises(StorageError, match="already exist"):
        restore(snapshot, live)


@pytest.mark.asyncio
async def test_backup_rotates_only_managed_snapshots_and_never_creates_source(tmp_path):
    live = tmp_path / "companion.sqlite3"
    with pytest.raises(StorageError, match="does not exist"):
        backup(live, tmp_path / "backups")
    assert not live.exists()
    async with aiosqlite.connect(live) as db:
        await initialize(db, Mode.AUTO)
    folder = tmp_path / "backups"
    oldest = backup(live, folder, keep=1)
    manual = folder / "manual.sqlite3"
    manual.touch()
    newest = backup(live, folder, keep=1)
    assert not oldest.exists()
    assert newest.is_file() and manual.is_file()
