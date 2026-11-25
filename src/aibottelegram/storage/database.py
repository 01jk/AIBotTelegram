import sqlite3
from pathlib import Path

import aiosqlite

from ..config import Mode
from .migrations import MIGRATIONS


class StorageError(RuntimeError):
    pass


async def initialize(db: aiosqlite.Connection, mode: Mode) -> None:
    await db.execute("PRAGMA foreign_keys = ON")
    # Migrate under a write lock so concurrent startups see one migration history.
    await db.execute("BEGIN IMMEDIATE")
    try:
        try:
            await db.execute("CREATE VIRTUAL TABLE temp.fts5_check USING fts5(content)")
        except sqlite3.OperationalError as error:
            if error.sqlite_errorcode == sqlite3.SQLITE_ERROR and "no such module" in str(error):
                raise StorageError("SQLite was built without FTS5 support") from error
            raise
        await db.execute("DROP TABLE temp.fts5_check")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY CHECK (version > 0),
                applied_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )
            """
        )
        rows = await db.execute_fetchall("SELECT version FROM schema_migrations ORDER BY version")
        applied = [row[0] for row in rows]
        if len(applied) > len(MIGRATIONS) or applied != list(range(1, len(applied) + 1)):
            raise StorageError("database migration history is incompatible")
        for version, statements in enumerate(MIGRATIONS[len(applied):], start=len(applied) + 1):
            for statement in statements:
                await db.execute(statement)
            await db.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version,))
        await db.execute(
            "INSERT INTO runtime_state (id, mode) VALUES (1, ?) ON CONFLICT(id) DO NOTHING",
            (mode.value,),
        )
        await db.commit()
    except BaseException:
        await db.rollback()
        raise


async def healthcheck(path: Path, now: int, max_age: int) -> bool:
    uri = path.resolve().as_uri() + "?mode=ro"
    async with aiosqlite.connect(uri, uri=True, timeout=2) as db:
        rows = await db.execute_fetchall("SELECT heartbeat_at FROM runtime_state WHERE id = 1")
    return bool(rows and rows[0][0] is not None and 0 <= now - rows[0][0] <= max_age)
