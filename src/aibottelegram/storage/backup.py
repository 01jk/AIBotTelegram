import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from .database import MIGRATIONS, StorageError

DEFAULT_KEEP = 7
SNAPSHOT = re.compile(r"companion-\d{8}T\d{12}Z-[0-9a-f]{8}\.sqlite3")


def _copy(source: Path, target: Path, *, restore: bool) -> None:
    if not source.is_file():
        raise StorageError("source database does not exist")
    sidecars = [target.with_name(target.name + suffix) for suffix in ("-wal", "-shm", "-journal")]
    if target.exists() or any(path.exists() or path.is_symlink() for path in sidecars):
        raise StorageError("destination database or sidecars already exist")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    deadline = time.monotonic() + 30

    def progress(_status: int, _remaining: int, _total: int) -> None:
        if time.monotonic() > deadline:
            raise StorageError("database snapshot timed out")

    with TemporaryDirectory(dir=target.parent, prefix=".snapshot-") as directory:
        staged = Path(directory) / "snapshot.sqlite3"
        staged.touch(mode=0o600)
        with (
            closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as original,
            closing(sqlite3.connect(staged)) as snapshot,
        ):
            original.backup(snapshot, pages=128, progress=progress, sleep=0.1)
            snapshot.execute("PRAGMA journal_mode = DELETE")
            if snapshot.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise StorageError("snapshot integrity check failed")
            versions = [row[0] for row in snapshot.execute("SELECT version FROM schema_migrations ORDER BY version")]
            if not versions or versions != list(range(1, len(versions) + 1)) or len(versions) > len(MIGRATIONS):
                raise StorageError("snapshot migration history is incompatible")
            if restore:
                snapshot.execute("UPDATE runtime_state SET heartbeat_at = NULL, heartbeat_owner = NULL WHERE id = 1")
                snapshot.commit()
        # Link the verified file atomically and refuse to replace an existing database.
        os.link(staged, target)


def backup(source: Path, directory: Path, keep: int = DEFAULT_KEEP) -> Path:
    if not 1 <= keep <= 365:
        raise StorageError("keep must be between 1 and 365")
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    target = directory / f"companion-{timestamp}-{uuid4().hex[:8]}.sqlite3"
    _copy(source, target, restore=False)
    managed = sorted(
        (file for file in directory.iterdir() if SNAPSHOT.fullmatch(file.name) and file.is_file() and not file.is_symlink()),
        reverse=True,
    )
    for expired in managed[keep:]:
        expired.unlink(missing_ok=True)
    return target


def restore(source: Path, target: Path) -> Path:
    _copy(source, target, restore=True)
    return target
