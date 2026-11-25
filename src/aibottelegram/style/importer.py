import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import ijson
from ijson.common import JSONError, ObjectBuilder

from .profile import Profile, classify, safe_example

EXAMPLE_LIMIT = 2000
MAX_EXPORT_BYTES = 2 * 1024**3
MESSAGE_PREFIXES = {"messages.item", "chats.list.item.messages.item", "left_chats.list.item.messages.item"}


class StyleImportError(ValueError):
    pass


def telegram_records(path: Path) -> Iterator[tuple[int, dict]]:
    with path.open("rb") as source:
        if source.read(3) != b"\xef\xbb\xbf":
            source.seek(0)
        chat = 0
        builder = None
        message_prefix = ""
        for prefix, event, value in ijson.parse(source):
            if builder is not None:
                builder.event(event, value)
                if prefix == message_prefix and event == "end_map":
                    yield chat, builder.value
                    builder = None
                continue
            if event != "start_map":
                continue
            if prefix in {"chats.list.item", "left_chats.list.item"}:
                chat += 1
            if prefix in MESSAGE_PREFIXES:
                builder = ObjectBuilder()
                builder.event(event, value)
                message_prefix = prefix


def telegram_text(message: dict, owner_id: int) -> str:
    if (
        message.get("type") != "message" or message.get("from_id") != f"user{owner_id}"
        or any(key.startswith("forwarded_") for key in message) or message.get("via_bot")
    ):
        return ""
    text = message.get("text")
    if isinstance(text, list):
        text = "".join(
            part if isinstance(part, str) else part["text"]
            for part in text
            if isinstance(part, str) or (isinstance(part, dict) and isinstance(part.get("text"), str))
        )
    return text.strip() if isinstance(text, str) else ""


def corpus_records(path: Path) -> Iterator[str]:
    with path.open("rb") as source:
        while line := source.readline(65537):
            if len(line) > 65536:
                raise StyleImportError("corpus contains an oversized row")
            if not line.strip():
                continue
            record = json.loads(line)
            if not isinstance(record, dict) or not isinstance(record.get("text"), str):
                raise StyleImportError("invalid owner corpus row")
            yield record["text"].strip()


def build_profile(path: Path, owner_id: int, bundled: bool, staging: sqlite3.Connection) -> dict:
    profile = Profile()
    staging.execute("CREATE TABLE seen (chat INT, id INT, PRIMARY KEY (chat, id))")
    staging.execute("""
        CREATE TABLE examples (fingerprint TEXT PRIMARY KEY, language TEXT, situation TEXT, length INT, text TEXT)
    """)
    def texts():
        if bundled:
            yield from corpus_records(path)
            return
        for chat, record in telegram_records(path):
            text = telegram_text(record, owner_id)
            identifier = record.get("id")
            if text and type(identifier) is int and 0 < identifier < 2**63:
                if not staging.execute("INSERT OR IGNORE INTO seen VALUES (?, ?)", (chat, identifier)).rowcount:
                    continue
            yield text

    for text in texts():
        if not text:
            continue
        # Count style signals on at most one Telegram-sized segment per row.
        segment = text[:4096]
        lang, kind, words = classify(segment)
        profile.observe(segment, lang, kind, words)
        if safe_example(text, words):
            fingerprint = hashlib.sha256(text.casefold().encode("utf-8")).hexdigest()
            staging.execute("INSERT OR IGNORE INTO examples VALUES (?, ?, ?, ?, ?)",
                            (fingerprint, lang, kind, len(text), text))
    report = profile.report()
    if not report["message_count"]:
        raise StyleImportError("no usable messages from the configured owner")
    [count] = staging.execute("SELECT COUNT(*) FROM examples").fetchone()
    report["example_count"] = min(count, EXAMPLE_LIMIT)
    return report


def import_style(database: Path, path: Path, owner_id: int, *, bundled: bool = False) -> dict:
    path = path.resolve(strict=True)
    if (not bundled and path.suffix.lower() != ".json") or (bundled and path.suffix.lower() != ".jsonl"):
        raise StyleImportError("expected a Telegram JSON export or the declared owner JSONL corpus")
    stamp = path.stat()
    if stamp.st_size > MAX_EXPORT_BYTES:
        raise StyleImportError("style source exceeds 2 GiB")
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    source_path = str(path)
    with closing(sqlite3.connect(database, timeout=5)) as db:
        previous = db.execute(
            "SELECT content_hash, report FROM style_sources WHERE owner_id = ? AND source_path = ?",
            (owner_id, source_path),
        ).fetchone()
        if previous and previous[0] == digest:
            return {**json.loads(previous[1]), "changed": False}
        with closing(sqlite3.connect("")) as staging:
            try:
                report = build_profile(path, owner_id, bundled, staging)
            except (JSONError, json.JSONDecodeError, UnicodeError, RecursionError):
                raise StyleImportError("invalid style source JSON") from None
            with db:
                db.execute("BEGIN IMMEDIATE")
                current = path.stat()
                if (current.st_size, current.st_mtime_ns) != (stamp.st_size, stamp.st_mtime_ns):
                    raise StyleImportError("style source changed while importing")
                latest = db.execute(
                    "SELECT content_hash, report FROM style_sources WHERE owner_id = ? AND source_path = ?",
                    (owner_id, source_path),
                ).fetchone()
                if latest and latest[0] == digest:
                    return {**json.loads(latest[1]), "changed": False}
                db.execute("DELETE FROM style_examples WHERE owner_id = ? AND source_path = ?", (owner_id, source_path))
                rows = staging.execute("SELECT * FROM examples ORDER BY fingerprint LIMIT ?", (EXAMPLE_LIMIT,))
                db.executemany("INSERT INTO style_examples VALUES (?, ?, ?, ?, ?, ?, ?)",
                               ((owner_id, source_path, *row) for row in rows))
                db.execute("""
                    INSERT INTO style_sources VALUES (?, ?, ?, ?)
                    ON CONFLICT(owner_id, source_path) DO UPDATE SET
                        content_hash = excluded.content_hash, report = excluded.report
                """, (owner_id, source_path, digest, json.dumps(report, ensure_ascii=False)))
        return {**report, "changed": True}
