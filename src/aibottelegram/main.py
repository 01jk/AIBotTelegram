import argparse
import asyncio
import json
import logging
import signal
import sqlite3
import sys
import time
from pathlib import Path
from uuid import uuid4

import aiosqlite
from pydantic import ValidationError
from pydantic_settings import SettingsError

from .config import Mode, Settings
from .control.bot import run_control
from .control.commands import ControlConfigError, control_config
from .llm.config import ModelConfigError, load_models
from .llm.credentials import KeyFileError, install_key, read_new_key
from .llm.router import BASE_URL, CATALOG_TTL, OpenRouter, openrouter_http
from .replay import ReplayError, replay
from .storage.backup import DEFAULT_KEEP, backup, restore
from .storage.database import StorageError, healthcheck, initialize
from .style.importer import StyleImportError, import_style
from .style.runtime import PersonaError, bundled_source
from .telegram.session import SessionError, login, replace_session_with_qr, telegram_credentials
from .telegram.worker import WorkerStatus, run_worker

LOG = logging.getLogger(__name__)
HEARTBEAT_INTERVAL = 5
MAX_HEARTBEAT_AGE = 20


async def run_heartbeat(db: aiosqlite.Connection, stop: asyncio.Event) -> None:
    owner = uuid4().hex
    try:
        while not stop.is_set():
            await db.execute(
                "UPDATE runtime_state SET heartbeat_at = ?, heartbeat_owner = ? WHERE id = 1",
                (int(time.time()), owner),
            )
            await db.commit()
            try:
                await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL)
            except TimeoutError:
                continue
    finally:
        await db.execute(
            """
            UPDATE runtime_state SET heartbeat_at = NULL, heartbeat_owner = NULL
            WHERE id = 1 AND heartbeat_owner = ?
            """,
            (owner,),
        )
        await db.commit()


async def import_owner_style(settings: Settings, path: Path | None, *, background: bool) -> None:
    owner = settings.owner_telegram_id or settings.expected_account_id
    if owner is None:
        if background:
            return
        raise StyleImportError("set OWNER_TELEGRAM_ID or EXPECTED_ACCOUNT_ID")
    try:
        bundled = path is None
        path = path if path is not None else bundled_source(settings, owner)
        if path is None:
            if background:
                return
            raise StyleImportError("specify the export path or declare a bundled corpus in persona.yaml")
        if background:
            async with aiosqlite.connect(settings.database_path, timeout=5) as db:
                existing = await db.execute_fetchall(
                    "SELECT 1 FROM style_sources WHERE owner_id = ? AND source_path != ? LIMIT 1",
                    (owner, str(path)),
                )
            if existing:
                return
        report = await asyncio.to_thread(import_style, settings.database_path, path, owner, bundled=bundled)
    except (OSError, sqlite3.Error, StyleImportError, PersonaError) as error:
        if not background:
            raise
        LOG.warning("style_import_failed reason=%s", type(error).__name__)
        return
    LOG.info("style_profile messages=%s examples=%s changed=%s",
             report["message_count"], report["example_count"], report["changed"])


async def refresh_catalog(router: OpenRouter) -> None:
    while True:
        await router.refresh()
        await asyncio.sleep(CATALOG_TTL if router.catalog else 60)


async def execute(
    command: str, settings: Settings, *, qr: bool = False, source: Path | None = None, replace: bool = False,
) -> int:
    if command == "healthcheck":
        ok = await healthcheck(settings.database_path, int(time.time()), MAX_HEARTBEAT_AGE)
        print("healthy" if ok else "unhealthy")
        return 0 if ok else 1

    credentials = telegram_credentials(settings) if command in {"login", "run"} else None
    control = control_config(settings) if command == "run" else None
    if command == "login":
        if credentials is None:
            raise SessionError("set API_ID, API_HASH and EXPECTED_ACCOUNT_ID to log in")
        await login(settings.session_path, credentials, qr=qr, replace=replace)
        print("Telegram session verified.", flush=True)
        return 0

    settings.data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    async with aiosqlite.connect(settings.database_path, timeout=5) as db:
        await initialize(db, settings.mode)
        if command == "style-import":
            await import_owner_style(settings, source, background=False)
            return 0
        if command == "models-check":
            async with openrouter_http() as http:
                router = OpenRouter(http, db, load_models(settings.models_path), settings.openrouter_api_key,
                                    key_file=settings.openrouter_api_key_file)
                await router.refresh()
                print(router.model_status())
                print(await router.provider_status(db))
                return int(bool(router.unconfirmed_models()))
        [(mode, paused)] = await db.execute_fetchall(
            "SELECT mode, paused FROM runtime_state WHERE id = 1"
        )
        print(json.dumps({
            "event": "storage_ready",
            "mode": mode,
            "paused": bool(paused),
            "dry_run": settings.dry_run,
            "fts5": True,
        }), flush=True)
        if command == "run":
            models = load_models(settings.models_path)
            LOG.info("Service running in the foreground. Press Ctrl+C to stop.")
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()

            def on_terminate(_signum: int, _frame: object) -> None:
                loop.call_soon_threadsafe(stop.set)

            previous_handler = signal.signal(signal.SIGTERM, on_terminate)
            try:
                async with (
                    openrouter_http() as http,
                    aiosqlite.connect(settings.database_path, timeout=5) as llm_db,
                    asyncio.TaskGroup() as tasks,
                ):
                    tasks.create_task(run_heartbeat(db, stop))
                    status = WorkerStatus()
                    send_lock = asyncio.Lock()
                    router = OpenRouter(http, llm_db, models, settings.openrouter_api_key,
                                        key_file=settings.openrouter_api_key_file)
                    workers = [
                        tasks.create_task(refresh_catalog(router)),
                        tasks.create_task(import_owner_style(settings, None, background=True)),
                    ]
                    if credentials is None:
                        LOG.info("Telegram is disabled until API credentials and account ID are configured.")
                    else:
                        worker_finished = asyncio.Event()
                        reauthorized = asyncio.Event()

                        async def telegram_worker() -> None:
                            while True:
                                worker_finished.clear()
                                await run_worker(
                                    settings.session_path, settings.database_path, credentials, status,
                                    dry_run=settings.dry_run, timezone=settings.timezone, send_lock=send_lock,
                                    settings=settings, router=router,
                                )
                                worker_finished.set()
                                await reauthorized.wait()
                                reauthorized.clear()

                        async def qr_login(publish) -> None:
                            async with asyncio.timeout(5):
                                await worker_finished.wait()
                            await replace_session_with_qr(settings.session_path, credentials, publish)
                            reauthorized.set()

                        workers.append(tasks.create_task(telegram_worker()))
                    if control is None:
                        LOG.info("Control bot is disabled until CONTROL_BOT_TOKEN is configured.")
                    else:
                        workers.append(tasks.create_task(
                            run_control(
                                settings.database_path, control, status, send_lock=send_lock, router=router,
                                qr_login=qr_login if credentials is not None else None,
                            )
                        ))
                    await stop.wait()
                    for worker in workers:
                        worker.cancel()
            finally:
                signal.signal(signal.SIGTERM, previous_handler)
    return 0


async def update_openrouter_key(settings: Settings, from_env: bool) -> None:
    path = settings.openrouter_api_key_file
    if path is None:
        raise KeyFileError("set OPENROUTER_API_KEY_FILE before replacing a key")
    key = settings.openrouter_api_key if from_env else read_new_key()
    if key is None or not key.get_secret_value().strip():
        raise KeyFileError("OPENROUTER_API_KEY is missing")
    async with openrouter_http() as http:
        await install_key(http, BASE_URL, path, key, initialize=from_env)
    print("OpenRouter key verified and saved.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram AI companion")
    parser.add_argument("command", choices=(
        "init", "login", "run", "healthcheck", "style-import", "models-check", "backup", "restore", "replay",
        "openrouter-key",
    ))
    parser.add_argument("source", type=Path, nargs="?", help="export, backup directory or snapshot, depending on the command")
    parser.add_argument("--qr", action="store_true", help="scan a QR code to log in to Telegram")
    parser.add_argument("--replace", action="store_true", help="replace a Telegram session after a verified login")
    parser.add_argument("--from-env", action="store_true", help="initialize the OpenRouter key file from the current key")
    parser.add_argument("--keep", type=int, help="number of SQLite snapshots to retain")
    parser.add_argument("--mode", type=str.upper, choices=list(Mode), help="runtime mode to simulate in replay")
    args = parser.parse_args()
    if args.qr and args.command != "login":
        parser.error("--qr can only be used with login")
    if args.replace and args.command != "login":
        parser.error("--replace can only be used with login")
    if args.from_env and args.command != "openrouter-key":
        parser.error("--from-env can only be used with openrouter-key")
    if args.source is not None and args.command not in {"style-import", "backup", "restore", "replay"}:
        parser.error("a path can only be used with style-import, backup, restore or replay")
    if args.command in {"restore", "replay"} and args.source is None:
        parser.error(f"{args.command} requires a path")
    if args.keep is not None and args.command != "backup":
        parser.error("--keep can only be used with backup")
    if args.mode is not None and args.command != "replay":
        parser.error("--mode can only be used with replay")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        if args.command == "replay":
            asyncio.run(replay(args.source, Mode(args.mode or Mode.AUTO)))
            return 0
        settings = Settings()
        if args.command == "openrouter-key":
            asyncio.run(update_openrouter_key(settings, args.from_env))
            return 0
        if args.command == "backup":
            keep = DEFAULT_KEEP if args.keep is None else args.keep
            print(backup(settings.database_path, args.source or settings.data_dir / "backups", keep))
            return 0
        if args.command == "restore":
            print(restore(args.source, settings.database_path))
            return 0
        return asyncio.run(execute(args.command, settings, qr=args.qr, source=args.source, replace=args.replace))
    except ValidationError as error:
        for entry in error.errors(include_input=False, include_context=False, include_url=False):
            field = ".".join(map(str, entry["loc"]))
            print(f"invalid configuration: {field} ({entry['type']})", file=sys.stderr)
        return 2
    except SettingsError as error:
        print(f"invalid configuration: {error}", file=sys.stderr)
        return 2
    except SessionError as error:
        print(f"Telegram: {error}", file=sys.stderr)
        return 1
    except ControlConfigError as error:
        print(f"Control bot: {error}", file=sys.stderr)
        return 2
    except KeyFileError as error:
        print(f"OpenRouter: {error}", file=sys.stderr)
        return 2
    except (ModelConfigError, PersonaError, StyleImportError) as error:
        print(f"configuration: {error}", file=sys.stderr)
        return 2
    except ReplayError as error:
        print(f"replay: {error}", file=sys.stderr)
        return 2
    except (OSError, sqlite3.Error, StorageError) as error:
        if args.command == "healthcheck":
            print(f"unhealthy: {error}", file=sys.stderr)
            return 1
        print(f"storage unavailable: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
