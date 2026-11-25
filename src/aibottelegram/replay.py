import asyncio
import json
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .config import Mode, Settings
from .features.memory import Memory, MemoryEvent
from .features.participation import ParticipationDecision, ParticipationPlanner
from .llm.config import ModelsConfig
from .llm.drafting import Draft
from .llm.router import RouterUnavailable
from .policy.outbound import OutboundGateway, Queued
from .storage.database import initialize

ACCOUNT = 1
EPOCH = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp())
MAX_EVENTS = 100_000
MAX_LINE = 32_768


class ReplayError(ValueError):
    pass


class ReplayEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    t: int = Field(ge=0, le=365 * 86400)
    chat: int = Field(ge=-(2**63), lt=0)
    message: int = Field(gt=0, le=2**31 - 1)
    sender: int = Field(gt=0, le=2**63 - 2)
    text: str = Field(max_length=4096)
    thread: int = Field(default=0, ge=0, le=2**31 - 1)
    reply_to: int | None = Field(default=None, gt=0, le=2**31 - 1)
    outgoing: bool = False
    bot: bool = False
    addressed: bool = False


@dataclass
class VirtualClock:
    now: int = EPOCH

    def __call__(self) -> int:
        return self.now


class ReplayRouter:
    config = ModelsConfig.model_validate({"models": {
        "participation": {"primary": "replay/local:free"},
        "conversation": {"primary": "replay/local:free"},
    }})

    def __init__(self):
        self.context = None

    async def complete(self, role, *, system, public_data, schema, authorize, max_tokens=384):
        if not await authorize():
            raise RouterUnavailable("replay authorization changed")
        if role == "participation":
            self.context = public_data
            action = "FOLLOW_UP" if "?" in public_data["latest"] or public_data["direct"] else "IGNORE"
            return ParticipationDecision(action=action, relevance=90)
        return Draft(text="Можеш уточнити, що саме не працює?" if public_data["language"] == "uk" else
                          "Можешь уточнить, что именно не работает?")


class ReplayTransport:
    async def prepare(self, candidate: Queued) -> Queued:
        return candidate

    async def send(self, request: Queued) -> int:
        return request.context_id + 1


def read_events(source: Path):
    last = -1
    count = 0
    with source.open(encoding="utf-8-sig") as stream:
        number = 0
        while line := stream.readline(MAX_LINE + 1):
            number += 1
            if len(line) > MAX_LINE:
                raise ReplayError(f"line {number}: too long")
            if not line.strip():
                continue
            try:
                event = ReplayEvent.model_validate_json(line)
            except ValidationError:
                raise ReplayError(f"line {number}: invalid replay record") from None
            count += 1
            if event.t < last or count > MAX_EVENTS:
                raise ReplayError(f"line {number}: timestamps must be ordered and input limited to {MAX_EVENTS} events")
            last = event.t
            yield event
    if not count:
        raise ReplayError("export has no replay events")


async def replay(source: Path, mode: Mode = Mode.AUTO) -> None:
    started = time.perf_counter()
    outcomes = Counter()

    def report(event: MemoryEvent, reason: str, context=None) -> None:
        outcomes[reason] += 1
        print(json.dumps({
            "t": event.timestamp - EPOCH, "chat": event.chat, "thread": event.thread,
            "message": event.message // 2, "outcome": reason, "model_context": context,
        }, ensure_ascii=True))

    with TemporaryDirectory(prefix="companion-replay-") as directory:
        root = Path(directory)
        guide = root / "style.md"
        guide.write_text("Пиши коротко.", encoding="utf-8")
        settings = Settings.model_construct(owner_telegram_id=ACCOUNT, persona_path=root / "unused.yaml",
                                            style_guide_path=guide)
        clock, router, transport = VirtualClock(), ReplayRouter(), ReplayTransport()
        path = root / "replay.sqlite3"
        async with aiosqlite.connect(path) as db, aiosqlite.connect(path) as reader:
            await initialize(db, mode)
            await db.executemany("INSERT INTO send_limits VALUES (?, 0, ?, 10)",
                                 [(ACCOUNT, "global_daily"), (ACCOUNT, "global_rolling")])
            await db.commit()
            lock = asyncio.Lock()
            memory = Memory(db, ACCOUNT, lock, settings.opportunity_weights)
            gateway = OutboundGateway(db, ACCOUNT, transport, dry_run=False, zone=ZoneInfo(settings.timezone),
                                      lock=lock, clock=clock)
            planner = ParticipationPlanner(reader, settings, ACCOUNT, router, gateway, clock=clock)

            async def drain(until: int) -> None:
                while planner.jobs:
                    key, (due, pending) = next(iter(planner.jobs.items()))
                    if due > until:
                        break
                    planner.jobs.pop(key)
                    clock.now = int(due)
                    router.context = None
                    reason = await planner.consider(pending)
                    if reason == "queued":
                        dispatched = await gateway.dispatch_once()
                        reason = "mock_sent" if dispatched == "sent" else (dispatched or reason)
                        if dispatched == "sent":
                            await memory.maintain(settings.memory_retention_days, clock.now)
                    report(pending, reason, router.context)

            for entry in read_events(source):
                timestamp = EPOCH + entry.t
                await drain(timestamp)
                clock.now = timestamp
                await db.execute(
                    """
                    INSERT INTO chats (account_id, chat_id, mode, external_context)
                    VALUES (?, ?, 'AUTO_REPLY', 1) ON CONFLICT DO NOTHING
                    """, (ACCOUNT, entry.chat),
                )
                await db.commit()
                event = MemoryEvent(
                    entry.chat, entry.thread, entry.message * 2, ACCOUNT if entry.outgoing else entry.sender + 1,
                    timestamp, entry.text, entry.reply_to * 2 if entry.reply_to else None,
                    outgoing=entry.outgoing, bot=entry.bot,
                    username="replay_group", chat_known=True, addressed=entry.addressed,
                )
                await gateway.observe(event.chat, event.thread, event.message, event.outgoing)
                if not await memory.record(event):
                    report(event, "outgoing_bot_or_duplicate")
                    continue
                key = (event.chat, event.thread)
                previous = planner.jobs.get(key)
                oldest = next(iter(planner.jobs.items()), None)
                if not planner.offer(event):
                    report(event, "local_filter")
                elif previous:
                    report(previous[1], "debounced")
                elif oldest and oldest[0] not in planner.jobs:
                    report(oldest[1][1], "planner_capacity")
            await drain(clock.now + 3)
    print(json.dumps({"mode": mode.value, "events": sum(outcomes.values()), "outcomes": outcomes,
                      "elapsed_seconds": round(time.perf_counter() - started, 3), "transport": "mock"}))
