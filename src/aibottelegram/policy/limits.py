import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiosqlite

# Each setting has a default and a maximum valid input.
GLOBAL_LIMITS = {
    "global_daily": (0, 100000),
    "global_rolling": (0, 100000),
    "proactive_daily": (0, 100000),
    "min_interval": (60, 86400),
    "switch_cooldown": (120, 86400),
    "conversation_idle": (300, 86400),
    "conversation_max": (900, 86400),
    "candidate_ttl": (120, 86400),
    "max_length": (500, 4096),
    "quiet_start": (0, 1439),
    "quiet_end": (0, 1439),
    "manual_cooldown": (300, 86400),
}
CHAT_LIMITS = {
    "daily": (3, 100000),
    "rolling": (3, 100000),
    "proactive_daily": (0, 100000),
    "min_interval": (180, 86400),
    "allow_links": (0, 1),
}


async def load_limits(db: aiosqlite.Connection, account_id: int, chat_id: int = 0) -> dict[str, int]:
    definitions = CHAT_LIMITS if chat_id else GLOBAL_LIMITS
    values = {key: definition[0] for key, definition in definitions.items()}
    rows = await db.execute_fetchall(
        "SELECT name, value FROM send_limits WHERE account_id = ? AND chat_id = ?",
        (account_id, chat_id),
    )
    values.update(rows)
    return values


def parse_limit(name: str, value: str, chat_id: int) -> int | None:
    definitions = CHAT_LIMITS if chat_id else GLOBAL_LIMITS
    if name not in definitions:
        return None
    if name in {"quiet_start", "quiet_end"}:
        if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value):
            return None
        hours, minutes = map(int, value.split(":"))
        return hours * 60 + minutes
    if not re.fullmatch(r"[0-9]{1,6}", value):
        return None
    number = int(value)
    return number if number <= definitions[name][1] else None


def format_limit(name: str, value: int) -> str:
    if name in {"quiet_start", "quiet_end"}:
        return f"{value // 60:02d}:{value % 60:02d}"
    return str(value)


def local_day(now: int, zone: ZoneInfo) -> tuple[int, int]:
    local = datetime.fromtimestamp(now, zone)
    start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp()), int((start + timedelta(days=1)).timestamp())


def is_quiet(now: int, zone: ZoneInfo, start: int, end: int) -> bool:
    local = datetime.fromtimestamp(now, zone)
    minute = local.hour * 60 + local.minute
    if start <= end:
        return start <= minute < end
    return minute >= start or minute < end
