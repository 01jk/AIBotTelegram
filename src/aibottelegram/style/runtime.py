import json
import re
from dataclasses import dataclass
from pathlib import Path

import aiosqlite
import yaml

from ..config import Settings
from .profile import classify, safe_example

PROTECTED = re.compile(r"`[^`\n]+`|(?:https?://|tg://)[^\s<>]+", re.I)
MARKUP = re.compile(r"(?m)^\s*(?:#{1,6}\s|`{3})|\*\*")
TEMPLATES = re.compile(
    r"^(?:конечно[,!]|отличный вопрос|давайте разбер[её]мся|"
    r"варто зазначити|чудове питання|як мовна модель|как языковая модель)",
    re.I,
)
INTERNAL = re.compile(r"(?i)\b(?:system prompt|openrouter_api_key|api_hash|control_bot_token)\b")
TONES = {"разговорный", "спокойный", "нейтральный", "розмовний", "спокійний", "нейтральний"}


class PersonaError(ValueError):
    pass


def read_persona(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8-sig") as file:
        raw = file.read(65537)
    if len(raw) > 65536:
        raise PersonaError("persona file exceeds 64 KiB")
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError:
        raise PersonaError("invalid persona YAML") from None
    if not isinstance(data, dict) or data.get("version") != 1:
        raise PersonaError("unsupported persona format")
    return data


def bundled_source(settings: Settings, owner_id: int) -> Path | None:
    persona = read_persona(settings.persona_path)
    source = persona.get("style_corpus")
    if not isinstance(source, dict):
        return None
    if source.get("owner_id") != owner_id or source.get("usage") != "local_only":
        raise PersonaError("bundled style corpus does not declare the configured owner")
    path = source.get("path")
    if not isinstance(path, str):
        raise PersonaError("invalid style corpus path")
    # The persona file resides in the prompts directory under the project root.
    root = settings.persona_path.resolve().parent.parent
    result = (root / path).resolve()
    if not result.is_relative_to(root):
        raise PersonaError("bundled style corpus must be inside the project")
    return result


async def select_examples(
    db: aiosqlite.Connection, owner_id: int, language: str, situation: str, target_length: int,
) -> list[str]:
    rows = await db.execute_fetchall(
        """
        SELECT text FROM style_examples
        WHERE owner_id = ? AND (language = ? OR language = 'und' OR ? = 'und')
        GROUP BY fingerprint
        ORDER BY (language != ?), (situation != ?), ABS(length - ?), fingerprint
        LIMIT 5
        """,
        (owner_id, language, language, language, situation, target_length),
    )
    return [text for (text,) in rows if safe_example(text, classify(text)[2])]


@dataclass(frozen=True)
class StylePrompt:
    system: str
    examples: tuple[str, ...]
    language: str
    preferred: tuple[str, ...]


async def make_style_prompt(
    settings: Settings, db: aiosqlite.Connection, owner_id: int, language: str,
    situation: str, target_length: int,
) -> StylePrompt:
    if language not in {"ru", "uk", "und"}:
        raise ValueError("unsupported style language")
    persona = read_persona(settings.persona_path)
    with settings.style_guide_path.open(encoding="utf-8") as file:
        guide = file.read(8193)
    if len(guide) > 8192 or not guide.strip():
        raise PersonaError("invalid runtime style guide")
    examples = await select_examples(db, owner_id, language, situation, target_length)
    if len(examples) < 3 and isinstance(persona.get("examples"), list):
        for text in persona["examples"]:
            if isinstance(text, str) and text not in examples:
                lang, _, words = classify(text)
                if (language == "und" or lang in {language, "und"}) and safe_example(text, words):
                    examples.append(text)
            if len(examples) >= 5:
                break
    tone = persona.get("tone")
    preference = f"Тон: {tone}. " if isinstance(tone, str) and tone in TONES else ""
    rows = await db.execute_fetchall(
        """
        SELECT text FROM style_preferences
        WHERE owner_id = ? AND situation = ? AND (language = ? OR language = 'und' OR ? = 'und')
        GROUP BY text
        ORDER BY MAX(language = ?) DESC, MAX(updated_at) DESC, MAX(draft_id) DESC
        LIMIT 3
        """, (owner_id, situation, language, language, language),
    )
    preferred = tuple(text for (text,) in rows)
    preferred_normalized = {" ".join(text.casefold().split()) for text in preferred}
    examples = [text for text in examples if " ".join(text.casefold().split()) not in preferred_normalized]
    return StylePrompt(f"{guide}\n{preference}", tuple(examples), language, preferred)


def normalize_dashes(text: str) -> str:
    parts = []
    start = 0
    for match in PROTECTED.finditer(text):
        parts.extend((text[start:match.start()].replace("–", "-").replace("—", "-"), match[0]))
        start = match.end()
    parts.append(text[start:].replace("–", "-").replace("—", "-"))
    return "".join(parts)


def style_issue(text: str, examples: tuple[str, ...], max_length: int) -> str | None:
    try:
        length = len(text.encode("utf-16-le")) // 2
    except UnicodeEncodeError:
        return "invalid_unicode"
    if not text.strip() or length > max_length:
        return "length"
    visible = PROTECTED.sub("", text)
    if "–" in visible or "—" in visible or MARKUP.search(visible):
        return "punctuation_or_markup"
    if TEMPLATES.search(visible.strip()) or INTERNAL.search(text):
        return "template_or_internal"
    normalized = " ".join(text.casefold().split())
    if any(normalized == " ".join(example.casefold().split()) for example in examples):
        return "copied_example"
    lines = [" ".join(line.casefold().split()) for line in text.splitlines() if line.strip()]
    if len(lines) != len(set(lines)):
        return "repetition"
    return None


async def style_status(db: aiosqlite.Connection, owner_id: int) -> str:
    rows = await db.execute_fetchall("SELECT report FROM style_sources WHERE owner_id = ?", (owner_id,))
    [(examples,)] = await db.execute_fetchall(
        "SELECT COUNT(DISTINCT fingerprint) FROM style_examples WHERE owner_id = ?", (owner_id,)
    )
    [(preferences,)] = await db.execute_fetchall(
        "SELECT COUNT(*) FROM style_preferences WHERE owner_id = ?", (owner_id,)
    )
    preference_status = f"Предпочтений из /prefer: {preferences}."
    if not rows:
        return f"Профиль стиля ещё не импортирован. {preference_status}"
    messages = sum(json.loads(report)["message_count"] for (report,) in rows)
    return f"Источников: {len(rows)}. Реплик в профилях: {messages}. Примеров стиля: {examples}. {preference_status}"
