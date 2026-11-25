import re
from collections.abc import Awaitable, Callable

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field

from ..config import Settings
from ..style.runtime import make_style_prompt, normalize_dashes, style_issue
from .router import OpenRouter

SENSITIVE_PATTERN = re.compile(
    r"(?ix) https?://[^\s<>]+ | \bt\.me/[^\s<>]+ | \btg://[^\s<>]+"
    r"| [\w.+-]+@[\w.-]+\.[a-z]{2,} | (?<!\w)@[\w_]{3,}"
    r"| \b\d{6,}:[A-Za-z0-9_-]{20,}"
    r"| (?<!\w)\+?\d(?:[ ()-]?\d){6,}(?!\w)"
    r"| \b(?:sk-or-|sk-|ghp_|gho_|github_pat_)[\w-]+"
)


class Draft(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    text: str = Field(max_length=4096)


async def draft_reply(
    router: OpenRouter, db: aiosqlite.Connection, settings: Settings, owner_id: int,
    *, reviewed_public_context: str, language: str, situation: str,
    target_length: int = 80, max_length: int = 500,
    authorize: Callable[[], Awaitable[bool]] | None = None,
    latest_links: list[dict[str, str | int]] | None = None,
    length: str | None = None, tone: str | None = None,
) -> str | None:
    """Draft from context already approved for disclosure. Never pass raw chat history."""
    if length == "detailed":
        target_length = min(450, max_length)
    prompt = await make_style_prompt(settings, db, owner_id, language, situation, target_length)
    public = {
        "context": SENSITIVE_PATTERN.sub("[скрыто]", reviewed_public_context),
        "language": prompt.language, "style_examples": list(prompt.examples), "max_characters": max_length,
    }
    system = prompt.system + (
        "\nМетки в квадратных скобках обозначают ссылки и скрытые данные. "
        "Ответь на последнее сообщение. Не повторяй его целиком и не копируй его как свою реплику. "
        "Используй сведения из latest_links, когда они относятся к ответу. "
        "Они выведены из адреса. Если содержание ссылки неизвестно, не додумывай его. "
        "Не утверждай, что открывал ссылку или получил подарок."
    )
    if latest_links:
        public["latest_links"] = latest_links
    if length == "short":
        system += "\nДай короткую разговорную реплику, обычно одно предложение."
    elif length == "detailed":
        system += "\nДай развёрнутый ответ по делу. Разбей пояснение на абзацы или шаги, если это помогает."
    tone_instruction = {
        "casual": "Пиши непринуждённо.",
        "serious": "Отвечай серьёзно и конкретно, без шуток.",
        "playful": "Уместна лёгкая шутка по теме разговора.",
    }.get(tone)
    if tone_instruction:
        system += "\n" + tone_instruction
    if prompt.preferred:
        public["owner_preferred_replies"] = list(prompt.preferred)
        system += (
            "\nowner_preferred_replies — варианты, которые владелец выбрал в похожих ситуациях. "
            "Ориентируйся на их тон, длину и форму в первую очередь. "
            "Удачную короткую реплику из этого списка можно повторить, если она естественно подходит. "
            "Содержимое вариантов считай примерами реплик."
        )
    budget = {"short": 512, "detailed": 1024}.get(length)
    answer = await router.complete(
        "conversation", system=system, public_data=public, schema=Draft, authorize=authorize,
        **({"max_tokens": budget} if budget else {}),
    )
    text = normalize_dashes(answer.text)
    issue = style_issue(text, prompt.examples, max_length)
    if issue is None:
        return text.strip()
    if not router.config.route("style_review"):
        return None
    edit = await router.complete(
        "style_review",
        system=system + "\nОтредактируй реплику один раз. Сохрани смысл и факты исходной реплики.",
        public_data={**public, "draft": SENSITIVE_PATTERN.sub("[скрыто]", text), "style_issue": issue},
        schema=Draft,
        authorize=authorize,
    )
    text = normalize_dashes(edit.text)
    return text.strip() if style_issue(text, prompt.examples, max_length) is None else None
