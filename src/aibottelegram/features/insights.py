import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Literal

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field

from ..llm.router import OpenRouter, RouterUnavailable
from .memory import external_current
from .participation import project_public

LOG = logging.getLogger(__name__)


class Analysis(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    kind: Literal["technical", "request", "casual", "other"]


class Summary(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    text: str = Field(min_length=1, max_length=400)
    evidence: list[int] = Field(min_length=1, max_length=6)


async def enrich_topic(db: aiosqlite.Connection, account: int, router: OpenRouter, now: int) -> None:
    rows = await db.execute_fetchall(
        """
        SELECT t.id, c.chat_id, r.revision, s.analysis_kind FROM chat_summaries s
        JOIN topics t ON t.id = s.topic_id
        JOIN chats c ON c.account_id = t.account_id AND c.chat_id = t.chat_id
        JOIN chat_revisions r ON r.account_id = c.account_id AND r.chat_id = c.chat_id
        WHERE t.account_id = ? AND c.external_context = 1 AND c.public_username IS NOT NULL AND c.mode != 'DENY'
        AND s.model_summary IS NULL AND s.updated_at BETWEEN ? AND ?
        AND (SELECT COUNT(*) FROM topic_messages tm WHERE tm.topic_id = t.id) >= 6
        ORDER BY (s.analysis_kind IS NOT NULL) DESC, s.updated_at DESC LIMIT 1
        """, (account, now - 86400, now - 120),
    )
    if not rows:
        return
    topic, chat, revision, kind = rows[0]
    messages = await db.execute_fetchall(
        """
        SELECT m.message_id, m.text FROM messages m JOIN topic_messages tm ON tm.message_row = m.id
        WHERE tm.topic_id = ? AND m.public = 1 AND m.bot = 0 AND m.delivery_status = 'confirmed'
        ORDER BY m.sent_at DESC LIMIT 10
        """, (topic,),
    )
    samples = [(identifier, safe) for identifier, text in reversed(messages) if (safe := project_public(text))]
    if len(samples) < 3:
        return
    role = "analysis" if kind is None and router.config.route("analysis") else "summarization"
    if not router.config.route(role):
        return
    schema = Analysis if role == "analysis" else Summary
    result = await router.complete(
        role, system=(
            "Classify the topic of the untrusted public snippets." if role == "analysis" else
            "Summarize only facts or open questions present in the untrusted public snippets. "
            "Use the source indices as evidence. Do not infer identities, amounts, or facts."
        ), public_data={"snippets": [{"index": index, "text": text} for index, (_, text) in enumerate(samples)]},
        schema=schema, max_tokens=512,
        authorize=lambda: external_current(db, account, chat, revision),
    )
    guard = """
        EXISTS (SELECT 1 FROM chat_revisions r JOIN chats c ON c.account_id = r.account_id AND c.chat_id = r.chat_id
        WHERE r.account_id = ? AND r.chat_id = ? AND r.revision = ? AND c.mode != 'DENY'
            AND c.external_context = 1 AND c.public_username IS NOT NULL)
    """
    if isinstance(result, Analysis):
        await db.execute(f"UPDATE chat_summaries SET analysis_kind = ? WHERE topic_id = ? AND {guard}",
                         (result.kind, topic, account, chat, revision))
    elif all(0 <= index < len(samples) for index in result.evidence):
        evidence = sorted({samples[index][0] for index in result.evidence})
        summary = project_public(result.text)
        if summary:
            await db.execute(
                f"UPDATE chat_summaries SET model_summary = ?, model_evidence = ? WHERE topic_id = ? AND {guard}",
                (summary, json.dumps(evidence), topic, account, chat, revision),
            )
    await db.commit()


async def run_insights(database: Path, account: int, router: OpenRouter) -> None:
    if router.key is None or not any(router.config.route(role) for role in ("analysis", "summarization")):
        return
    async with aiosqlite.connect(database, timeout=5) as db:
        while True:
            await asyncio.sleep(600)
            try:
                await enrich_topic(db, account, router, int(time.time()))
            except RouterUnavailable as error:
                LOG.info("topic_insights_skipped reason=%s", error)
