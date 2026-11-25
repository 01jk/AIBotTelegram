import json
import re
from dataclasses import dataclass

import aiosqlite

WORD = re.compile(r"[\w-]+", re.UNICODE)
COMMON = set("этот этом будет было просто почему чтобы вообще здесь вопрос только теперь такое тоді взагалі можна зараз вони люди сказал пишет сказать".split())
BRANCH = re.compile(r"(?i)\b(?:другая тема|другой вопрос|інша тема|інше питання|к слову|до речі)\b")


def keywords(text: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(word for word in WORD.findall(text.casefold())
                              if len(word) >= 4 and word.isalpha() and word not in COMMON))[:12]


@dataclass(frozen=True)
class TopicManager:
    db: aiosqlite.Connection

    async def assign(self, account: int, chat: int, thread: int, message_row: int, text: str,
                     reply_to: int | None, timestamp: int) -> int:
        terms = set(keywords(text))
        linked = []
        if reply_to is not None:
            linked = await self.db.execute_fetchall(
                """
                SELECT tm.topic_id FROM topic_messages tm JOIN messages m ON m.id = tm.message_row
                WHERE m.account_id = ? AND m.chat_id = ? AND m.thread_id = ? AND m.message_id = ?
                LIMIT 1
                """, (account, chat, thread, reply_to),
            )
        recent = await self.db.execute_fetchall(
            """
            SELECT id, keywords, last_at FROM topics WHERE account_id = ? AND chat_id = ? AND thread_id = ?
            AND status != 'closed' ORDER BY last_at DESC LIMIT 8
            """, (account, chat, thread),
        )
        parent = linked[0][0] if linked else (recent[0][0] if recent else None)
        topic = linked[0][0] if linked and not BRANCH.search(text) else None
        if topic is None and not BRANCH.search(text):
            matches = [(len(terms.intersection(json.loads(words))), last, identifier)
                       for identifier, words, last in recent]
            score, _, candidate = max(matches, default=(0, 0, None))
            if score >= 2:
                topic = candidate
            elif recent and timestamp - recent[0][2] <= 180 and (score or len(terms) <= 1):
                topic = recent[0][0]
        if topic is None:
            title = " ".join(keywords(text)[:4]) or "Разговор"
            [(topic,)] = await self.db.execute_fetchall(
                """
                INSERT INTO topics (account_id, chat_id, thread_id, title, keywords, parent_id, status, last_at)
                VALUES (?, ?, ?, ?, ?, ?, 'active', ?) RETURNING id
                """, (account, chat, thread, title, json.dumps(keywords(text)), parent if BRANCH.search(text) else None, timestamp),
            )
        await self.db.execute(
            "UPDATE topics SET status = 'dormant' WHERE account_id = ? AND chat_id = ? AND thread_id = ? AND status = 'active' AND id != ?",
            (account, chat, thread, topic),
        )
        await self.db.execute(
            "UPDATE topics SET status = 'active', last_at = MAX(last_at, ?) WHERE id = ?", (timestamp, topic),
        )
        await self.db.execute("INSERT OR IGNORE INTO topic_messages VALUES (?, ?)", (topic, message_row))
        return topic

    async def rebuild(self, topic: int) -> None:
        rows = await self.db.execute_fetchall(
            """
            SELECT m.message_id, m.text, m.sent_at, m.direction, m.reply_to FROM messages m
            JOIN topic_messages tm ON tm.message_row = m.id WHERE tm.topic_id = ?
            AND m.delivery_status = 'confirmed' ORDER BY m.sent_at DESC, m.message_id DESC LIMIT 12
            """, (topic,),
        )
        if not rows:
            await self.db.execute("UPDATE topics SET status = 'closed', title = '', keywords = '[]' WHERE id = ?", (topic,))
            await self.db.execute("DELETE FROM chat_summaries WHERE topic_id = ?", (topic,))
            return
        replies = {reply for _, _, _, _, reply in rows if reply is not None}
        entries = [{"id": mid, "text": text[:200], "kind": (
            "account_reply" if direction == "outgoing" else
            "open_question" if "?" in text and mid not in replies else "source"
        )} for mid, text, _, direction, _ in reversed(rows)]
        # Refresh searchable episode labels so edits and deletions cannot leave old terms behind.
        terms = tuple(dict.fromkeys(term for _, text, _, _, _ in rows for term in keywords(text)))[:12]
        await self.db.execute(
            "UPDATE topics SET title = ?, keywords = ?, last_at = ? WHERE id = ?",
            (" ".join(terms[:4]) or "Разговор", json.dumps(terms), rows[0][2], topic),
        )
        await self.db.execute(
            """
            INSERT INTO chat_summaries (topic_id, evidence, summary, updated_at) VALUES (?, ?, ?, ?)
            ON CONFLICT(topic_id) DO UPDATE SET
                evidence = excluded.evidence, summary = excluded.summary, updated_at = excluded.updated_at,
                analysis_kind = NULL, model_summary = NULL, model_evidence = NULL
            """, (topic, json.dumps([row[0] for row in rows]), json.dumps(entries, ensure_ascii=False), rows[0][2]),
        )
