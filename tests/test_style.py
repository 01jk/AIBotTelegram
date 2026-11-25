import json
from pathlib import Path
from unittest.mock import AsyncMock

import aiosqlite
import pytest

from aibottelegram.config import Mode, Settings
from aibottelegram.llm.config import ModelsConfig
from aibottelegram.llm.drafting import Draft, draft_reply
from aibottelegram.storage.database import initialize
from aibottelegram.style.importer import StyleImportError, import_style
from aibottelegram.style.runtime import select_examples, style_issue

OWNER = 42


def message(identifier, text, **fields):
    return {"id": identifier, "type": "message", "from_id": "user42", "text": text, **fields}


@pytest.mark.asyncio
async def test_export_is_streamed_filtered_and_replaced_by_hash(tmp_path):
    db_path = tmp_path / "style.sqlite3"
    path = tmp_path / "result.json"
    export = {"chats": {"list": [
        {"id": 1, "messages": [
            message(1, ["А ", {"type": "bold", "text": "чому так?"}]),
            message(1, "А чому так?"),
            message(2, "Дякую", forwarded_from="someone"),
            {**message(3, "Дякую"), "from_id": "user7"},
            message(4, "Мій приватний номер +380000000000"),
        ]},
        {"id": 2, "messages": [message(1, "Чому саме?")]},
    ]}, "left_chats": {"list": [{"id": 3, "messages": [message(1, "А як саме?")]}]}}
    path.write_text(json.dumps(export, ensure_ascii=False), encoding="utf-8-sig")
    async with aiosqlite.connect(db_path) as db:
        await initialize(db, Mode.OBSERVE)
        first = import_style(db_path, path, OWNER)
        assert (first["message_count"], first["example_count"], first["changed"]) == (4, 3, True)
        assert not import_style(db_path, path, OWNER)["changed"]
        picked = await select_examples(db, OWNER, "uk", "question", 11)
        assert set(picked) == {"А чому так?", "Чому саме?", "А як саме?"}
        path.write_text(json.dumps({"messages": [message(1, "Спасибо")]}, ensure_ascii=False), encoding="utf-8")
        assert import_style(db_path, path, OWNER)["changed"]
        assert await db.execute_fetchall("SELECT text FROM style_examples") == [("Спасибо",)]
        path.write_text('{"messages":[', encoding="utf-8")
        with pytest.raises(StyleImportError):
            import_style(db_path, path, OWNER)
        assert await db.execute_fetchall("SELECT text FROM style_examples") == [("Спасибо",)]


def test_style_validator_checks_prose_without_altering_code_or_urls():
    assert style_issue("Ось `x—y` і https://example.org/a–b", (), 100) is None
    assert style_issue("Тут — звичайний текст", (), 100) == "punctuation_or_markup"
    assert style_issue("  чому так? ", ("Чому так?",), 100) == "copied_example"


@pytest.mark.asyncio
async def test_drafting_uses_filtered_examples_and_one_style_edit(tmp_path):
    guide = tmp_path / "guide.md"
    guide.write_text("Пиши коротко.", encoding="utf-8")
    settings = Settings(_env_file=None, persona_path=tmp_path / "missing.yaml", style_guide_path=guide)
    fake = AsyncMock()
    fake.config = ModelsConfig.model_validate({"models": {"style_review": {"primary": "example/test:free"}}})
    fake.complete.side_effect = [Draft(text="Отличный вопрос!"), Draft(text="Так, можемо.")]
    async with aiosqlite.connect(tmp_path / "style.sqlite3") as db:
        await initialize(db, Mode.OBSERVE)
        await db.execute("INSERT INTO style_examples VALUES (42, 'fixture', '1', 'uk', 'question', 9, 'Чому так?')")
        result = await draft_reply(
            fake, db, settings, OWNER, reviewed_public_context="Питання @sample_contact",
            language="uk", situation="question",
        )
    assert result == "Так, можемо."
    assert [item.args[0] for item in fake.complete.await_args_list] == ["conversation", "style_review"]
    prompt = fake.complete.await_args_list[0].kwargs["public_data"]
    assert prompt["style_examples"] == ["Чому так?"]
    assert "@sample_contact" not in prompt["context"]
