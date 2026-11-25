from pathlib import Path

import pytest
from pydantic import ValidationError

from aibottelegram.config import Mode, Settings


def test_defaults(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for key in Settings.model_fields:
        monkeypatch.delenv(key.upper(), raising=False)
    settings = Settings()
    assert settings.mode is Mode.OBSERVE
    assert settings.dry_run is True
    assert settings.timezone == "Europe/Kyiv"
    assert settings.database_path == Path("data/companion.sqlite3")
    assert settings.session_path == Path("data/telegram.session")


def test_environment_overrides_dotenv(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "MODE=DRAFT\nDRY_RUN=false\nADMIN_IDS=[4294967296]\nAPI_HASH=example-secret\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MODE", "OBSERVE")
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.delenv("ADMIN_IDS", raising=False)
    monkeypatch.delenv("API_HASH", raising=False)

    settings = Settings()
    assert settings.mode is Mode.OBSERVE
    assert settings.dry_run is True
    assert settings.admin_ids == (4294967296,)
    assert "example-secret" not in repr(settings)


@pytest.mark.parametrize("overrides", [{"timezone": "Invalid/Zone"}, {"admin_ids": [0]}])
def test_invalid_configuration(overrides):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **overrides)
