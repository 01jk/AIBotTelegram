from enum import StrEnum
from pathlib import Path
from typing import Annotated
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PositiveID = Annotated[int, Field(gt=0, le=2**63 - 1)]


class Mode(StrEnum):
    OBSERVE = "OBSERVE"
    DRAFT = "DRAFT"
    AUTO = "AUTO"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8-sig",
        extra="ignore",
        validate_default=True,
    )

    mode: Mode = Mode.OBSERVE
    dry_run: bool = True
    timezone: str = "Europe/Kyiv"
    data_dir: Path = Path("data")
    telegram_session_path: Path | None = None

    api_id: Annotated[int, Field(gt=0, le=2**31 - 1)] | None = None
    api_hash: SecretStr | None = None
    expected_account_id: PositiveID | None = None
    owner_telegram_id: PositiveID | None = None
    control_bot_token: SecretStr | None = None
    admin_ids: tuple[PositiveID, ...] = ()
    openrouter_api_key: SecretStr | None = None
    openrouter_api_key_file: Path | None = None
    models_path: Path = Path("models.yaml")
    persona_path: Path = Path("prompts/persona.yaml")
    style_guide_path: Path = Path("prompts/style_guide.md")
    memory_retention_days: int = Field(default=30, ge=1, le=365)
    digest_enabled: bool = True
    digest_time: str = Field(default="19:00", pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")
    opportunity_weights: tuple[int, int, int, int, int] = (25, 30, 20, 15, 10)

    @field_validator("opportunity_weights")
    @classmethod
    def validate_weights(cls, values):
        if min(values) < 0 or sum(values) != 100:
            raise ValueError("opportunity weights must be nonnegative and sum to 100")
        return values

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError("expected an IANA time zone") from None
        return value

    @property
    def database_path(self) -> Path:
        return self.data_dir / "companion.sqlite3"

    @property
    def session_path(self) -> Path:
        return self.telegram_session_path or self.data_dir / "telegram.session"
