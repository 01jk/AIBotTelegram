import re
from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ROLES = ("conversation", "participation", "analysis", "mining", "summarization", "style_review")
FREE_ID = re.compile(r"[a-z0-9][a-z0-9_.-]*/[a-z0-9][a-z0-9_.-]*:free")
PAID_ID = re.compile(r"[a-z0-9][a-z0-9_.-]*/[a-z0-9][a-z0-9_.-]*")
PROVIDER_TAG = re.compile(r"[a-z0-9][a-z0-9-]*(?:/[a-z0-9-]+)*")


class ModelConfigError(ValueError):
    pass


class Route(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    primary: str | None = None
    fallbacks: tuple[str, ...] = Field(default=(), max_length=3)

    @model_validator(mode="after")
    def validate_models(self):
        models = self.chain()
        if self.primary is None and self.fallbacks:
            raise ValueError("fallbacks require a primary model")
        if len(models) != len(set(models)) or any(
            len(name) > 96 or not (FREE_ID.fullmatch(name) or PAID_ID.fullmatch(name)) for name in models
        ):
            raise ValueError("routes require unique, explicit model IDs")
        return self

    def chain(self) -> tuple[str, ...]:
        return (self.primary, *self.fallbacks) if self.primary else ()


class PaidModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    provider: str = Field(pattern="^" + PROVIDER_TAG.pattern + "$", max_length=96)
    prompt_per_million: Decimal = Field(gt=0, le=1, allow_inf_nan=False)
    completion_per_million: Decimal = Field(gt=0, le=2, allow_inf_nan=False)
    temperature: float | None = Field(default=None, ge=0, le=2)
    reasoning: Literal["none"] | None = None

    def max_price(self) -> dict:
        return {"prompt": float(self.prompt_per_million), "completion": float(self.completion_per_million), "request": 0}


class ModelsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    free_only: bool = Field(default=True, strict=True)
    max_requests_per_minute: int = Field(default=20, ge=0, le=20, strict=True)
    max_requests_per_day: int = Field(default=50, ge=0, le=1000, strict=True)
    foreground_reserve_per_day: int = Field(default=15, ge=0, le=1000, strict=True)
    paid_requests_per_minute: int = Field(default=20, ge=0, le=60, strict=True)
    paid_requests_per_day: int = Field(default=100, ge=0, le=1000, strict=True)
    paid_models: dict[str, PaidModel] = Field(default_factory=dict)
    models: dict[str, Route] = Field(default_factory=dict)

    @field_validator("models")
    @classmethod
    def known_roles(cls, routes):
        if set(routes) - set(ROLES):
            raise ValueError("unknown role")
        if len({model for route in routes.values() for model in route.chain()}) > 12:
            raise ValueError("too many distinct models")
        return routes

    def route(self, role: str) -> tuple[str, ...]:
        return self.models[role].chain() if role in self.models else ()

    @model_validator(mode="after")
    def validate_paid_models(self):
        used = {model for route in self.models.values() for model in route.chain() if not model.endswith(":free")}
        if used != self.paid_models.keys() or (self.free_only and used):
            raise ValueError("each paid route requires explicit paid_models settings and free_only: false")
        return self


def load_models(path: Path) -> ModelsConfig:
    if not path.exists():
        return ModelsConfig()
    try:
        with path.open(encoding="utf-8-sig") as file:
            raw = file.read(65537)
        if len(raw) > 65536:
            raise ValueError
        return ModelsConfig.model_validate(yaml.safe_load(raw))
    except (OSError, ValueError, yaml.YAMLError):
        raise ModelConfigError("invalid models.yaml; check model IDs and paid route settings") from None
