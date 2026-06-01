from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel

_CONFIG_PATH = Path(__file__).parent / "settings.yaml"


class ExchangeConfig(BaseModel):
    name: str
    ws_base: str
    rest_base: str


class CaptureConfig(BaseModel):
    book_depth: int = 20
    data_dir: str = "data"


class Settings(BaseModel):
    exchange: ExchangeConfig
    symbols: list[str]
    capture: CaptureConfig


def load_settings(path: Path = _CONFIG_PATH) -> Settings:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Settings.model_validate(raw)


settings = load_settings()
