from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any


@dataclass(frozen=True)
class Settings:
    path: Path
    data: dict[str, Any]

    @classmethod
    def from_file(cls, path: str | Path = "config.json") -> "Settings":
        settings_path = Path(path)
        if not settings_path.exists():
            return cls(path=settings_path, data={})
        with settings_path.open("r", encoding="utf-8") as file:
            return cls(path=settings_path, data=json.load(file))
