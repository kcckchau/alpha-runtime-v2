from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from alpha.config.settings import AlphaSettings


def snapshot_path(settings: AlphaSettings) -> Path:
    data_root = settings.storage.parquet_root.parent
    return data_root / "runtime" / "status.json"


def write_snapshot(settings: AlphaSettings, payload: dict[str, Any]) -> Path:
    path = snapshot_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=str(path.parent),
        delete=False,
    ) as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        temp_path = Path(tmp.name)
    temp_path.replace(path)
    return path


def read_snapshot(settings: AlphaSettings) -> dict[str, Any] | None:
    path = snapshot_path(settings)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
