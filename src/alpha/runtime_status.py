from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from alpha.config.settings import AlphaSettings

logger = logging.getLogger(__name__)


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

    # Side-effect: persist session contexts to per-date JSON files so the API
    # can serve historical dates without re-running setup detection.
    try:
        data_root = settings.storage.parquet_root.parent
        _persist_session_contexts(data_root, payload)
    except Exception:
        logger.warning("Failed to persist session contexts from snapshot", exc_info=True)

    return path


def _persist_session_contexts(data_root: Path, payload: dict[str, Any]) -> None:
    from alpha.engines.storage.session_context_store import SessionContextStore
    store = SessionContextStore(data_root)

    for ctx_key in ("setup_contexts", "prev_setup_contexts"):
        contexts = payload.get(ctx_key) or {}
        for symbol, ctx_data in contexts.items():
            if not ctx_data:
                continue
            session_date_str = ctx_data.get("session_date")
            if not session_date_str:
                continue
            try:
                d = date.fromisoformat(str(session_date_str))
                store.save_raw(symbol, d, ctx_data)
            except (ValueError, KeyError, TypeError):
                logger.debug("Skipping context persist for %s: bad session_date", symbol)


def read_snapshot(settings: AlphaSettings) -> dict[str, Any] | None:
    path = snapshot_path(settings)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
