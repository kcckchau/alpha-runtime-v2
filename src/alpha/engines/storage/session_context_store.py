"""
Per-date session context persistence.

Stores the full SessionSetupContext JSON for each symbol × trading date so that
historical dates can be served without re-running the setup detection pipeline.

Layout:
    {data_root}/session_contexts/{SYMBOL}/{YYYY-MM-DD}.json
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from alpha.models.setup import SessionSetupContext

logger = logging.getLogger(__name__)


class SessionContextStore:
    """
    Read / write SessionSetupContext objects keyed by (symbol, trading_date).

    Separate from ParquetStore — these are small JSON summaries, not columnar
    event data. They are written by the runtime when a session rolls (or after
    a backfill replay) and read by the API when a historical date is requested.
    """

    def __init__(self, data_root: Path) -> None:
        self._root = data_root / "session_contexts"

    # ── Paths ─────────────────────────────────────────────────────────────────

    def _path(self, symbol: str, d: date) -> Path:
        return self._root / symbol.upper() / f"{d.isoformat()}.json"

    # ── Write ─────────────────────────────────────────────────────────────────

    def save(self, symbol: str, d: date, context: SessionSetupContext) -> None:
        """Serialize and persist a SessionSetupContext for symbol+date."""
        path = self._path(symbol, d)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(context.model_dump_json(indent=2), encoding="utf-8")
        logger.debug("Saved session context: %s %s → %s", symbol, d, path)

    def save_raw(self, symbol: str, d: date, data: dict[str, Any]) -> None:
        """Persist an already-serialized context dict (e.g. from runtime snapshot)."""
        path = self._path(symbol, d)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        logger.debug("Saved raw session context: %s %s → %s", symbol, d, path)

    # ── Read ──────────────────────────────────────────────────────────────────

    def load(self, symbol: str, d: date) -> SessionSetupContext | None:
        """Return SessionSetupContext for symbol+date, or None if not stored."""
        path = self._path(symbol, d)
        if not path.exists():
            return None
        try:
            return SessionSetupContext.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to load session context: %s %s", symbol, d, exc_info=True)
            return None

    def load_raw(self, symbol: str, d: date) -> dict[str, Any] | None:
        """Return raw dict for symbol+date, or None if not stored."""
        path = self._path(symbol, d)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[return-value]
        except Exception:
            logger.warning("Failed to load raw session context: %s %s", symbol, d, exc_info=True)
            return None

    def load_prev(self, symbol: str, d: date) -> SessionSetupContext | None:
        """
        Return the most recent session context strictly before `d`.
        Walks backward up to 7 calendar days to handle weekends and holidays.
        """
        for delta in range(1, 8):
            candidate = d - timedelta(days=delta)
            ctx = self.load(symbol, candidate)
            if ctx is not None:
                return ctx
        return None

    def load_prev_raw(self, symbol: str, d: date) -> dict[str, Any] | None:
        """Raw-dict variant of load_prev."""
        for delta in range(1, 8):
            candidate = d - timedelta(days=delta)
            raw = self.load_raw(symbol, candidate)
            if raw is not None:
                return raw
        return None

    # ── Introspection ─────────────────────────────────────────────────────────

    def exists(self, symbol: str, d: date) -> bool:
        return self._path(symbol, d).exists()

    def list_dates(self, symbol: str) -> list[date]:
        base = self._root / symbol.upper()
        if not base.exists():
            return []
        dates: list[date] = []
        for f in sorted(base.glob("*.json")):
            try:
                dates.append(date.fromisoformat(f.stem))
            except ValueError:
                pass
        return sorted(dates)
