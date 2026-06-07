"""
JSON file historical data source.

Reads 1-minute OHLCV bars from vendor-exported JSON files with the layout:

    {
      "symbol": "QQQ",
      "tradingDate": "2026-06-03",
      "timezone": "America/New_York",
      "sessions": {
        "premarket":  { "candles": [ { "time", "open", "high", "low", "close", "volume" }, ... ] },
        "regular":    { "candles": [ ... ] },
        "aftermarket": { "candles": [ ... ] }
      }
    }

File lookup convention:
    {data_dir}/{SYMBOL}/{YYYY-MM-DD}.json

Only BarTimeframe.M1 is natively supported. Requesting any other timeframe
raises NotImplementedError — use the aggregation helpers in the historical
engine or API layer to resample.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import AsyncIterator

from alpha.engines.historical.sources.base import HistoricalDataSource
from alpha.models.enums import BarTimeframe, DataSourceId
from alpha.models.events import BarEvent, EventMetadata, QuoteEvent, TradeEvent

logger = logging.getLogger(__name__)

_UTC = timezone.utc

# Session order for consistent chronological output
_SESSION_ORDER = ("premarket", "regular", "aftermarket")


class JSONFileHistoricalSource(HistoricalDataSource):
    """
    Reads pre-downloaded daily JSON bar files.

    Designed as an offline-first alternative to broker-based sources:
    drop a file at ``{data_dir}/{SYMBOL}/{YYYY-MM-DD}.json`` and backfill
    works without any live broker connection.

    Parameters
    ----------
    data_dir:
        Root directory containing per-symbol subdirectories.
        E.g. ``Path("data/imports")``.
    """

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir

    # ── HistoricalDataSource interface ────────────────────────────────────────

    @property
    def source_id(self) -> DataSourceId:
        return DataSourceId.JSON_FILE

    @property
    def supports_trades(self) -> bool:
        return False

    @property
    def supports_quotes(self) -> bool:
        return False

    async def ping(self) -> bool:
        return self._data_dir.exists()

    # ── Fetch ─────────────────────────────────────────────────────────────────

    async def fetch_bars(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        adjust: bool = True,
    ) -> AsyncIterator[BarEvent]:
        if timeframe != BarTimeframe.M1:
            raise NotImplementedError(
                f"JSONFileHistoricalSource only provides 1m bars "
                f"(requested {timeframe}). Resample in the engine layer."
            )

        symbol = symbol.upper()
        start_utc = start.astimezone(_UTC)
        end_utc = end.astimezone(_UTC)

        received_at = datetime.now(_UTC)

        # Walk every calendar date covered by [start, end]
        from datetime import timedelta
        current = start_utc.date()
        end_date = end_utc.date()

        while current <= end_date:
            path = self._data_dir / symbol / f"{current.isoformat()}.json"
            if path.exists():
                async for event in self._read_file(path, symbol, start_utc, end_utc, received_at):
                    yield event
            else:
                logger.debug("JSONFileSource: no file for %s %s", symbol, current)
            current += timedelta(days=1)

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _read_file(
        self,
        path: Path,
        symbol: str,
        start_utc: datetime,
        end_utc: datetime,
        received_at: datetime,
    ) -> AsyncIterator[BarEvent]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("JSONFileSource: failed to read %s: %s", path, exc)
            return

        sessions: dict = data.get("sessions", {})

        # Emit sessions in chronological order (premarket → regular → aftermarket)
        for session_name in _SESSION_ORDER:
            session = sessions.get(session_name)
            if not session:
                continue
            candles: list[dict] = session.get("candles", [])
            for candle in candles:
                event = self._candle_to_bar_event(candle, symbol, received_at)
                if event is None:
                    continue
                if event.timestamp < start_utc or event.timestamp > end_utc:
                    continue
                yield event

        # Also handle any sessions not in the expected list
        for session_name, session in sessions.items():
            if session_name in _SESSION_ORDER:
                continue
            candles = session.get("candles", []) if isinstance(session, dict) else []
            for candle in candles:
                event = self._candle_to_bar_event(candle, symbol, received_at)
                if event is None:
                    continue
                if event.timestamp < start_utc or event.timestamp > end_utc:
                    continue
                yield event

    @staticmethod
    def _candle_to_bar_event(
        candle: dict,
        symbol: str,
        received_at: datetime,
    ) -> BarEvent | None:
        try:
            raw_time: str = candle["time"]
            # Parse ISO8601 with UTC offset (e.g. "2026-06-03T04:00:00-0400")
            ts = datetime.fromisoformat(raw_time).astimezone(_UTC)

            return BarEvent(
                symbol=symbol,
                timestamp=ts,
                metadata=EventMetadata(
                    source=DataSourceId.JSON_FILE,
                    received_at=received_at,
                    is_replay=True,
                ),
                timeframe=BarTimeframe.M1,
                open=Decimal(str(candle["open"])),
                high=Decimal(str(candle["high"])),
                low=Decimal(str(candle["low"])),
                close=Decimal(str(candle["close"])),
                # volume is stored as float in the vendor format — normalise to int
                volume=int(candle["volume"]),
            )
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("JSONFileSource: skipping malformed candle %s: %s", candle, exc)
            return None

    # ── Convenience ───────────────────────────────────────────────────────────

    def available_dates(self, symbol: str) -> list[str]:
        """Return sorted list of YYYY-MM-DD strings that have a file on disk."""
        base = self._data_dir / symbol.upper()
        if not base.exists():
            return []
        return sorted(
            f.stem
            for f in base.glob("*.json")
            if len(f.stem) == 10  # YYYY-MM-DD
        )

    def has_date(self, symbol: str, d: "date") -> bool:  # type: ignore[name-defined]
        from datetime import date
        return (self._data_dir / symbol.upper() / f"{d.isoformat()}.json").exists()

    def fetch_trades(self, symbol: str, start: datetime, end: datetime) -> AsyncIterator[TradeEvent]:
        raise NotImplementedError("JSONFileHistoricalSource does not support trade ticks")
        return
        yield  # type: ignore[misc]

    def fetch_quotes(self, symbol: str, start: datetime, end: datetime) -> AsyncIterator[QuoteEvent]:
        raise NotImplementedError("JSONFileHistoricalSource does not support quotes")
        return
        yield  # type: ignore[misc]
