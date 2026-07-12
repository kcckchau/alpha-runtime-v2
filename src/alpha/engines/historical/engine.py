"""
Engine 1 — Historical Data Engine

Responsibilities:
  - Fetch historical OHLCV bars, trades, and quotes via registered sources
  - Validate timestamps and detect gaps
  - Normalize all raw data into the standard event pipeline
  - Emit BarEvent / TradeEvent / QuoteEvent to the EventBus
  - Support multi-symbol parallel fetches
  - Resample bars across timeframes on demand

This engine is used for:
  - HISTORICAL_BACKFILL mode (write raw data to storage)
  - REPLAY mode (re-run events through the live pipeline)
  - LIVE / PAPER catch-up (fill bars from last session to present)

All emitted events carry `metadata.is_replay = True` so downstream
engines can distinguish historical events from real-time ones.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from alpha.calendar.base import SessionCalendar
from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.historical.sources.base import HistoricalDataSource
from alpha.models.enums import BarTimeframe, HealthStatus
from alpha.models.events import BarEvent, QuoteEvent, TradeEvent

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class GapRecord:
    def __init__(self, symbol: str, start: datetime, end: datetime) -> None:
        self.symbol = symbol
        self.start = start
        self.end = end
        self.duration = end - start

    def __repr__(self) -> str:
        return f"Gap({self.symbol} {self.start}–{self.end})"


class HistoricalDataEngine(BaseEngine):
    """
    Fetches, validates, and emits historical market data.

    Usage::

        engine = HistoricalDataEngine(settings, event_bus, registry, calendar)
        engine.register_source(PolygonDataSource(settings))
        await engine.initialize()
        await engine.start()

        async for event in engine.fetch_bars("AAPL", BarTimeframe.M1, start, end):
            ...  # events also flow through the EventBus automatically
    """

    def __init__(
        self,
        settings: AlphaSettings,
        event_bus: EventBus,
        registry: SymbolRegistry,
        calendar: SessionCalendar,
    ) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._registry = registry
        self._calendar = calendar
        self._sources: dict[str, HistoricalDataSource] = {}
        self._gaps: list[GapRecord] = []
        self._bars_emitted: int = 0

    @property
    def name(self) -> str:
        return "HistoricalDataEngine"

    # ── Source registration ───────────────────────────────────────────────────

    def register_source(self, source: HistoricalDataSource) -> None:
        self._sources[source.source_id] = source
        logger.info("Registered historical source: %s", source.source_id)

    def get_source(self, source_id: str) -> HistoricalDataSource:
        if source_id not in self._sources:
            raise KeyError(f"No historical source registered: {source_id}")
        return self._sources[source_id]

    @property
    def primary_source(self) -> HistoricalDataSource:
        sid = self._settings.historical.primary_source
        return self.get_source(sid)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        for source in self._sources.values():
            if not await source.ping():
                logger.warning("Historical source %s is unreachable", source.source_id)

    async def _on_start(self) -> None:
        pass  # fetches are driven externally by BootstrapEngine

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        details = {
            "bars_emitted": self._bars_emitted,
            "gaps_detected": len(self._gaps),
            "sources": list(self._sources.keys()),
        }
        return EngineHealth(HealthStatus.HEALTHY, self.name, details)

    # ── Public fetch API ──────────────────────────────────────────────────────

    async def fetch_bars(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        source_id: str | None = None,
        emit: bool = True,
    ) -> list[BarEvent]:
        """
        Fetch, validate, and optionally emit bars to the event bus.

        Returns the list of emitted events so callers can inspect them.
        """
        source = self._sources.get(source_id or self._settings.historical.primary_source)
        if source is None:
            raise RuntimeError(f"No source available for id={source_id}")

        events: list[BarEvent] = []
        prev_ts: datetime | None = None

        async for event in source.fetch_bars(symbol, timeframe, start, end):
            self._validate_bar_event(event, prev_ts)
            prev_ts = event.timestamp
            events.append(event)
            if emit:
                await self._event_bus.publish(event)
                self._bars_emitted += 1

        logger.debug(
            "Fetched %d bars for %s [%s] %s→%s",
            len(events), symbol, timeframe, start.date(), end.date(),
        )
        return events

    async def fetch_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        source_id: str | None = None,
        emit: bool = True,
    ) -> list[TradeEvent]:
        """Fetch, and optionally emit, trade ticks."""
        source = self._sources.get(source_id or self._settings.historical.primary_source)
        if source is None:
            raise RuntimeError(f"No source available for id={source_id}")

        events: list[TradeEvent] = []
        async for event in source.fetch_trades(symbol, start, end):
            events.append(event)
            if emit:
                await self._event_bus.publish(event)

        logger.debug(
            "Fetched %d trades for %s %s→%s", len(events), symbol, start.date(), end.date(),
        )
        return events

    async def fetch_quotes(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        source_id: str | None = None,
        emit: bool = True,
    ) -> list[QuoteEvent]:
        """Fetch, and optionally emit, top-of-book (MBP-1) quotes."""
        source = self._sources.get(source_id or self._settings.historical.primary_source)
        if source is None:
            raise RuntimeError(f"No source available for id={source_id}")

        events: list[QuoteEvent] = []
        async for event in source.fetch_quotes(symbol, start, end):
            events.append(event)
            if emit:
                await self._event_bus.publish(event)

        logger.debug(
            "Fetched %d quotes for %s %s→%s", len(events), symbol, start.date(), end.date(),
        )
        return events

    async def fetch_bars_multi(
        self,
        symbols: list[str],
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
    ) -> None:
        """Fetch bars for multiple symbols concurrently."""
        tasks = [
            self.fetch_bars(sym, timeframe, start, end, emit=True)
            for sym in symbols
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # ── Validation ────────────────────────────────────────────────────────────

    def _validate_bar_event(self, event: BarEvent, prev_ts: datetime | None) -> None:
        if prev_ts is not None and event.timestamp <= prev_ts:
            logger.warning(
                "Out-of-order bar for %s: %s <= %s",
                event.symbol, event.timestamp, prev_ts,
            )

        if prev_ts is not None:
            gap = event.timestamp - prev_ts
            max_gap = timedelta(seconds=self._settings.historical.max_gap_seconds)
            if gap > max_gap:
                rec = GapRecord(event.symbol, prev_ts, event.timestamp)
                self._gaps.append(rec)
                logger.warning("Gap detected: %s", rec)

    @property
    def gaps(self) -> list[GapRecord]:
        return list(self._gaps)
