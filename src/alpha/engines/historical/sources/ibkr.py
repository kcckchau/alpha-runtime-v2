"""
IBKR historical data source.

Fetches bars via reqHistoricalData, chunking large date ranges to stay within
IBKR's pacing limits (typically 60 requests per 10 minutes).

Pacing notes:
  - IBKR blocks requests that exceed 60 historical data requests per 10 min
  - We wait `settings.ibkr.pacing_delay` seconds between chunks
  - 1-min bars: max 7 days per request → 30-day backfill = ~5 requests
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from alpha.config.settings import IBKRSettings
from alpha.core.registry import SymbolRegistry
from alpha.engines.historical.sources.base import HistoricalDataSource
from alpha.engines.ibkr.connection import IBKRConnection
from alpha.engines.ibkr.contracts import (
    MAX_CHUNK_DAYS,
    TIMEFRAME_TO_IBKR,
    make_contract,
    normalize_bar,
)
from alpha.models.enums import BarTimeframe, DataSourceId
from alpha.models.events import BarEvent

logger = logging.getLogger(__name__)

_UTC = timezone.utc


class IBKRHistoricalDataSource(HistoricalDataSource):
    """
    Fetches and normalizes historical bars from IBKR.

    Handles:
      - Multi-chunk fetching for large date ranges
      - Pacing delay between requests
      - Filtering bars outside the requested [start, end] window
    """

    def __init__(self, conn: IBKRConnection, registry: SymbolRegistry, settings: IBKRSettings) -> None:
        self._conn = conn
        self._registry = registry
        self._settings = settings

    @property
    def source_id(self) -> DataSourceId:
        return DataSourceId.INTERACTIVE_BROKERS

    @property
    def supports_trades(self) -> bool:
        return False  # IBKR tick data requires separate subscription

    @property
    def supports_quotes(self) -> bool:
        return False

    async def ping(self) -> bool:
        # Don't initiate a connection just to ping — check current state only.
        # The real connection happens on the first fetch_bars() call.
        return self._conn.is_connected

    async def fetch_bars(  # type: ignore[override]
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        adjust: bool = True,
    ) -> AsyncIterator[BarEvent]:
        ib = await self._conn.get()
        sym = self._registry.get(symbol)
        contract = make_contract(sym)
        bar_size = TIMEFRAME_TO_IBKR[timeframe]
        chunk_days = MAX_CHUNK_DAYS.get(timeframe, 7)

        # Build list of (chunk_start, chunk_end) pairs, newest first
        chunks: list[tuple[datetime, datetime]] = []
        chunk_end = end
        while chunk_end > start:
            chunk_start = max(start, chunk_end - timedelta(days=chunk_days))
            chunks.append((chunk_start, chunk_end))
            chunk_end = chunk_start

        # Fetch oldest-first so bars are yielded in chronological order
        for chunk_start, chunk_end in reversed(chunks):
            days = max(1, (chunk_end - chunk_start).days)
            duration = f"{days} D"

            logger.debug(
                "Fetching %s %s [%s] %s → %s (%s)",
                symbol, timeframe, bar_size, chunk_start.date(), chunk_end.date(), duration,
            )

            try:
                raw_bars = await ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=chunk_end.strftime("%Y%m%d %H:%M:%S") + " UTC",
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow=self._settings.what_to_show,
                    useRTH=self._settings.use_rth,
                    formatDate=2,   # 2 = return datetime objects
                )
            except Exception as exc:
                logger.error("IBKR reqHistoricalData error for %s: %s", symbol, exc)
                continue

            for raw in raw_bars:
                event = normalize_bar(raw, symbol, timeframe, is_replay=True)
                if start <= event.timestamp <= end:
                    yield event

            if len(chunks) > 1:
                await asyncio.sleep(self._settings.pacing_delay)
