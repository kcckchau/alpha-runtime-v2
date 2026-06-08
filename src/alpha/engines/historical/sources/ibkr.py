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
from datetime import date, datetime, time, timedelta, timezone
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
from alpha.instruments import quarterly_contract_month
from alpha.models.enums import AssetClass
from alpha.models.enums import BarTimeframe, DataSourceId
from alpha.models.events import BarEvent

logger = logging.getLogger(__name__)

_UTC = timezone.utc
_QUARTERLY_MONTHS = (3, 6, 9, 12)


def _third_friday(year: int, month: int) -> date:
    first_day = date(year, month, 1)
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    return first_friday + timedelta(weeks=2)


def _previous_roll_date(as_of: date) -> date:
    candidates = [
        _third_friday(year, month)
        for year in (as_of.year - 1, as_of.year)
        for month in _QUARTERLY_MONTHS
    ]
    return max(candidate for candidate in candidates if candidate < as_of)


def _roll_start_for_contract_month(contract_month: str) -> datetime:
    year = int(contract_month[:4])
    month = int(contract_month[4:6])
    return datetime.combine(_third_friday(year, month), time.min, tzinfo=_UTC)


def _future_contract_segments(start: datetime, end: datetime) -> list[tuple[datetime, datetime, str]]:
    """Split a futures lookback into front-month segments bounded by quarterly rolls."""
    segments: list[tuple[datetime, datetime, str]] = []
    segment_start = start

    while segment_start < end:
        contract_month = quarterly_contract_month(segment_start.date())
        roll_start = _roll_start_for_contract_month(contract_month)
        segment_end = min(end, roll_start)

        # If we're already at or past the active contract's roll boundary,
        # advance to the next loop iteration instead of stalling on a zero-width segment.
        if segment_end <= segment_start:
            segment_end = end

        segments.append((segment_start, segment_end, contract_month))
        if segment_end <= segment_start:
            break
        segment_start = segment_end

    return segments


def _should_retry_security_definition(exc: Exception) -> bool:
    return "No security definition has been found" in str(exc)


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
        bar_size = TIMEFRAME_TO_IBKR[timeframe]
        chunk_days = MAX_CHUNK_DAYS.get(timeframe, 7)

        if sym.asset_class == AssetClass.FUTURE:
            contract_segments = _future_contract_segments(start, end)
        else:
            contract_segments = [(start, end, sym.contract_month or "")]

        for segment_start, segment_end, contract_month in contract_segments:
            contract_symbol = (
                sym.model_copy(update={"contract_month": contract_month})
                if sym.asset_class == AssetClass.FUTURE
                else sym
            )
            contract = make_contract(contract_symbol)

            chunks: list[tuple[datetime, datetime]] = []
            chunk_end = segment_end
            while chunk_end > segment_start:
                chunk_start = max(segment_start, chunk_end - timedelta(days=chunk_days))
                chunks.append((chunk_start, chunk_end))
                chunk_end = chunk_start

            # Fetch oldest-first so bars are yielded in chronological order
            for chunk_start, chunk_end in reversed(chunks):
                # CME futures sessions span midnight UTC (18:00–17:00 ET).
                # With durationStr="1 D", IBKR returns the single trading
                # session active AT endDateTime — if that session started only
                # 2 hours ago we get just 120 bars.  Using at least "2 D"
                # guarantees the full calendar-day range is covered; the
                # start/end filter below trims the extra bars.
                days = max(2, (chunk_end - chunk_start).days)
                duration = f"{days} D"

                logger.debug(
                    "Fetching %s %s [%s] %s → %s (%s) contract=%s",
                    symbol,
                    timeframe,
                    bar_size,
                    chunk_start.date(),
                    chunk_end.date(),
                    duration,
                    contract_month or getattr(contract_symbol, "contract_month", ""),
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
                    if sym.asset_class == AssetClass.FUTURE and _should_retry_security_definition(exc):
                        fallback_month = quarterly_contract_month(chunk_end.date())
                        attempted_month = contract_month or getattr(contract_symbol, "contract_month", "")
                        if fallback_month and fallback_month != attempted_month:
                            fallback_symbol = sym.model_copy(update={"contract_month": fallback_month})
                            logger.warning(
                                "Retrying %s %s historical fetch with fallback contract month %s (was %s)",
                                symbol,
                                timeframe,
                                fallback_month,
                                attempted_month,
                            )
                            try:
                                raw_bars = await ib.reqHistoricalDataAsync(
                                    make_contract(fallback_symbol),
                                    endDateTime=chunk_end.strftime("%Y%m%d %H:%M:%S") + " UTC",
                                    durationStr=duration,
                                    barSizeSetting=bar_size,
                                    whatToShow=self._settings.what_to_show,
                                    useRTH=self._settings.use_rth,
                                    formatDate=2,
                                )
                            except Exception:
                                logger.error(
                                    "IBKR reqHistoricalData retry failed for %s contract=%s",
                                    symbol,
                                    fallback_month,
                                )
                                continue
                        else:
                            logger.error(
                                "IBKR reqHistoricalData error for %s contract=%s: %s",
                                symbol,
                                attempted_month,
                                exc,
                            )
                            continue
                    else:
                        logger.error(
                            "IBKR reqHistoricalData error for %s contract=%s: %s",
                            symbol,
                            contract_month or getattr(contract_symbol, "contract_month", ""),
                            exc,
                        )
                        continue

                for raw in raw_bars:
                    event = normalize_bar(raw, symbol, timeframe, is_replay=True)
                    if start <= event.timestamp <= end:
                        yield event

                if len(chunks) > 1:
                    await asyncio.sleep(self._settings.pacing_delay)
