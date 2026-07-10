"""
CatchupService — historical bar fetch/emit logic for the bootstrap pipeline.

Handles day-aware caching and gap detection for past days.
Today's bars are intentionally skipped here — the live feed connects with
start=session_open so it replays today's bars from the gateway (24h replay
window) and then transitions to real-time seamlessly.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from alpha.calendar.resolver import calendar_for_symbol
from alpha.models.enums import AssetClass, BarTimeframe

if TYPE_CHECKING:
    from alpha.calendar.base import SessionCalendar
    from alpha.config.settings import AlphaSettings
    from alpha.core.event_bus import EventBus
    from alpha.core.registry import SymbolRegistry
    from alpha.engines.historical.engine import HistoricalDataEngine
    from alpha.engines.storage.engine import StorageEngine

logger = logging.getLogger(__name__)


def _force_replay(bar: Any) -> Any:
    if bar.metadata.is_replay:
        return bar
    return bar.model_copy(update={"metadata": bar.metadata.model_copy(update={"is_replay": True})})


def _timeframe_delta(timeframe: BarTimeframe) -> timedelta:
    mapping = {
        BarTimeframe.M1: timedelta(minutes=1),
        BarTimeframe.M5: timedelta(minutes=5),
        BarTimeframe.H1: timedelta(hours=1),
        BarTimeframe.D1: timedelta(days=1),
    }
    return mapping[timeframe]


class CatchupService:
    """Encapsulates all historical catch-up logic for the bootstrap sequence."""

    def __init__(
        self,
        settings: "AlphaSettings",
        storage: "StorageEngine",
        historical: "HistoricalDataEngine",
        event_bus: "EventBus",
        registry: "SymbolRegistry",
        calendar: "SessionCalendar",
    ) -> None:
        self._settings = settings
        self._storage = storage
        self._historical = historical
        self._event_bus = event_bus
        self._registry = registry
        self._calendar = calendar

    # ── Public API ────────────────────────────────────────────────────────────

    async def run(self, symbols: list[str]) -> dict[str, dict[str, list]]:
        """Load recent history for all symbols before the live feed connects.

        Returns a dict keyed by symbol, each containing minute_bars, hourly_bars,
        daily_bars lists (matching the keys expected by build_symbol_context).
        """
        hist = self._settings.historical
        logger.info(
            "Running catch-up | symbols=%s | 1m_bars=%d | 5m_bars=%d | 1h_bars=%d | 1d_bars=%d | vwap=%s",
            symbols,
            hist.minute1_warmup_bars,
            hist.minute5_warmup_bars,
            hist.hourly_warmup_bars,
            hist.daily_warmup_bars,
            hist.vwap_session,
        )

        # Stop at end of yesterday — today's bars come from the live feed replay.
        now = datetime.now(timezone.utc)
        yesterday = now.date() - timedelta(days=1)
        end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=timezone.utc)
        vwap_start = self.session_start(now)

        m1_days = max(3, hist.minute1_warmup_bars // 390 + 1)
        m5_days = max(7, hist.minute5_warmup_bars // 78 + 2)
        m1_start = min(end - timedelta(days=m1_days), vwap_start)
        m5_start = min(end - timedelta(days=m5_days), vwap_start)
        h1_start = end - timedelta(days=int(hist.hourly_warmup_bars * 0.22) + 30)
        d1_start = end - timedelta(days=int(hist.daily_warmup_bars * 1.5))

        result: dict[str, dict[str, list]] = {}

        for symbol in symbols:
            logger.info("Catch-up starting for %s", symbol)
            symbol_def = self._registry.get(symbol)
            symbol_d1_start = d1_start
            if symbol_def.asset_class == AssetClass.FUTURE:
                # Avoid multi-year expired-contract daily backfills on futures during startup.
                symbol_d1_start = max(d1_start, end - timedelta(days=45))

            minute_bars = await self._fetch_bars_day_cached(
                symbol=symbol,
                timeframe=BarTimeframe.M1,
                start=m1_start,
                end=end,
                emit=True,
            )
            minute5_bars = await self._fetch_bars_day_cached(
                symbol=symbol,
                timeframe=BarTimeframe.M5,
                start=m5_start,
                end=end,
                emit=True,
            )
            hourly_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.H1,
                start=h1_start,
                end=end,
                emit=False,
            )
            daily_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.D1,
                start=symbol_d1_start,
                end=end,
                emit=False,
            )

            logger.info(
                "Catch-up complete for %s | 1m=%d 5m=%d 1h=%d 1d=%d",
                symbol,
                len(minute_bars),
                len(minute5_bars),
                len(hourly_bars),
                len(daily_bars),
            )

            result[symbol] = {
                "minute_bars": minute_bars,
                "hourly_bars": hourly_bars,
                "daily_bars": daily_bars,
            }

        return result

    async def fetch_range(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
    ) -> list[Any]:
        """Fetch a bar range without emitting. Used by BackfillEngine."""
        return await self._load_or_fetch_bars(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            emit=False,
        )

    def session_start(self, now: datetime) -> datetime:
        """Return the start of the current (or most recent completed) VWAP session in UTC.

        Ensures M1/M5 catch-up windows always reach back to the session open so that
        session-VWAP can be computed accurately from the first bar.
        """
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/New_York")
        now_et = now.astimezone(tz)
        today = now_et.date()

        if self._settings.historical.vwap_session == "extended":
            hour, minute = 4, 0
        else:  # "rth" (default): regular session opens at 09:30 ET
            hour, minute = 9, 30

        session_open_et = now_et.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # If we haven't reached today's open yet, fall back to the previous trading day
        if now_et < session_open_et:
            prev_day = self._calendar.prev_trading_day(today)
            session_open_et = session_open_et.replace(
                year=prev_day.year, month=prev_day.month, day=prev_day.day
            )

        return session_open_et.astimezone(timezone.utc)

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch_bars_day_cached(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        emit: bool,
    ) -> list[Any]:
        """Day-level cache strategy for M1/M5 catchup.

        Covers past days only (up to end of yesterday). Today's bars come from
        the live feed gateway replay.

        Days with an existing Parquet file → load from storage (0 Databento calls).
        Days with no file → 1 Databento call per missing day.
        """
        all_bars: list[Any] = []

        current_day = start.date()
        last_day = end.date()  # end is always end-of-yesterday from run()
        while current_day <= last_day:
            if await self._storage.has_bars(symbol, timeframe, current_day):
                day_bars = await self._storage.load_bar_events(symbol, timeframe, current_day, current_day)
                all_bars.extend(day_bars)
                logger.info(
                    "Day cache hit %s %s %s | bars=%d",
                    symbol, timeframe, current_day, len(day_bars),
                )
            else:
                day_start = max(
                    start,
                    datetime(current_day.year, current_day.month, current_day.day, tzinfo=timezone.utc),
                )
                day_end = datetime(current_day.year, current_day.month, current_day.day, 23, 59, 59, tzinfo=timezone.utc)
                logger.info(
                    "Fetching %s %s %s | %s -> %s",
                    symbol, timeframe, current_day,
                    day_start.isoformat(), day_end.isoformat(),
                )
                bars = await self._historical.fetch_bars(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=day_start,
                    end=day_end,
                    emit=False,
                )
                for bar in bars:
                    await self._storage.save_bar(bar)
                all_bars.extend(bars)

            current_day += timedelta(days=1)

        all_bars.sort(key=lambda b: b.timestamp)

        if emit:
            for bar in all_bars:
                bar = _force_replay(bar)
                await self._event_bus.publish(bar)

        return all_bars

    async def _load_or_fetch_bars(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        emit: bool,
    ) -> list[Any]:
        stored = await self._storage.load_bar_events(symbol, timeframe, start.date(), end.date())
        stored = [bar for bar in stored if start <= bar.timestamp <= end]
        missing_ranges = self._missing_ranges(symbol, stored, start, end, timeframe)
        logger.info(
            "Catch-up %s %s | stored=%d | missing_ranges=%d | window=%s -> %s",
            symbol,
            timeframe,
            len(stored),
            len(missing_ranges),
            start.isoformat(),
            end.isoformat(),
        )

        fetched: list[Any] = []
        for gap_start, gap_end in missing_ranges:
            logger.info(
                "Fetching gap for %s %s | %s -> %s",
                symbol,
                timeframe,
                gap_start.isoformat(),
                gap_end.isoformat(),
            )
            bars = await self._historical.fetch_bars(
                symbol=symbol,
                timeframe=timeframe,
                start=gap_start,
                end=gap_end,
                emit=False,  # Always defer — we emit the full merged set below in order.
            )
            fetched.extend(bars)
            for bar in bars:
                await self._storage.save_bar(bar)

        merged_by_ts = {bar.timestamp: bar for bar in stored}
        for bar in fetched:
            merged_by_ts[bar.timestamp] = bar

        merged = sorted(merged_by_ts.values(), key=lambda bar: bar.timestamp)
        logger.info(
            "Catch-up %s %s complete | fetched=%d | merged_total=%d",
            symbol,
            timeframe,
            len(fetched),
            len(merged),
        )

        if emit:
            # Emit all bars in strict chronological order (stored + fetched).
            # Previously only gap bars were emitted as they were fetched, which meant
            # stored bars never flowed through BarFlowAggregator → BarPipeline →
            # SetupEngine, leaving _session_contexts empty after a restart.
            #
            # Force is_replay=True regardless of what is stored in metadata.
            # Stored bars from a previous live session have is_replay=False; re-emitting
            # them without this flag causes SetupEngine to emit "live" SetupEvents,
            # triggering Telegram notifications for historical setups during catchup.
            for bar in merged:
                bar = _force_replay(bar)
                await self._event_bus.publish(bar)

        return merged

    def _missing_ranges(
        self,
        symbol: str,
        stored_bars: list[Any],
        start: datetime,
        end: datetime,
        timeframe: BarTimeframe,
    ) -> list[tuple[datetime, datetime]]:
        step = _timeframe_delta(timeframe)
        calendar = calendar_for_symbol(self._registry.get(symbol))
        relevant = sorted(
            [bar for bar in stored_bars if start <= bar.timestamp <= end],
            key=lambda bar: bar.timestamp,
        )
        if not relevant:
            return [(start, end)]

        missing: list[tuple[datetime, datetime]] = []

        first = relevant[0]
        if _should_fill_head_gap(calendar, timeframe, start, first.timestamp):
            missing.append((start, first.timestamp - step))

        for left, right in zip(relevant, relevant[1:]):
            expected_next = left.timestamp + step
            if _should_fill_internal_gap(calendar, timeframe, left.timestamp, right.timestamp, expected_next):
                missing.append((expected_next, right.timestamp - step))

        last = relevant[-1]
        if _should_fill_tail_gap(calendar, timeframe, last.timestamp, end):
            missing.append((last.timestamp + step, end))

        return [
            (gap_start, gap_end)
            for gap_start, gap_end in missing
            if gap_start <= gap_end
        ]


def _should_fill_head_gap(
    calendar: "SessionCalendar",
    timeframe: BarTimeframe,
    start: datetime,
    first: datetime,
) -> bool:
    # Always fill if data starts after the requested window — applies to all timeframes.
    return first > start


def _should_fill_internal_gap(
    calendar: "SessionCalendar",
    timeframe: BarTimeframe,
    left: datetime,
    right: datetime,
    expected_next: datetime,
) -> bool:
    if right <= expected_next:
        return False
    if timeframe in {BarTimeframe.H1, BarTimeframe.D1}:
        return calendar.session_key(left) == calendar.session_key(right)
    return True


def _should_fill_tail_gap(
    calendar: "SessionCalendar",
    timeframe: BarTimeframe,
    last: datetime,
    end: datetime,
) -> bool:
    # Always fill if stored data ends before the requested window — applies to all timeframes.
    return last < end
