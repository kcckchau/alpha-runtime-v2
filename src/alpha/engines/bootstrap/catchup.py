"""
CatchupService — historical bar fetch and context warm-up for the bootstrap pipeline.

Bootstrap runs in three phases:

  Phase 1 — DISCOVERING
    fetch_m1_history(): fetch M1 bars for the warmup window (past days, day-cached).
    historical_watermark(): find the latest M1 bar timestamp → gateway replay start.

  Phase 2 — WARMING
    warm_context(): load D1/H1 from Parquet cache, resample M5 from M1, emit all
    bars through the pipeline in dependency order (D1 → H1 → M5 → M1).

  Phase 3 — ACTIVATING (in BootstrapEngine)
    drain gateway buffer, skip overlap bars (timestamp ≤ watermark), reconcile
    active setups, enable external side effects, mark READY.

Today's bars are NOT fetched here — the live gateway connects with
start = watermark - 1m and replays the gap to real-time.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from alpha.calendar.resolver import calendar_for_symbol
from alpha.models.enums import AssetClass, BarTimeframe
from alpha.models.events import BarEvent, EventMetadata

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


def _resample_m5(m1_bars: list[Any]) -> list[BarEvent]:
    """Aggregate M1 bars into M5 bars by 5-minute boundary.

    Databento has no ohlcv-5m schema — M5 is always derived from M1 here.
    Groups by flooring each bar's timestamp to the nearest 5-minute mark.
    """
    if not m1_bars:
        return []

    groups: dict[datetime, list[Any]] = defaultdict(list)
    for bar in m1_bars:
        ts = bar.timestamp
        bucket = ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)
        groups[bucket].append(bar)

    m5_bars: list[BarEvent] = []
    for bucket_ts in sorted(groups):
        bars = sorted(groups[bucket_ts], key=lambda b: b.timestamp)
        m5_bars.append(BarEvent(
            symbol=bars[0].symbol,
            timestamp=bucket_ts,
            timeframe=BarTimeframe.M5,
            open=bars[0].open,
            high=max(b.high for b in bars),
            low=min(b.low for b in bars),
            close=bars[-1].close,
            volume=sum(b.volume for b in bars),
            metadata=EventMetadata(
                source=bars[0].metadata.source,
                received_at=bars[0].metadata.received_at,
                is_replay=True,
            ),
        ))

    return m5_bars


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

    # ── Phase 1: DISCOVERING ──────────────────────────────────────────────────

    async def fetch_m1_history(self, symbols: list[str]) -> dict[str, list[Any]]:
        """Fetch M1 bars for the warmup window (past days only, day-level cache).

        Does NOT emit — bars are emitted later by warm_context() after D1/H1
        context is loaded, ensuring engines receive bars in dependency order.

        Returns dict[symbol → list[BarEvent]].
        """
        hist = self._settings.historical
        now = datetime.now(timezone.utc)
        yesterday = now.date() - timedelta(days=1)
        end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=timezone.utc)

        vwap_start = self.session_start(now)
        m1_days = max(3, hist.minute1_warmup_bars // 390 + 1)
        m1_start = min(end - timedelta(days=m1_days), vwap_start)

        result: dict[str, list[Any]] = {}
        for symbol in symbols:
            bars = await self._fetch_bars_day_cached(
                symbol=symbol,
                timeframe=BarTimeframe.M1,
                start=m1_start,
                end=end,
                emit=False,
            )
            logger.info(
                "M1 history fetched for %s | bars=%d | window=%s → %s",
                symbol, len(bars), m1_start.date(), end.date(),
            )
            result[symbol] = bars

        return result

    def historical_watermark(self, m1_bars_by_symbol: dict[str, list[Any]]) -> datetime | None:
        """Return the latest M1 bar timestamp across all symbols.

        This is the handoff boundary: the live gateway replays from
        (watermark - 1m) to cover the gap between historical and real-time.
        """
        timestamps = [
            bar.timestamp
            for bars in m1_bars_by_symbol.values()
            for bar in bars
        ]
        return max(timestamps) if timestamps else None

    # ── Phase 2: WARMING ──────────────────────────────────────────────────────

    async def warm_context(
        self,
        symbols: list[str],
        m1_bars_by_symbol: dict[str, list[Any]],
    ) -> dict[str, dict[str, list[Any]]]:
        """Load H1/D1 from cache, resample M5 from M1, emit all in dependency order.

        Emit order: D1 → H1 → M5 → M1 (each as is_replay=True).
        Higher-timeframe context must be present before lower-timeframe bars
        flow through SetupEngine so structural levels and EMAs are ready.

        Returns dict[symbol → {minute_bars, hourly_bars, daily_bars}].
        """
        hist = self._settings.historical
        now = datetime.now(timezone.utc)
        yesterday = now.date() - timedelta(days=1)
        end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=timezone.utc)

        h1_start = end - timedelta(days=max(60, hist.hourly_warmup_bars // 23 + 15))
        d1_start = end - timedelta(days=int(hist.daily_warmup_bars * 1.5))

        result: dict[str, dict[str, list[Any]]] = {}

        for symbol in symbols:
            m1_bars = m1_bars_by_symbol.get(symbol, [])

            daily_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.D1,
                start=d1_start,
                end=end,
                emit=False,
            )
            hourly_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.H1,
                start=h1_start,
                end=end,
                emit=False,
            )
            minute5_bars = _resample_m5(m1_bars)

            # Emit in dependency order so downstream engines have structural
            # context before processing lower-timeframe setup signals.
            for bar in daily_bars:
                await self._event_bus.publish(_force_replay(bar))
            for bar in hourly_bars:
                await self._event_bus.publish(_force_replay(bar))
            for bar in minute5_bars:
                await self._event_bus.publish(_force_replay(bar))
            for bar in m1_bars:
                await self._event_bus.publish(_force_replay(bar))

            logger.info(
                "Warm context complete for %s | 1m=%d 5m=%d 1h=%d 1d=%d",
                symbol, len(m1_bars), len(minute5_bars), len(hourly_bars), len(daily_bars),
            )

            result[symbol] = {
                "minute_bars": m1_bars,
                "hourly_bars": hourly_bars,
                "daily_bars": daily_bars,
            }

        return result

    # ── BackfillEngine API ────────────────────────────────────────────────────

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

    # ── Session helpers ───────────────────────────────────────────────────────

    def session_start(self, now: datetime) -> datetime:
        """Return the start of the current (or most recent completed) VWAP session in UTC."""
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/New_York")
        now_et = now.astimezone(tz)
        today = now_et.date()

        if self._settings.historical.vwap_session == "extended":
            hour, minute = 4, 0
        else:
            hour, minute = 9, 30

        session_open_et = now_et.replace(hour=hour, minute=minute, second=0, microsecond=0)

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
        """Day-level cache for M1 catchup.

        Past days with an existing Parquet file → load from disk (0 API calls).
        Days with no file → 1 Databento call for that full day → save to Parquet.
        """
        all_bars: list[Any] = []

        current_day = start.date()
        last_day = end.date()
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
                    "Fetching %s %s %s | %s → %s",
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
                await self._event_bus.publish(_force_replay(bar))

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
            "Catch-up %s %s | stored=%d | missing_ranges=%d | window=%s → %s",
            symbol, timeframe, len(stored), len(missing_ranges),
            start.isoformat(), end.isoformat(),
        )

        fetched: list[Any] = []
        for gap_start, gap_end in missing_ranges:
            logger.info(
                "Fetching gap for %s %s | %s → %s",
                symbol, timeframe, gap_start.isoformat(), gap_end.isoformat(),
            )
            bars = await self._historical.fetch_bars(
                symbol=symbol,
                timeframe=timeframe,
                start=gap_start,
                end=gap_end,
                emit=False,
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
            symbol, timeframe, len(fetched), len(merged),
        )

        if emit:
            for bar in merged:
                await self._event_bus.publish(_force_replay(bar))

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
    return last < end
