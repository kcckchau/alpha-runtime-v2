"""
CatchupService — historical warm-up for the bootstrap pipeline.

Single entry point: run()

  1. Get per-schema availability edges from the vendor (one metadata call).
  2. Fetch M1: past days from Parquet day-cache, today from historical API.
  3. Fetch H1/D1: Parquet cache with gap-fill from API.
  4. Resample M5 from M1 (no native Databento 5m schema).
  5. Emit in dependency order: D1 → H1 → M5 → M1 (all is_replay=True).
  6. Return context map and M1 availability edge.

BootstrapEngine then starts the live feed from (m1_end - 1m) so the
gateway naturally replays the small gap and transitions to real-time.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from alpha.calendar.resolver import calendar_for_symbol
from alpha.features.volume_profile import VolumeProfileBuilder
from alpha.features.volume_profile_loader import VolumeProfileLoader
from alpha.models.enums import AssetClass, BarTimeframe
from alpha.models.events import BarEvent, EventMetadata

_ET = ZoneInfo("America/New_York")
_RTH_OPEN_H, _RTH_OPEN_M = 9, 30
_RTH_CLOSE_H, _RTH_CLOSE_M = 16, 0
_GLOBEX_START_H, _GLOBEX_START_M = 18, 0
# A session is considered sealed RTH if bars exist at/past this time (ET).
# Guards against building an incomplete profile mid-session.
_RTH_SEAL_H, _RTH_SEAL_M = 15, 55

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

    Databento has no ohlcv-5m schema — M5 is always derived from M1.
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
    """Fetches and emits all historical warm-up bars for the bootstrap sequence."""

    def __init__(
        self,
        settings: "AlphaSettings",
        storage: "StorageEngine",
        historical: "HistoricalDataEngine",
        event_bus: "EventBus",
        registry: "SymbolRegistry",
    ) -> None:
        self._settings = settings
        self._storage = storage
        self._historical = historical
        self._event_bus = event_bus
        self._registry = registry

    # ── Public API ────────────────────────────────────────────────────────────

    async def run(
        self,
        symbols: list[str],
    ) -> tuple[dict[str, dict[str, list[Any]]], datetime]:
        """Warm up all timeframes and emit in dependency order.

        Returns:
            context_map  — {symbol: {minute_bars, hourly_bars, daily_bars}}
            m1_end       — Databento M1 availability edge; use as live gateway
                           replay start so live naturally covers the final gap.
        """
        hist = self._settings.historical
        now = datetime.now(timezone.utc)

        # ── Availability edges (single metadata call) ─────────────────────────
        m1_end, h1_end, d1_end = self._availability_ends()
        logger.info(
            "Catchup availability edges | M1=%s | H1=%s | D1=%s",
            m1_end.isoformat(), h1_end.isoformat(), d1_end.isoformat(),
        )

        # ── Window calculations ───────────────────────────────────────────────
        # M1: always fetch fresh from API — no Parquet cache.
        # Cached M1 files may be incomplete (written mid-session, stale if vendor
        # was delayed). A fresh fetch ensures the warmup reflects exactly what
        # the vendor has available, regardless of when the process last ran.
        # 3-day M1 fetch takes ~6-10s; acceptable vs. serving stale data.
        m1_days = max(3, hist.minute1_warmup_bars // 390 + 1)
        m1_start = m1_end - timedelta(days=m1_days)

        h1_start = h1_end - timedelta(days=max(60, hist.hourly_warmup_bars // 23 + 15))
        d1_start = d1_end - timedelta(days=int(hist.daily_warmup_bars * 1.5))

        result: dict[str, dict[str, list[Any]]] = {}

        for symbol in symbols:
            logger.info("Catchup starting for %s", symbol)
            symbol_def = self._registry.get(symbol)

            # ── M1: always fetch fresh from historical API ────────────────────
            m1_bars = await self._historical.fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.M1,
                start=m1_start,
                end=m1_end,
                emit=False,
            )
            logger.info("M1 fetched for %s | bars=%d | %s → %s",
                        symbol, len(m1_bars), m1_start.date(), m1_end.isoformat())

            # Persist M1 bars to Parquet so the chart shows the full session on
            # next open. Unlike H1/D1 (_load_or_fetch_bars handles their storage),
            # M1 has no Parquet cache — it's always fetched fresh. Without this
            # step the StorageEngine's is_replay skip means M1 catchup bars never
            # land on disk, so the chart only shows data from when the live feed
            # started rather than from the beginning of the session.
            # Bars dedup on timestamp, so restarting overwrites stale rows cleanly.
            for bar in m1_bars:
                await self._storage.save_bar(bar)
            await self._storage.flush()

            # ── VP profiles: build from M1 bars for any sealed sessions ───────
            self._build_vp_profiles(symbol, m1_bars)

            # ── H1/D1: Parquet cache with gap-fill ────────────────────────────
            hourly_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.H1,
                start=h1_start,
                end=h1_end,
                emit=False,
            )
            symbol_d1_start = d1_start
            if symbol_def.asset_class == AssetClass.FUTURE:
                symbol_d1_start = max(d1_start, d1_end - timedelta(days=45))
            daily_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.D1,
                start=symbol_d1_start,
                end=d1_end,
                emit=False,
            )

            # ── M5: resample from M1 ──────────────────────────────────────────
            minute5_bars = _resample_m5(m1_bars)

            # ── Emit in dependency order ──────────────────────────────────────
            for bar in daily_bars:
                await self._event_bus.publish(_force_replay(bar))
            for bar in hourly_bars:
                await self._event_bus.publish(_force_replay(bar))
            for bar in minute5_bars:
                await self._event_bus.publish(_force_replay(bar))
            for bar in m1_bars:
                await self._event_bus.publish(_force_replay(bar))

            logger.info(
                "Catchup complete for %s | 1m=%d 5m=%d 1h=%d 1d=%d",
                symbol,
                len(m1_bars), len(minute5_bars), len(hourly_bars), len(daily_bars),
            )

            result[symbol] = {
                "minute_bars": m1_bars,
                "hourly_bars": hourly_bars,
                "daily_bars": daily_bars,
            }

        return result, m1_end

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

    # ── Volume Profile auto-build ─────────────────────────────────────────────

    def _build_vp_profiles(self, symbol: str, m1_bars: list[Any]) -> None:
        """
        Build VP profiles for sealed sessions found in the catchup M1 bars.

        Called after M1 bars are saved to Parquet, before they are emitted.
        Skips sessions that already have JSON profiles on disk.

        Source is always "bars" here — trade-based profiles are built by the
        offline build_volume_profiles.py script when trade data is available.
        Bar-based profiles are sufficient for POC/VA in the live engine.

        RTH seal gate: only build RTH if the session has a bar at/after 15:55 ET
        (guards against building an incomplete mid-session profile on restart).

        Globex completeness gate: only build Globex if prior-evening bars exist
        (18:00–23:59 ET on prior day), to avoid a silent half-session profile.
        """
        loader = VolumeProfileLoader(self._settings.storage.volume_profiles_root)
        builder = VolumeProfileBuilder()

        # Group bars by their ET calendar date
        bars_by_date: dict[date, list[Any]] = defaultdict(list)
        for bar in m1_bars:
            bars_by_date[bar.timestamp.astimezone(_ET).date()].append(bar)

        for session_date in sorted(bars_by_date.keys()):
            day_bars = bars_by_date[session_date]

            # ── RTH ──────────────────────────────────────────────────────────
            rth_path = loader._profile_path(symbol, session_date, "rth")
            if not rth_path.exists():
                rth_open = datetime(
                    session_date.year, session_date.month, session_date.day,
                    _RTH_OPEN_H, _RTH_OPEN_M, tzinfo=_ET,
                )
                rth_close = datetime(
                    session_date.year, session_date.month, session_date.day,
                    _RTH_CLOSE_H, _RTH_CLOSE_M, tzinfo=_ET,
                )
                rth_seal = datetime(
                    session_date.year, session_date.month, session_date.day,
                    _RTH_SEAL_H, _RTH_SEAL_M, tzinfo=_ET,
                )
                rth_bars = [b for b in day_bars if rth_open <= b.timestamp.astimezone(_ET) < rth_close]
                is_sealed = any(b.timestamp.astimezone(_ET) >= rth_seal for b in rth_bars)
                if rth_bars and is_sealed:
                    profile = builder.build(rth_bars, symbol, session_date, "rth")  # type: ignore[arg-type]
                    loader.save(symbol, profile)
                    logger.info(
                        "VP RTH [bars] built at startup | %s %s | POC=%s VA=[%s,%s] vol=%d",
                        symbol, session_date, profile.poc, profile.val, profile.vah, profile.total_volume,
                    )
                elif rth_bars:
                    logger.debug("VP RTH skipped | %s %s | session not yet sealed", symbol, session_date)

            # ── Globex ───────────────────────────────────────────────────────
            globex_path = loader._profile_path(symbol, session_date, "globex")
            if not globex_path.exists():
                prior_date = session_date - timedelta(days=1)
                prior_bars = bars_by_date.get(prior_date, [])

                globex_start = datetime(
                    prior_date.year, prior_date.month, prior_date.day,
                    _GLOBEX_START_H, _GLOBEX_START_M, tzinfo=_ET,
                )
                rth_open = datetime(
                    session_date.year, session_date.month, session_date.day,
                    _RTH_OPEN_H, _RTH_OPEN_M, tzinfo=_ET,
                )
                midnight = datetime(
                    session_date.year, session_date.month, session_date.day,
                    0, 0, tzinfo=_ET,
                )

                overnight_bars = [
                    b for b in prior_bars + day_bars
                    if globex_start <= b.timestamp.astimezone(_ET) < rth_open
                ]
                has_prior_evening = any(b.timestamp.astimezone(_ET) < midnight for b in overnight_bars)

                if overnight_bars and has_prior_evening:
                    profile = builder.build(overnight_bars, symbol, session_date, "globex")  # type: ignore[arg-type]
                    loader.save(symbol, profile)
                    logger.info(
                        "VP Globex [bars] built at startup | %s %s | POC=%s VA=[%s,%s] vol=%d",
                        symbol, session_date, profile.poc, profile.val, profile.vah, profile.total_volume,
                    )
                elif overnight_bars:
                    logger.debug(
                        "VP Globex skipped | %s %s | no prior-evening bars (incomplete session)",
                        symbol, session_date,
                    )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _availability_ends(self) -> tuple[datetime, datetime, datetime]:
        """Return (m1_end, h1_end, d1_end) from a single metadata call."""
        source = self._historical.primary_source
        m1_end = source.availability_end("ohlcv-1m")
        h1_end = source.availability_end("ohlcv-1h")
        d1_end = source.availability_end("ohlcv-1d")
        return m1_end, h1_end, d1_end

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
                "Fetching gap %s %s | %s → %s",
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
            "Catch-up %s %s complete | fetched=%d | total=%d",
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
        if first.timestamp > start:
            missing.append((start, first.timestamp - step))

        for left, right in zip(relevant, relevant[1:]):
            expected_next = left.timestamp + step
            if right.timestamp > expected_next:
                if timeframe in {BarTimeframe.H1, BarTimeframe.D1}:
                    if calendar.session_key(left.timestamp) != calendar.session_key(right.timestamp):
                        continue
                missing.append((expected_next, right.timestamp - step))

        last = relevant[-1]
        if last.timestamp < end:
            missing.append((last.timestamp + step, end))

        return [
            (gs, ge) for gs, ge in missing if gs <= ge
        ]
