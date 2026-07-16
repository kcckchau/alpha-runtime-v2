"""
Research replay — drives LevelObserver from stored Parquet bars.

Reads M1 bars from the production Parquet store for a given symbol + date,
runs them through FeatureEngine → BarFlowAggregator → BarPipeline →
PipelineOutputEvent → LevelObserver, then flushes to research Parquet.

Warmup: replays the prior N trading days first so ATR/EMA are seeded before
the target date begins.

Usage:
    python scripts/research_replay.py --symbol MNQ --date 2026-07-14
    python scripts/research_replay.py --symbol MNQ --date 2026-07-14 --warmup-days 5
    python scripts/research_replay.py --symbol MNQ --date 2026-07-14 --fetch

With --fetch: missing dates are pulled from Databento and saved to Parquet
before replay begins (requires DATABENTO_API_KEY in environment / .env).
Fetched bars are persisted so subsequent runs do not re-fetch.

Prerequisites (without --fetch):
    M1 bars for the target date and warmup days must exist in the production
    Parquet store (data/parquet/bars/1m/MNQ/year=.../month=.../day=.../data.parquet).
    Run the dashboard backfill (POST /runtime/backfill-date) first if they're missing.

Output:
    data/research/level_observations/MNQ/session_date=2026-07-14/part-*.parquet

Inspect:
    python scripts/inspect_level_observations.py --symbol MNQ
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Ensure src is on path when run directly
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.loader import get_settings
from alpha.core.clock import WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.feature.engine import FeatureEngine
from alpha.engines.flow.aggregator import BarFlowAggregator
from alpha.engines.flow.pipeline import BarPipeline
from alpha.engines.market_state.engine import MarketStateEngine
from alpha.engines.storage.engine import StorageEngine
from alpha.instruments import resolve_symbol
from alpha.models.enums import BarTimeframe
from alpha.research.interaction.config import LevelDistanceConfig
from alpha.research.interaction.engine import LevelInteractionEngine
from alpha.research.level_observer import LevelObserver, RunMode
from alpha.research.parquet_writer import LevelObservationWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("research_replay")

_UTC = timezone.utc


async def _fetch_and_save(
    sym_obj: Symbol,
    d: date,
    storage: StorageEngine,
    registry: SymbolRegistry,
    settings,
) -> list:
    """Fetch M1 bars for one calendar date from Databento and save to Parquet.

    CME futures sessions run 22:00 UTC previous day → 21:00 UTC.
    Saved via StorageEngine so they land in the standard Parquet layout and
    are available to all other tooling (dashboard, backtest, etc.).
    """
    from alpha.engines.historical.sources.databento import DatabentoHistoricalDataSource

    start_utc = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=_UTC) - timedelta(hours=2)
    end_utc = datetime(d.year, d.month, d.day, 21, 0, 0, tzinfo=_UTC)

    source = DatabentoHistoricalDataSource(registry, settings.databento)
    bars = []
    async for bar in source.fetch_bars(sym_obj.ticker, BarTimeframe.M1, start_utc, end_utc):
        bars.append(bar)
        await storage.save_bar(bar)

    bars.sort(key=lambda b: b.timestamp)
    logger.info("Fetched %d M1 bars for %s from Databento → saved to Parquet", len(bars), d)
    return bars


async def _load_or_fetch(
    sym_obj: Symbol,
    d: date,
    storage: StorageEngine,
    registry: SymbolRegistry,
    settings,
    fetch: bool,
) -> list:
    bars = await storage.load_bar_events(sym_obj.ticker, BarTimeframe.M1, d, d)
    if bars:
        return bars
    if not fetch:
        return []
    logger.info("No bars in Parquet for %s — fetching from Databento…", d)
    return await _fetch_and_save(sym_obj, d, storage, registry, settings)


async def replay(symbol: str, target_date: date, warmup_days: int, fetch: bool, clear: bool = True) -> None:
    settings = get_settings()

    sym_obj = resolve_symbol(symbol)
    registry = SymbolRegistry()
    registry.register(sym_obj)

    calendar = calendar_for_symbol(sym_obj)
    clock = WallClock()

    bus = EventBus()
    await bus.start()

    # ── Wire minimal pipeline ──────────────────────────────────────────────────
    feature = FeatureEngine(settings, bus, registry, calendar, clock)
    market_state = MarketStateEngine(settings, bus, registry)
    market_state.set_feature_engine(feature)

    aggregator = BarFlowAggregator(
        symbol=sym_obj.ticker,
        event_bus=bus,
        large_trade_threshold=10,
    )

    pipeline = BarPipeline(bus)
    pipeline.set_feature_engine(feature)
    pipeline.set_market_state_engine(market_state)
    # ThesisEngine and SetupEngine deliberately not wired — research replay needs
    # only FeatureEngine output for LevelBarObservation geometry.

    # ── Wire LevelObserver ────────────────────────────────────────────────────
    research_root = settings.storage.parquet_root.parent / "research"

    # Clear prior runs for this date so re-runs are idempotent (use --no-clear to append).
    if clear:
        date_str = target_date.isoformat()
        ticker = sym_obj.ticker
        _clear_dirs = [
            research_root / "level_observations" / ticker / f"session_date={date_str}",
            research_root / "interaction" / "episodes" / ticker / f"session_date={date_str}",
            research_root / "interaction" / "episode_bars" / ticker / f"session_date={date_str}",
        ]
        import shutil
        for d in _clear_dirs:
            if d.exists():
                shutil.rmtree(d)
                logger.info("Cleared %s", d)

    writer = LevelObservationWriter(research_root=research_root)
    observer = LevelObserver(
        event_bus=bus,
        registry=registry,
        writer=writer,
        run_mode=RunMode.REPLAY,
    )

    # ── Start engines ─────────────────────────────────────────────────────────
    await feature.initialize()
    await market_state.initialize()
    await feature.start()
    await market_state.start()
    interaction_engine = LevelInteractionEngine(
        event_bus=bus,
        registry=registry,
        research_root=research_root,
    )

    # Only pipeline engines during warmup — research writers attach after.
    aggregator.attach()
    pipeline.attach()

    storage = StorageEngine(settings, bus)

    # ── Warmup: prior N trading days ──────────────────────────────────────────
    warmup_dates: list[date] = []
    prev = target_date
    for _ in range(warmup_days):
        prev = calendar.prev_trading_day(prev)
        warmup_dates.insert(0, prev)

    for wd in warmup_dates:
        bars = await _load_or_fetch(sym_obj, wd, storage, registry, settings, fetch)
        if not bars:
            logger.warning("No bars for warmup date %s — skipping", wd)
            continue
        bars.sort(key=lambda e: e.timestamp)
        logger.info("Warmup: %d bars for %s", len(bars), wd)
        for bar in bars:
            await bus.publish(bar)
            await asyncio.sleep(0)
        await bus.flush()

    # ── Attach research writers now — target date only ───────────────────────
    observer.attach()
    interaction_engine.attach()

    # ── Target date replay ────────────────────────────────────────────────────
    bars = await _load_or_fetch(sym_obj, target_date, storage, registry, settings, fetch)
    if not bars:
        logger.error(
            "No M1 bars for %s %s.\n"
            "Use --fetch to pull from Databento, or run POST /runtime/backfill-date first.",
            symbol, target_date,
        )
        await bus.stop()
        return

    bars.sort(key=lambda e: e.timestamp)
    logger.info("Replaying %d bars for %s %s", len(bars), symbol, target_date)

    for bar in bars:
        await bus.publish(bar)
        await asyncio.sleep(0)

    await bus.flush()

    # ── Shutdown ──────────────────────────────────────────────────────────────
    await market_state.stop()
    await feature.stop()
    await bus.stop()

    observer.flush()
    interaction_engine.flush()
    logger.info(
        "Interaction episodes written: %d → %s",
        interaction_engine.episodes_written,
        research_root / "interaction",
    )

    obs = writer.rows_written
    logger.info(
        "Research replay complete | symbol=%s date=%s observations=%d → %s",
        symbol, target_date, obs, research_root / "level_observations" / sym_obj.ticker,
    )
    if obs == 0:
        logger.warning(
            "Zero observations written. Check that BarFlowAggregator sealed windows "
            "(it seals on bar N+1 arrival — the last bar of the session only seals "
            "if there is a subsequent bar in the replay sequence, e.g. overnight data)."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Research replay: record LevelBarObservation from stored bars")
    parser.add_argument("--symbol", default="MNQ-09", help="Ticker (default: MNQ-09). Must match the ticker used when bars were saved — check data/parquet/bars/1m/ for available tickers.")
    parser.add_argument("--date", required=True, help="Target date YYYY-MM-DD")
    parser.add_argument("--warmup-days", type=int, default=5, help="Prior trading days to seed ATR/EMA (default 5)")
    parser.add_argument(
        "--fetch", action="store_true",
        help="Fetch missing dates from Databento and save to Parquet before replaying",
    )
    parser.add_argument(
        "--no-clear", action="store_true",
        help="Append to existing partition instead of clearing prior run data (default: clear)",
    )
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    asyncio.run(replay(args.symbol, target_date, args.warmup_days, args.fetch, clear=not args.no_clear))


if __name__ == "__main__":
    main()
