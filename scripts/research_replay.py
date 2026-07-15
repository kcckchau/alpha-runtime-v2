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

Prerequisites:
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
from datetime import date, timedelta
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
from alpha.models.enums import AssetClass, BarTimeframe
from alpha.models.symbol import Symbol
from alpha.research.level_observer import LevelObserver, RunMode
from alpha.research.parquet_writer import LevelObservationWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("research_replay")

_FUTURES_TICKERS = frozenset({
    "MNQ", "NQ", "ES", "MES", "RTY", "M2K", "YM", "MYM",
})


def _infer_symbol(ticker: str) -> Symbol:
    root = ticker.rstrip("0123456789").upper()
    asset_class = AssetClass.FUTURE if root in _FUTURES_TICKERS else AssetClass.EQUITY
    exchange = "CME" if asset_class == AssetClass.FUTURE else "NASDAQ"
    return Symbol(ticker=ticker.upper(), exchange=exchange, asset_class=asset_class)


async def replay(symbol: str, target_date: date, warmup_days: int) -> None:
    settings = get_settings()

    sym_obj = _infer_symbol(symbol)
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
    aggregator.attach()
    pipeline.attach()
    observer.attach()

    storage = StorageEngine(settings, bus)

    # ── Warmup: prior N trading days ──────────────────────────────────────────
    warmup_dates: list[date] = []
    prev = target_date
    for _ in range(warmup_days):
        prev = calendar.prev_trading_day(prev)
        warmup_dates.insert(0, prev)

    for wd in warmup_dates:
        bars = await storage.load_bar_events(sym_obj.ticker, BarTimeframe.M1, wd, wd)
        if not bars:
            logger.warning("No bars in Parquet for warmup date %s — skipping", wd)
            continue
        bars.sort(key=lambda e: e.timestamp)
        logger.info("Warmup: %d bars for %s", len(bars), wd)
        for bar in bars:
            await bus.publish(bar)
            await asyncio.sleep(0)
        await bus.flush()

    # ── Target date replay ────────────────────────────────────────────────────
    bars = await storage.load_bar_events(sym_obj.ticker, BarTimeframe.M1, target_date, target_date)
    if not bars:
        logger.error(
            "No M1 bars in Parquet for %s %s.\n"
            "Fetch them first via POST /runtime/backfill-date or the dashboard.",
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
    parser.add_argument("--symbol", required=True, help="Ticker, e.g. MNQ")
    parser.add_argument("--date", required=True, help="Target date YYYY-MM-DD")
    parser.add_argument("--warmup-days", type=int, default=5, help="Prior trading days to seed ATR/EMA (default 5)")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    asyncio.run(replay(args.symbol, target_date, args.warmup_days))


if __name__ == "__main__":
    main()
