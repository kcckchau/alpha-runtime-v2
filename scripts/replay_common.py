"""
replay_common.py — Shared infrastructure for backtest.py and replay_day.py.

Not a runnable script — imported by both to avoid duplicated Parquet
bar-loading logic (previously copy-pasted as backtest.py:_load_parquet_bars
and replay_day.py:_load_from_parquet).
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.clock import WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.context.engine import ContextEngine
from alpha.engines.feature.engine import FeatureEngine
from alpha.engines.flow.aggregator import BarFlowAggregator
from alpha.engines.flow.pipeline import BarPipeline
from alpha.engines.market_state.engine import MarketStateEngine
from alpha.engines.scoring.engine import ScoringEngine
from alpha.engines.setup.engine import SetupEngine
from alpha.engines.storage.engine import StorageEngine
from alpha.engines.storage.parquet import ParquetStore
from alpha.engines.thesis.engine import ThesisEngine
from alpha.models.enums import BarTimeframe
from alpha.models.events import BarEvent
from alpha.models.symbol import Symbol

_REPO = Path(__file__).resolve().parent.parent


@dataclass
class EngineBundle:
    """Everything backtest.py/replay_day.py need to feed bars and read state back."""
    bus: EventBus
    registry: SymbolRegistry
    calendar: SessionCalendar
    feature: FeatureEngine
    context: ContextEngine
    market_state: MarketStateEngine
    setup: SetupEngine
    thesis: ThesisEngine
    scoring: ScoringEngine | None


async def build_replay_pipeline(
    settings: AlphaSettings,
    symbol: str,
    sym_obj: Symbol,
    *,
    include_scoring: bool = True,
) -> EngineBundle:
    """
    Construct and wire FeatureEngine/ContextEngine/MarketStateEngine/SetupEngine/
    ThesisEngine[/ScoringEngine] onto BarFlowAggregator + BarPipeline (same
    sequential-stage pattern as live), started and ready for bar events.

    include_scoring=False (replay_day.py) intentionally omits ScoringEngine —
    that script shows SetupEngine's raw score, not a final letter grade.

    Caller owns feeding bars via bus.publish()/bus.flush() and must call
    stop_replay_pipeline() when done.
    """
    registry = SymbolRegistry()
    registry.register(sym_obj)
    calendar = calendar_for_symbol(sym_obj)
    bus = EventBus(queue_size=5000)
    await bus.start()
    clock = WallClock()

    feature_engine      = FeatureEngine(settings, bus, registry, calendar, clock)
    context_engine      = ContextEngine(settings, bus, registry, calendar, clock)
    market_state_engine = MarketStateEngine(settings, bus, registry)
    setup_engine        = SetupEngine(settings, bus, registry)
    thesis_engine       = ThesisEngine(settings, bus, registry)
    scoring_engine      = ScoringEngine(settings, bus) if include_scoring else None

    market_state_engine.set_feature_engine(feature_engine)
    context_engine.set_feature_engine(feature_engine)
    market_state_engine.set_context_engine(context_engine)
    setup_engine.set_feature_engine(feature_engine)
    setup_engine.set_market_state_engine(market_state_engine)
    setup_engine.set_context_engine(context_engine)
    thesis_engine.set_feature_engine(feature_engine)
    thesis_engine.set_context_engine(context_engine)
    if scoring_engine is not None:
        scoring_engine.set_setup_engine(setup_engine)
        scoring_engine.set_feature_engine(feature_engine)

    # Wire BarFlowAggregator + BarPipeline (same as live)
    agg = BarFlowAggregator(
        symbol=symbol,
        event_bus=bus,
        large_trade_threshold=getattr(settings.runtime, "large_trade_threshold", 10),
    )
    agg.attach()

    pipeline = BarPipeline(bus)
    pipeline.set_feature_engine(feature_engine)
    pipeline.set_market_state_engine(market_state_engine)
    pipeline.set_thesis_engine(thesis_engine)
    pipeline.set_setup_engine(setup_engine)
    if scoring_engine is not None:
        pipeline.set_scoring_engine(scoring_engine)
    pipeline.attach()

    await feature_engine.initialize()
    await context_engine.initialize()
    await market_state_engine.initialize()
    await setup_engine.initialize()
    await thesis_engine.initialize()
    if scoring_engine is not None:
        await scoring_engine.initialize()

    # ContextEngine must start AFTER FeatureEngine so its BAR subscription
    # fires second — guaranteeing FeatureEngine.get_snapshot() is current.
    # (ContextEngine is not migrated onto BarPipeline; BarPipeline has no
    # set_context_engine — it still relies on this start-order.)
    await feature_engine.start()
    await context_engine.start()
    await market_state_engine.start()
    await setup_engine.start()
    await thesis_engine.start()
    if scoring_engine is not None:
        await scoring_engine.start()

    return EngineBundle(
        bus=bus, registry=registry, calendar=calendar,
        feature=feature_engine, context=context_engine,
        market_state=market_state_engine, setup=setup_engine,
        thesis=thesis_engine, scoring=scoring_engine,
    )


async def stop_replay_pipeline(bundle: EngineBundle) -> None:
    if bundle.scoring is not None:
        await bundle.scoring.stop()
    await bundle.thesis.stop()
    await bundle.setup.stop()
    await bundle.market_state.stop()
    await bundle.context.stop()
    await bundle.feature.stop()
    await bundle.bus.stop()

# Paths where a code change would silently change replay/backtest output
# without touching AlphaSettings — most SetupEngine/ScoringEngine/ThesisEngine/
# MarketStateEngine thresholds are hardcoded Python literals with no version
# string of their own (unlike FeatureEngine's norm3/ribbon policies below), so
# the git commit + dirty flag is the only reliable "did the logic change"
# signal for those engines.
_TRADING_LOGIC_PATHS = ("src/alpha/engines/", "src/alpha/features/", "scripts/")


def config_fingerprint_lines() -> list[str]:
    """
    Human-readable lines identifying exactly what setup/context code produced
    a replay/backtest run, so a later run with different code (even
    uncommitted) doesn't get silently compared as if it used the same logic.
    """
    from alpha.features.slope import (
        EMA_1H_RIBBON_POLICY_VERSION,
        EMA_1H_SLOPE_POLICY_VERSION,
        SLOPE_POLICY_VERSION,
    )

    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_REPO, capture_output=True, text=True, timeout=5,
        ).stdout.strip() or "unknown"
    except Exception:
        commit = "unknown"

    dirty_files: list[str] = []
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=_REPO, capture_output=True, text=True, timeout=5,
        ).stdout
        for line in status.splitlines():
            path = line[3:].strip()
            if path.startswith(_TRADING_LOGIC_PATHS):
                dirty_files.append(path)
    except Exception:
        pass

    dirty_label = (
        f"DIRTY — {len(dirty_files)} uncommitted trading-logic file(s)"
        if dirty_files else "clean"
    )
    lines = [f"Config fingerprint: commit={commit} ({dirty_label})"]
    for f in dirty_files:
        lines.append(f"  uncommitted: {f}")
    lines.append(
        f"  policy versions: slope={SLOPE_POLICY_VERSION} "
        f"1h_ribbon={EMA_1H_RIBBON_POLICY_VERSION} 1h_slope={EMA_1H_SLOPE_POLICY_VERSION}"
    )
    return lines


def default_m1_warmup_days(settings: AlphaSettings) -> int:
    """
    Same formula backfill.py uses for its own default (warmup-driven) M1 fetch
    window, so backtest.py/replay_day.py's --warmup default tracks
    settings.historical.minute1_warmup_bars instead of an unrelated flat
    constant that silently drifts if that config changes.
    """
    return max(3, settings.historical.minute1_warmup_bars // 390 + 1)


def load_m1_bars(
    symbol: str,
    start: date,
    end: date,
    settings: AlphaSettings,
    *,
    skip_read_errors: bool = True,
) -> list[BarEvent]:
    """Load M1 bars from Parquet for [start, end] inclusive, sorted by timestamp.

    skip_read_errors=True (backtest.py's original behavior) silently skips a day
    on any read failure — appropriate when scanning many days in one run, since a
    single missing/corrupt day shouldn't abort the whole range.

    skip_read_errors=False (replay_day.py's original behavior) lets a read failure
    raise — appropriate for a single-day interactive debug run, where a silent
    empty result would be more confusing than a loud error.
    """
    parquet = ParquetStore(settings.storage)
    bars: list[BarEvent] = []
    d = start
    while d <= end:
        if skip_read_errors:
            try:
                table = parquet.read_range(f"bars/{BarTimeframe.M1}", symbol, d, d)
            except Exception:
                d += timedelta(days=1)
                continue
        else:
            table = parquet.read_range(f"bars/{BarTimeframe.M1}", symbol, d, d)
        for row in table.to_pylist():
            bar = StorageEngine._row_to_bar_event(row, BarTimeframe.M1)
            if bar.symbol != symbol:
                bar = bar.model_copy(update={"symbol": symbol})
            bars.append(bar)
        d += timedelta(days=1)
    bars.sort(key=lambda b: b.timestamp)
    return bars
