"""
Benchmark: FeatureEngine.process_bar() and BarSnapshot construction.

Measures:
  1. Full process_bar() — warm state, single symbol, RTH bars
  2. _build_snapshot() in isolation — pure computation + Pydantic construction
  3. BarSnapshot Pydantic construction alone (baseline vs model_construct)

Run from repo root:
    python scripts/bench_feature_pipeline.py
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from alpha.config.settings import AlphaSettings
from alpha.core.clock import WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.calendar.resolver import calendar_for_symbol
from alpha.engines.feature.engine import FeatureEngine, SymbolFeatureState
from alpha.models.bar import Bar
from alpha.models.enums import AssetClass, BarTimeframe, DataSourceId, EventType, SessionPhase
from alpha.models.events import BarBundleEvent, BarEvent, EventMetadata
from alpha.models.snapshot import BarSnapshot
from alpha.models.symbol import Symbol

_UTC = timezone.utc
_N = 2_000     # iterations per trial (process_bar runs once/min; 2000 = ~33h of bars)
_TRIALS = 5

# ── Synthetic setup ────────────────────────────────────────────────────────────

TICKER = "MNQ-09"

def _make_registry() -> SymbolRegistry:
    registry = SymbolRegistry()
    registry.register(Symbol(
        ticker=TICKER,
        exchange="CME",
        asset_class=AssetClass.FUTURE,
        root_symbol="MNQ",
        tick_size=Decimal("0.25"),
        point_value=Decimal("2"),
        lot_size=1,
    ))
    return registry


def _make_bar(ts: datetime, close: Decimal = Decimal("20050.25")) -> BarEvent:
    meta = EventMetadata(source=DataSourceId.DATABENTO, received_at=ts, is_replay=True)
    return BarEvent(
        symbol=TICKER,
        timestamp=ts,
        timeframe=BarTimeframe.M1,
        open=close - Decimal("2"),
        high=close + Decimal("5"),
        low=close - Decimal("5"),
        close=close,
        volume=1200,
        metadata=meta,
    )


def _make_bundle(bar: BarEvent) -> BarBundleEvent:
    meta = EventMetadata(source=DataSourceId.DATABENTO, received_at=bar.timestamp, is_replay=True)
    return BarBundleEvent(
        symbol=bar.symbol,
        timestamp=bar.timestamp,
        timeframe=bar.timeframe,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        metadata=meta,
        flow=None,
    )


async def _build_engine() -> tuple[FeatureEngine, list[BarBundleEvent]]:
    settings = AlphaSettings()
    registry = _make_registry()
    bus = EventBus()
    clock = WallClock()
    sym = registry.get(TICKER)
    calendar = calendar_for_symbol(sym)

    engine = FeatureEngine(settings, bus, registry, calendar, clock)
    engine._pipeline_mode = True
    await engine._on_initialize()

    # Warm up: feed 200 RTH bars so all EMAs, ATR, slopes are populated
    rth_open = datetime(2026, 8, 25, 14, 30, 0, tzinfo=_UTC)  # 09:30 ET = 14:30 UTC
    warmup_bars = []
    close = Decimal("20050.25")
    for i in range(200):
        ts = rth_open + timedelta(minutes=i)
        close += Decimal(str((i % 7) - 3)) * Decimal("0.25")
        bar = _make_bar(ts, close)
        bundle = _make_bundle(bar)
        engine.process_bar(bundle)
        warmup_bars.append(bundle)

    # Benchmark bars: another N bars right after warmup
    bench_ts_start = rth_open + timedelta(minutes=200)
    bench_bundles = []
    for i in range(_N):
        ts = bench_ts_start + timedelta(minutes=i)
        close += Decimal(str((i % 5) - 2)) * Decimal("0.25")
        bench_bundles.append(_make_bundle(_make_bar(ts, close)))

    return engine, bench_bundles


def _bench(label: str, fn, items, trials: int) -> float:
    best_ns = float("inf")
    for _ in range(trials):
        t0 = time.perf_counter_ns()
        for item in items:
            fn(item)
        elapsed = time.perf_counter_ns() - t0
        if elapsed < best_ns:
            best_ns = elapsed
    n = len(items)
    per_us = best_ns / n / 1000
    rate = n / (best_ns / 1e9)
    print(f"  {label:<45} {per_us:7.1f} µs/bar   {rate:>10,.0f} bars/s")
    return per_us


def _bench_snapshot_construction(state: SymbolFeatureState, bar: BarEvent) -> None:
    """Isolate just the Pydantic BarSnapshot(...) call inside _build_snapshot."""
    from alpha.models.enums import SessionPhase
    # Minimal snapshot — same field count, representative values
    BarSnapshot(
        symbol=bar.symbol,
        timestamp=bar.timestamp,
        timeframe=bar.timeframe,
        bar=Bar(
            symbol=bar.symbol, timeframe=bar.timeframe, timestamp=bar.timestamp,
            open=bar.open, high=bar.high, low=bar.low, close=bar.close,
            volume=bar.volume, source=DataSourceId.DATABENTO,
        ),
        vwap=state.vwap,
        vwap_deviation_pct=0.0,
        cumulative_volume=state.cumulative_volume,
        session_phase=SessionPhase.MID,
        bars_since_open=state.bars_since_open,
        ema_9=state.ema_9,
        ema_21=state.ema_21,
        ema_50=state.ema_50,
        atr_14=None,
        atr_30=state.atr_30,
        bid_price=state.latest_bid,
        ask_price=state.latest_ask,
    )


def _bench_snapshot_construct(state: SymbolFeatureState, bar: BarEvent) -> None:
    """Same snapshot via model_construct (skip validation)."""
    b = Bar.model_construct(
        symbol=bar.symbol, timeframe=bar.timeframe, timestamp=bar.timestamp,
        open=bar.open, high=bar.high, low=bar.low, close=bar.close,
        volume=bar.volume, vwap=None, trade_count=None, source=DataSourceId.DATABENTO,
    )
    BarSnapshot.model_construct(
        symbol=bar.symbol,
        timestamp=bar.timestamp,
        timeframe=bar.timeframe,
        bar=b,
        vwap=state.vwap,
        vwap_deviation_pct=0.0,
        cumulative_volume=state.cumulative_volume,
        session_phase=SessionPhase.MID,
        bars_since_open=state.bars_since_open,
        ema_9=state.ema_9,
        ema_21=state.ema_21,
        ema_50=state.ema_50,
        atr_14=None,
        atr_30=state.atr_30,
        bid_price=state.latest_bid,
        ask_price=state.latest_ask,
    )


async def main() -> None:
    print(f"\nFeatureEngine pipeline benchmark  n={_N:,}  trials={_TRIALS}  (best of {_TRIALS})\n")

    engine, bench_bundles = await _build_engine()
    print(f"State warm: ema_9={engine._states[TICKER].ema_9}  atr_30={engine._states[TICKER].atr_30}\n")

    print("Full process_bar() end-to-end:")
    process_us = _bench(
        "process_bar (full pipeline stage 1)",
        engine.process_bar,
        bench_bundles,
        _TRIALS,
    )

    # Isolate _build_snapshot by running process_bar once more to get a warm state,
    # then time _build_snapshot directly.
    state = engine._states[TICKER]
    last_bundle = bench_bundles[-1]
    last_bar = last_bundle.to_bar_event()

    print("\n_build_snapshot() in isolation:")
    build_us = _bench(
        "_build_snapshot (compute + Pydantic ~100 fields)",
        lambda _: engine._build_snapshot(state, last_bar),
        range(_N),
        _TRIALS,
    )

    print("\nBarSnapshot Pydantic construction (partial — same field count pattern):")
    snap_full_us = _bench(
        "BarSnapshot(...) full validation",
        lambda _: _bench_snapshot_construction(state, last_bar),
        range(_N),
        _TRIALS,
    )
    snap_construct_us = _bench(
        "BarSnapshot.model_construct() skip validation",
        lambda _: _bench_snapshot_construct(state, last_bar),
        range(_N),
        _TRIALS,
    )
    print(f"  speedup model_construct vs full: {snap_full_us/snap_construct_us:.1f}x\n")

    print("Summary:")
    print(f"  process_bar total:     {process_us:.1f} µs  ({process_us/1000:.1f} ms)")
    print(f"  _build_snapshot:       {build_us:.1f} µs  ({build_us/1000:.1f} ms)")
    compute_us = process_us - build_us
    print(f"  compute only (approx): {compute_us:.1f} µs  ({compute_us/1000:.1f} ms)")
    print()
    print(f"  At 1 bar/min: pipeline runs {60*1000/process_us:.0f}× faster than needed")
    print(f"  Event loop blocked for ~{process_us/1000:.2f} ms per bar — negligible")


if __name__ == "__main__":
    asyncio.run(main())
