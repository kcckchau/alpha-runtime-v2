"""
Benchmark: BarFlowAggregator seal path + full pipeline simulation.

Two parts:

Part 1 — BarFlowAggregator seal path in isolation
  Simulates _build_flow_context() with realistic quote/trade volumes:
    - RTH open bar:  ~2000 quotes/s × 60s = 120k quotes
    - RTH mid bar:   ~200  quotes/s × 60s = 12k  quotes
    - Overnight bar: ~30   quotes/s × 60s = 1.8k quotes

Part 2 — Full pipeline simulation (asyncio)
  Feeds a realistic stream of quote + trade events through the live
  EventBus → BarFlowAggregator → BarPipeline → FeatureEngine chain,
  then times the seal coroutine end-to-end.
  This is the closest we can get to what actually happens at RTH open
  without a live Databento connection.

Run from repo root:
    python scripts/bench_flow_pipeline.py
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import statistics

from alpha.config.settings import AlphaSettings
from alpha.core.clock import WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.calendar.resolver import calendar_for_symbol
from alpha.engines.feature.engine import FeatureEngine
from alpha.engines.flow.aggregator import BarFlowAggregator, _Window
from alpha.engines.flow.pipeline import BarPipeline
from alpha.models.enums import AssetClass, BarTimeframe, DataSourceId, EventType, TakerSide
from alpha.models.events import BarBundleEvent, BarEvent, EventMetadata, QuoteEvent, TradeEvent
from alpha.models.symbol import Symbol

_UTC = timezone.utc
TICKER = "MNQ-09"
_TRIALS = 3


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _meta(ts: datetime) -> EventMetadata:
    return EventMetadata(source=DataSourceId.DATABENTO, received_at=ts, is_replay=True)


def _make_quote(ts: datetime) -> QuoteEvent:
    return QuoteEvent(
        symbol=TICKER, timestamp=ts,
        bid_price=Decimal("20050.00"), bid_size=12,
        ask_price=Decimal("20050.25"), ask_size=8,
        metadata=_meta(ts),
    )


def _make_trade(ts: datetime, side: TakerSide = TakerSide.BUY) -> TradeEvent:
    return TradeEvent(
        symbol=TICKER, timestamp=ts,
        price=Decimal("20050.25"), size=3,
        taker_side=side,
        metadata=_meta(ts),
    )


def _make_bar(ts: datetime, tf: BarTimeframe = BarTimeframe.M1) -> BarEvent:
    return BarEvent(
        symbol=TICKER, timestamp=ts, timeframe=tf,
        open=Decimal("20048.00"), high=Decimal("20055.00"),
        low=Decimal("20043.00"), close=Decimal("20050.25"),
        volume=1200, metadata=_meta(ts),
    )


def _fill_window(w: _Window, n_quotes: int, n_trades: int, bar_open_ts: datetime) -> None:
    """Fill a _Window with n_quotes quotes and n_trades trades spread over 60s."""
    for i in range(n_quotes):
        ts = bar_open_ts + timedelta(seconds=i * 60 / max(n_quotes, 1))
        w.add_quote(QuoteEvent(
            symbol=TICKER, timestamp=ts,
            bid_price=Decimal("20050.00"), bid_size=12,
            ask_price=Decimal("20050.25"), ask_size=8,
            metadata=_meta(ts),
        ))
    for i in range(n_trades):
        ts = bar_open_ts + timedelta(seconds=i * 60 / max(n_trades, 1))
        w.add_trade(TradeEvent(
            symbol=TICKER, timestamp=ts,
            price=Decimal("20050.25"), size=3,
            taker_side=TakerSide.BUY if i % 2 == 0 else TakerSide.SELL,
            metadata=_meta(ts),
        ))
    # Add 60 fake 1s bars for split delta / absorption
    for i in range(60):
        ts = bar_open_ts + timedelta(seconds=i)
        w.add_s1_bar(BarEvent(
            symbol=TICKER, timestamp=ts, timeframe=BarTimeframe.S1,
            open=Decimal("20048.00"), high=Decimal("20049.00"),
            low=Decimal("20043.00") if i == 15 else Decimal("20047.00"),
            close=Decimal("20050.25"),
            volume=20, metadata=_meta(ts),
        ))


# ── Part 1: seal path in isolation ────────────────────────────────────────────

def _bench_seal(label: str, n_quotes: int, n_trades: int) -> float:
    """Time _build_flow_context() with a pre-filled window."""
    bar_open = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
    bar_close = bar_open + timedelta(minutes=1)
    bar = _make_bar(bar_close)
    agg = BarFlowAggregator(symbol=TICKER, event_bus=None)  # type: ignore[arg-type]
    agg._avg_bar_volume = 1000.0

    best_ns = float("inf")
    for _ in range(_TRIALS):
        w = _Window(bar_open, bar_close, 10)
        _fill_window(w, n_quotes, n_trades, bar_open)

        t0 = time.perf_counter_ns()
        agg._build_flow_context(w, bar)
        elapsed = time.perf_counter_ns() - t0
        if elapsed < best_ns:
            best_ns = elapsed

    ms = best_ns / 1_000_000
    print(f"  {label:<35} quotes={n_quotes:>6,}  trades={n_trades:>4,}  → {ms:.2f} ms")
    return ms


def _bench_quote_imbalance_only(label: str, n_quotes: int) -> float:
    """Time _compute_quote_imbalance() alone."""
    bar_open = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
    bar_close = bar_open + timedelta(minutes=1)
    agg = BarFlowAggregator(symbol=TICKER, event_bus=None)  # type: ignore[arg-type]

    w = _Window(bar_open, bar_close, 10)
    _fill_window(w, n_quotes, 0, bar_open)

    best_ns = float("inf")
    for _ in range(_TRIALS):
        t0 = time.perf_counter_ns()
        agg._compute_quote_imbalance(w)
        elapsed = time.perf_counter_ns() - t0
        if elapsed < best_ns:
            best_ns = elapsed

    ms = best_ns / 1_000_000
    print(f"  {label:<35} quotes={n_quotes:>6,}  → {ms:.3f} ms")
    return ms


# ── Part 2: full pipeline simulation ──────────────────────────────────────────

def _make_registry() -> SymbolRegistry:
    r = SymbolRegistry()
    r.register(Symbol(
        ticker=TICKER, exchange="CME", asset_class=AssetClass.FUTURE,
        root_symbol="MNQ", tick_size=Decimal("0.25"), point_value=Decimal("2"), lot_size=1,
    ))
    return r


async def _simulate_rth_open_bar(
    quote_rate: int,   # quotes per second
    trade_rate: int,   # trades per second
    label: str,
) -> dict:
    """
    Simulate one full 1m bar at the given quote/trade rate.
    Returns timing breakdown of:
      - total coroutine dispatch time (all _on_quote / _on_trade calls)
      - seal time (_seal_and_emit + full pipeline)
    """
    settings = AlphaSettings()
    registry = _make_registry()
    bus = EventBus()
    clock = WallClock()
    sym = registry.get(TICKER)
    calendar = calendar_for_symbol(sym)

    feature = FeatureEngine(settings, bus, registry, calendar, clock)
    feature._pipeline_mode = True
    await feature._on_initialize()

    pipeline = BarPipeline(bus)
    pipeline.set_feature_engine(feature)
    pipeline.attach()

    agg = BarFlowAggregator(symbol=TICKER, event_bus=bus)
    agg.attach()

    # Warm up FeatureEngine with 200 bars
    rth_open = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
    close = Decimal("20050.25")
    for i in range(200):
        ts = rth_open + timedelta(minutes=-(200 - i))
        bar = _make_bar(ts)
        # Feed directly into FeatureEngine (bypass aggregator for warmup)
        bundle = BarBundleEvent(
            symbol=bar.symbol, timestamp=bar.timestamp, timeframe=bar.timeframe,
            open=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume,
            metadata=bar.metadata, flow=None,
        )
        feature.process_bar(bundle)

    # ── Simulate the bar ───────────────────────────────────────────────────────
    bar_open_ts = rth_open
    bar_close_ts = bar_open_ts + timedelta(minutes=1)

    # First open a window in the aggregator
    agg._open_window(bar_open_ts, 60)

    n_quotes = quote_rate * 60
    n_trades = trade_rate * 60

    # Time: dispatch all quotes and trades (event loop cost per tick)
    dispatch_times: list[float] = []

    t_dispatch_start = time.perf_counter_ns()

    for i in range(n_quotes):
        ts = bar_open_ts + timedelta(microseconds=i * 60_000_000 // max(n_quotes, 1))
        evt = _make_quote(ts)
        t0 = time.perf_counter_ns()
        await agg._on_quote(evt)
        dispatch_times.append(time.perf_counter_ns() - t0)

    for i in range(n_trades):
        ts = bar_open_ts + timedelta(microseconds=i * 60_000_000 // max(n_trades, 1))
        side = TakerSide.BUY if i % 3 != 0 else TakerSide.SELL
        evt = _make_trade(ts, side)
        t0 = time.perf_counter_ns()
        await agg._on_trade(evt)
        dispatch_times.append(time.perf_counter_ns() - t0)

    # Also feed 60 S1 bars
    for i in range(60):
        ts = bar_open_ts + timedelta(seconds=i)
        s1 = _make_bar(ts, BarTimeframe.S1)
        await agg._on_bar(s1)

    total_dispatch_ns = time.perf_counter_ns() - t_dispatch_start

    # Time: seal (this triggers _build_flow_context + BarPipeline all 5 stages)
    seal_bar = _make_bar(bar_close_ts)
    t_seal = time.perf_counter_ns()
    await agg._on_bar(seal_bar)
    seal_ns = time.perf_counter_ns() - t_seal

    per_quote_us = statistics.mean(dispatch_times[:n_quotes]) / 1000 if n_quotes > 0 else 0
    per_trade_us = statistics.mean(dispatch_times[n_quotes:]) / 1000 if n_trades > 0 else 0

    print(f"\n  [{label}]  quotes/s={quote_rate}  trades/s={trade_rate}")
    print(f"    Per-tick dispatch (event loop coroutines):")
    print(f"      _on_quote avg:       {per_quote_us:.2f} µs   (total {n_quotes:,} quotes → {total_dispatch_ns/1e6:.1f} ms over 60s)")
    print(f"      _on_trade avg:       {per_trade_us:.2f} µs")
    print(f"    Bar seal (_seal_and_emit + pipeline stage 1):  {seal_ns/1e6:.2f} ms")

    return {
        "label": label,
        "n_quotes": n_quotes,
        "n_trades": n_trades,
        "per_quote_us": per_quote_us,
        "per_trade_us": per_trade_us,
        "total_dispatch_ms": total_dispatch_ns / 1e6,
        "seal_ms": seal_ns / 1e6,
    }


async def main() -> None:
    print("\n" + "=" * 70)
    print("Part 1 — _build_flow_context() seal cost in isolation")
    print("=" * 70)

    print("\n_compute_quote_imbalance() alone:")
    _bench_quote_imbalance_only("Overnight (30 q/s)",   30 * 60)
    _bench_quote_imbalance_only("RTH mid   (200 q/s)",  200 * 60)
    _bench_quote_imbalance_only("RTH open  (2000 q/s)", 2000 * 60)
    _bench_quote_imbalance_only("RTH burst (5000 q/s)", 5000 * 60)

    print("\n_build_flow_context() full (imbalance + split delta + absorption):")
    _bench_seal("Overnight (30 q/s, 10 t/s)",    30 * 60,   10 * 60)
    _bench_seal("RTH mid   (200 q/s, 100 t/s)", 200 * 60,  100 * 60)
    _bench_seal("RTH open  (2000 q/s, 500 t/s)", 2000 * 60, 500 * 60)

    print("\n" + "=" * 70)
    print("Part 2 — Full pipeline simulation (EventBus + FeatureEngine)")
    print("=" * 70)
    print("(Simulates real coroutine dispatch + seal end-to-end)")

    await _simulate_rth_open_bar(30,   10,  "Overnight")
    await _simulate_rth_open_bar(200,  100, "RTH mid  ")
    await _simulate_rth_open_bar(2000, 500, "RTH open ")

    print()


if __name__ == "__main__":
    asyncio.run(main())
