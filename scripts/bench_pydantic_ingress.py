"""
Benchmark: Pydantic construction overhead on the ingress hot path.

Measures per-record cost of QuoteEvent and TradeEvent construction
under four approaches:

  1. baseline   — current production path (full Pydantic + uuid4)
  2. no_uuid    — full Pydantic but EventMetadata.event_id suppressed
  3. construct  — model_construct() skips validation; uuid4 still called
  4. construct_cached_meta — model_construct() + cached EventMetadata (no uuid4 per record)

Run from repo root:
    python scripts/bench_pydantic_ingress.py
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from alpha.models.enums import DataSourceId, EventType, TakerSide
from alpha.models.events import EventMetadata, QuoteEvent, TradeEvent

_UTC = timezone.utc
_N = 100_000  # records per trial
_TRIALS = 5   # repeat and report best

# ── Shared inputs (simulate what _dispatch_quote / _dispatch_trade produce) ───

_SYMBOL = "MNQ-09"
_TIMESTAMP = datetime(2026, 8, 25, 14, 30, 0, tzinfo=_UTC)
_BID = Decimal("20050.25")
_ASK = Decimal("20050.50")
_BID_SZ = 12
_ASK_SZ = 8
_LAST = Decimal("20050.25")
_PRICE = Decimal("20050.25")
_SIZE = 3
_SOURCE = DataSourceId.DATABENTO


# ── Approach 1: baseline (current production) ─────────────────────────────────

def _quote_baseline() -> QuoteEvent:
    now = datetime.now(tz=_UTC)
    return QuoteEvent(
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        bid_price=_BID,
        bid_size=_BID_SZ,
        ask_price=_ASK,
        ask_size=_ASK_SZ,
        last_price=_LAST,
        metadata=EventMetadata(
            source=_SOURCE,
            received_at=now,
            is_replay=False,
        ),
    )


def _trade_baseline() -> TradeEvent:
    now = datetime.now(tz=_UTC)
    return TradeEvent(
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        price=_PRICE,
        size=_SIZE,
        taker_side=TakerSide.BUY,
        trade_id="12345678",
        metadata=EventMetadata(
            source=_SOURCE,
            received_at=now,
            is_replay=False,
        ),
    )


# ── Approach 2: full Pydantic, no uuid4 ───────────────────────────────────────
# EventMetadata without event_id (set to None — requires model_construct for meta)

def _quote_no_uuid() -> QuoteEvent:
    now = datetime.now(tz=_UTC)
    meta = EventMetadata.model_construct(
        event_id=None,
        source=_SOURCE,
        received_at=now,
        is_replay=False,
        sequence_num=None,
    )
    return QuoteEvent(
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        bid_price=_BID,
        bid_size=_BID_SZ,
        ask_price=_ASK,
        ask_size=_ASK_SZ,
        last_price=_LAST,
        metadata=meta,
    )


def _trade_no_uuid() -> TradeEvent:
    now = datetime.now(tz=_UTC)
    meta = EventMetadata.model_construct(
        event_id=None,
        source=_SOURCE,
        received_at=now,
        is_replay=False,
        sequence_num=None,
    )
    return TradeEvent(
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        price=_PRICE,
        size=_SIZE,
        taker_side=TakerSide.BUY,
        trade_id="12345678",
        metadata=meta,
    )


# ── Approach 3: model_construct (skip validation) ─────────────────────────────

def _quote_construct() -> QuoteEvent:
    now = datetime.now(tz=_UTC)
    meta = EventMetadata.model_construct(
        event_id=uuid4(),
        source=_SOURCE,
        received_at=now,
        is_replay=False,
        sequence_num=None,
    )
    return QuoteEvent.model_construct(
        event_type=EventType.QUOTE,
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        bid_price=_BID,
        bid_size=_BID_SZ,
        ask_price=_ASK,
        ask_size=_ASK_SZ,
        last_price=_LAST,
        last_size=None,
        bid_exchange=None,
        ask_exchange=None,
        metadata=meta,
    )


def _trade_construct() -> TradeEvent:
    now = datetime.now(tz=_UTC)
    meta = EventMetadata.model_construct(
        event_id=uuid4(),
        source=_SOURCE,
        received_at=now,
        is_replay=False,
        sequence_num=None,
    )
    return TradeEvent.model_construct(
        event_type=EventType.TRADE,
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        price=_PRICE,
        size=_SIZE,
        conditions=[],
        exchange=None,
        taker_side=TakerSide.BUY,
        trade_id="12345678",
        metadata=meta,
    )


# ── Approach 4: model_construct + cached metadata ─────────────────────────────
# Cache EventMetadata for 10ms; refresh on expiry.

_META_TTL_NS = 10_000_000  # 10ms
_cached_meta: EventMetadata | None = None
_cached_meta_ts_ns: int = 0


def _refresh_meta() -> EventMetadata:
    global _cached_meta, _cached_meta_ts_ns
    now_ns = time.time_ns()
    if _cached_meta is None or (now_ns - _cached_meta_ts_ns) > _META_TTL_NS:
        _cached_meta = EventMetadata.model_construct(
            event_id=None,
            source=_SOURCE,
            received_at=datetime.fromtimestamp(now_ns / 1e9, tz=_UTC),
            is_replay=False,
            sequence_num=None,
        )
        _cached_meta_ts_ns = now_ns
    return _cached_meta


def _quote_construct_cached() -> QuoteEvent:
    return QuoteEvent.model_construct(
        event_type=EventType.QUOTE,
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        bid_price=_BID,
        bid_size=_BID_SZ,
        ask_price=_ASK,
        ask_size=_ASK_SZ,
        last_price=_LAST,
        last_size=None,
        bid_exchange=None,
        ask_exchange=None,
        metadata=_refresh_meta(),
    )


def _trade_construct_cached() -> TradeEvent:
    return TradeEvent.model_construct(
        event_type=EventType.TRADE,
        symbol=_SYMBOL,
        timestamp=_TIMESTAMP,
        price=_PRICE,
        size=_SIZE,
        conditions=[],
        exchange=None,
        taker_side=TakerSide.BUY,
        trade_id="12345678",
        metadata=_refresh_meta(),
    )


# ── Runner ────────────────────────────────────────────────────────────────────

def _bench(label: str, fn, n: int, trials: int) -> float:
    best_ns = float("inf")
    for _ in range(trials):
        t0 = time.perf_counter_ns()
        for _ in range(n):
            fn()
        elapsed = time.perf_counter_ns() - t0
        if elapsed < best_ns:
            best_ns = elapsed
    per_record_us = best_ns / n / 1000
    throughput = n / (best_ns / 1e9)
    print(f"  {label:<35} {per_record_us:6.2f} µs/record   {throughput:>10,.0f} rec/s")
    return per_record_us


def main() -> None:
    print(f"\nPydantic ingress benchmark  n={_N:,}  trials={_TRIALS}  (best of {_TRIALS})\n")

    print("QuoteEvent:")
    b_q  = _bench("1. baseline (current)",         _quote_baseline,        _N, _TRIALS)
    n_q  = _bench("2. no uuid4",                   _quote_no_uuid,         _N, _TRIALS)
    c_q  = _bench("3. model_construct",             _quote_construct,       _N, _TRIALS)
    cc_q = _bench("4. model_construct + cached meta", _quote_construct_cached, _N, _TRIALS)
    print(f"  speedup  2 vs 1: {b_q/n_q:.1f}x   3 vs 1: {b_q/c_q:.1f}x   4 vs 1: {b_q/cc_q:.1f}x\n")

    print("TradeEvent:")
    b_t  = _bench("1. baseline (current)",         _trade_baseline,        _N, _TRIALS)
    n_t  = _bench("2. no uuid4",                   _trade_no_uuid,         _N, _TRIALS)
    c_t  = _bench("3. model_construct",             _trade_construct,       _N, _TRIALS)
    cc_t = _bench("4. model_construct + cached meta", _trade_construct_cached, _N, _TRIALS)
    print(f"  speedup  2 vs 1: {b_t/n_t:.1f}x   3 vs 1: {b_t/c_t:.1f}x   4 vs 1: {b_t/cc_t:.1f}x\n")

    print("Combined (quote-dominant at RTH open ~2000 quotes/s + 500 trades/s):")
    print(f"  baseline throughput ceiling:       {1e6/b_q:>8,.0f} quotes/s")
    print(f"  construct+cached throughput ceil:  {1e6/cc_q:>8,.0f} quotes/s")

    # Simulate burst: what fraction of CPU does each approach consume at peak?
    quote_rate = 2000
    trade_rate = 500
    baseline_load = (b_q * quote_rate + b_t * trade_rate) / 1e6
    cached_load   = (cc_q * quote_rate + cc_t * trade_rate) / 1e6
    print(f"\n  Estimated background-thread CPU load at {quote_rate} quotes/s + {trade_rate} trades/s:")
    print(f"    baseline:        {baseline_load:.2f} CPU-seconds per second (>{1.0:.0f}.0 = falling behind)")
    print(f"    construct+cached:{cached_load:.2f} CPU-seconds per second")


if __name__ == "__main__":
    main()
