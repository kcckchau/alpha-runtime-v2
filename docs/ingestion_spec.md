# Data Ingestion Spec

Implemented subset of the live data ingestion layer for alpha-runtime-v2.

---

## Overview

The live feed path is: **Databento/IBKR adapter → LiveIngestionEngine → EventBus → downstream engines**.

This document covers the quality controls layered onto that path:

1. EventBus backpressure: drop oldest, keep newest
2. Quote debounce: only publish on price change, not size-only
3. Outlier trade filter: reject off-market prints before they spike partial bars
4. IngestionMonitor: per-symbol data quality state machine
5. Kill switch: block WOULD_ENTER when data quality is not CLEAN

---

## 1. EventBus Backpressure — drop oldest, keep newest

**File:** `src/alpha/core/event_bus.py`

Each subscription has a bounded `asyncio.Queue` (default 2,000 events). When a subscriber
falls behind and the queue fills:

- **`drop_if_full=False`** (default): publisher `await`s until the queue drains —
  backpressure, appropriate for storage/feature engines that must process every bar.

- **`drop_if_full=True`**: oldest queued event is evicted and the incoming event is
  enqueued — appropriate for "latest value wins" handlers like quote subscribers and
  the PositionMonitor's quote feed, where staleness is worse than a missed event.

The eviction works because the event loop is single-threaded: `get_nowait()` followed
immediately by `put()` is atomic from the perspective of other coroutines.

```python
# publish() in EventBus
if sub.queue.full() and sub.drop_if_full:
    sub.queue.get_nowait()   # evict oldest
    sub.queue.task_done()
# then enqueue the new event unconditionally
await sub.queue.put(event)
```

---

## 2. Quote Debounce

**File:** `src/alpha/engines/live/engine.py` — `_on_quote()`

Databento `mbp-1` fires on every top-of-book change, including size-only updates — several
hundred per second for MNQ. Downstream engines (BarFlowAggregator, PositionMonitor) only
care about price changes, not queue depth reshuffling.

Rule: publish a `QuoteEvent` to the EventBus only when `bid_price` or `ask_price` changes.
Size-only changes update `_latest_quotes` (for the REST dashboard) but are not forwarded.

---

## 3. Outlier Trade Filter

**File:** `src/alpha/engines/live/engine.py` — `_is_outlier_trade()` / `_update_partial_bar()`

Databento's `trades` schema includes off-market prints: block trades, spread-implied fills,
late-reported negotiated trades. A single such tick would spike the live partial bar's wick
until the next completed exchange bar corrects it.

Filter band (adaptive):
- **When a live quote is available:** anchor = mid price; band = max(mid × 0.1%, spread × 30)
- **Fallback (no quote yet):** anchor = last accepted trade price; band = anchor × 0.2%

Outlier ticks are silently dropped from the partial bar accumulator. They are still published
to the EventBus (raw `TradeEvent`) so the flow aggregator can see them if needed.

Note: `match_type` filtering (keep only standard CLOB fills, reject block/EFRP/implied) is
handled at the adapter layer before events reach the engine.

---

## 4. IngestionMonitor — Data Quality State Machine

**File:** `src/alpha/engines/live/monitor.py`

One `IngestionMonitor` instance per runtime, shared between `LiveIngestionEngine` (writes)
and `PositionMonitor` (kill switch reads).

### Per-symbol state machine

```
CLEAN ──────────────────────────────────────────────────────────► DEGRADED
                         (bar lag >250ms, gap, or silence >30s during RTH)

DEGRADED ──────────────────────────────────────────────────────► RECOVERING
                         (first clean M1 bar arrives)

RECOVERING ─────────────────────────────────────────────────────► CLEAN
                         (second consecutive clean M1 bar)

any ────────────────────────────────────────────────────────────► FAILED
                         (silence ≥5 min during RTH — sticky, no auto-recovery)
```

### Checks

| Check | Threshold | Trigger |
|---|---|---|
| **Bar lag** | >250ms after expected close | M1 bar arrives late |
| **Bar gap** | >1 min + 5s tolerance | Jump in bar timestamps |
| **Silence** | >30s without any record during RTH | No trade or quote received |
| **Failed** | >5min silence during RTH | Escalation from DEGRADED |

### RTH awareness

Silence checks only fire during **09:30–16:00 ET**. Overnight and pre-market silence is
expected for MNQ and does not trigger degradation.

### Thresholds (constants in monitor.py)

```python
_BAR_LAG_WARN_MS    = 250   # ms
_SILENCE_DEGRADED_S = 30    # seconds
_FAILED_S           = 300   # seconds (5 minutes)
_CLEAN_BARS_NEEDED  = 2     # consecutive clean bars to exit DEGRADED/RECOVERING
```

### API

```python
monitor = IngestionMonitor(symbols=["MNQ-09"])

# Written by LiveIngestionEngine
monitor.on_bar_received("MNQ-09", bar_ts, received_at)   # on each completed M1 bar
monitor.on_record_received("MNQ-09")                      # on every trade or quote
monitor.check_health()                                     # called every second

# Read by PositionMonitor
monitor.is_signal_allowed("MNQ-09")   # → False when DEGRADED / RECOVERING / FAILED
monitor.get_quality("MNQ-09")         # → DataQualityState enum value
monitor.get_degraded_reason("MNQ-09") # → human-readable string or None
monitor.summary()                     # → dict for dashboards / health endpoints
```

---

## 5. Kill Switch — Block WOULD_ENTER When DEGRADED

**File:** `src/alpha/engines/position/engine.py` — `_on_s1_bar()`

Before `_check_entry()` is called, the PositionMonitor checks whether data quality is CLEAN:

```python
if (
    self._ingestion_monitor is not None
    and not self._ingestion_monitor.is_signal_allowed(sym)
):
    return  # block signal — degraded feed
```

This means: **no WOULD_ENTER signals fire when the live feed has a lag, gap, or silence
condition**. Existing open positions continue to track (exit checks still run).

The kill switch is conservative: RECOVERING blocks just as DEGRADED does. Only CLEAN allows
entry signals.

---

## 6. Wiring

**File:** `src/alpha/engines/bootstrap/engine.py` — `_wire_engines()`

```
IngestionMonitor(symbols)
    ↓ injected into
LiveIngestionEngine  →  writes on_bar_received / on_record_received / check_health
PositionMonitor      →  reads is_signal_allowed before every _check_entry
```

The monitor is created before all other engines in `_wire_engines()` and stored as
`self._ingestion_monitor` on the BootstrapEngine for future exposure to the REST API
or dashboard.

---

## What is NOT yet implemented

The following items from the full spec were deferred:

- **Macro calendar kill condition**: block entries 30 min before / 60 min after CPI, FOMC, NFP, etc.
- **Sequence number gap detection**: Databento `trades` schema carries sequence numbers; gaps
  would require tracking the expected next sequence per symbol.
- **FAILED state reset**: currently sticky — requires a process restart to clear.
- **Signal quality log**: structured JSONL log of every WOULD_ENTER with full context (setup,
  grade, BAI, delta) for offline backtesting. The shadow-mode track record depends on this.
- **Raw storage**: tick-level Parquet archiving for replay and pattern discovery.
