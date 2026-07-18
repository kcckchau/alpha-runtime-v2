# Phase 0 Design Note: Shadow Level-Interaction Research Pipeline

**Date:** 2026-07-15  
**Scope:** Phase 0 only — LevelBarObservation recording. Nothing beyond this.

---

## Reconnaissance findings

Each finding below references the exact file and line verified before any code was written.

### 1. Authoritative M1 event source

**Finding:** Subscribe to `EventType.PIPELINE_OUTPUT` via `PipelineOutputEvent`.

`BarPipeline` (`src/alpha/engines/flow/pipeline.py:106`) subscribes to `BAR_BUNDLE`, runs all five stages sequentially (Feature → MarketState → Thesis → Setup → Scoring), then publishes `PipelineOutputEvent` at line 192. When a `PipelineOutputEvent` arrives, all stage outputs are already embedded in `event.bar_snapshot` — no separate engine query is needed.

`PipelineOutputEvent.bar_snapshot` (`src/alpha/models/events.py:258`) carries:
- `snap.vwap` — session VWAP from FeatureEngine
- `snap.orb_high` / `snap.orb_low` — opening range levels (None when `orb_state == NOT_SET`)
- `snap.atr_14` — M1 ATR-14
- `snap.session_phase` — current session phase
- `snap.orb_state` — ORBState enum value
- `snap.bar` — a `Bar` object with open/high/low/close/volume

`event.timestamp` = `bundle.timestamp` = bar open time (convention from `BaseEvent.timestamp` docstring at `src/alpha/models/events.py:53`).

**Implication:** `LevelObserver` subscribes to `PIPELINE_OUTPUT`. No `ContextEngine.get_context()` call is made. `ContextEngine` subscribes to `BAR` events directly (`src/alpha/engines/context/engine.py:146`) for its own session state, but the observer does not need it.

### 2. ContextEngine synchrony

**Finding:** Not relevant for the observer — we read from `PipelineOutputEvent.bar_snapshot`, not from `ContextEngine.get_context()`.

`ContextEngine` (`src/alpha/engines/context/engine.py:18`) says: "Distance computation is deferred to get_context(), which is called AFTER bus.flush() completes." Since the observer subscribes to `PIPELINE_OUTPUT` (not `BAR` or `BAR_BUNDLE`), and `PipelineOutputEvent` already embeds the FeatureEngine output including VWAP/ORH/ORL, there is no synchrony issue.

### 3. ORH/ORL availability

**Finding:** `snap.orb_high` and `snap.orb_low` are `None` when `snap.orb_state == ORBState.NOT_SET` (`src/alpha/models/snapshot.py:31–33`). They become non-None when FeatureEngine locks the opening range. ORH and ORL are always locked together (both appear in the same `BarSnapshot` field set — there is no state where one is set and the other is not).

**Assumption:** ORH and ORL are always set/unset together. If this is ever violated (e.g. asymmetric range detection), the observer will emit an ORH row but no ORL row, which is correct behaviour — the missing level is simply skipped.

The observer records `orb_state` on every row so research can distinguish pre-lock from post-lock observations without a separate join.

### 4. Tick size and ATR availability

**Finding:** Tick size is available from `SymbolRegistry.get(symbol).tick_size` (`src/alpha/engines/execution/v1_risk.py:289–292`). Defined per instrument in `src/alpha/instruments.py:22–69`. MNQ tick size = `Decimal("0.25")`.

**Finding:** ATR is available as `snap.atr_14` (M1 14-period ATR, `src/alpha/models/snapshot.py:81`). Also `snap.atr_30` exists. The brief specified ATR-20; ATR-14 is what exists. Recorded as `volatility_definition_version="atr14_m1_v1"`.

**VWAP is not tick-aligned.** VWAP is a running volume-weighted average and will have sub-tick decimal values. All VWAP distance computations use `ROUND_HALF_UP` via `Decimal.to_integral_value()`. ORH/ORL are bar prices and are tick-aligned; they use exact integer arithmetic.

### 5. Existing Parquet conventions

**Finding:** The production storage engine uses `pyarrow` + `pyarrow.parquet` (`src/alpha/engines/storage/parquet.py:12–13`). Atomic write pattern: temp file → `os.replace()` (line 125). Compression: `snappy` (from `StorageSettings.compress`, `src/alpha/config/settings.py:58`).

The research pipeline uses the same library and the same atomic write pattern. It does **not** use `ParquetStore` directly because research has a different partition scheme (by `session_date` not `year/month/day`) and different naming conventions.

Production partition: `{root}/{data_type}/{symbol}/year={Y}/month={M}/day={D}/data.parquet`  
Research partition: `{research_root}/level_observations/{symbol}/session_date={YYYY-MM-DD}/part-{run_id}-{seq:06d}.parquet`

The `run_id` in the file name ensures that multiple runs writing to the same session date never overwrite each other. The `seq` counter is monotonic within a run.

### 6. EventBus failure isolation

**Finding:** `EventBus._drain()` (`src/alpha/core/event_bus.py:192–201`) catches all exceptions from handlers and logs them. A subscriber exception never propagates to the publisher or other subscribers.

**Implication:** The observer's `_handle()` method wraps `_process()` in a try/except for belt-and-suspenders safety, but EventBus already guarantees isolation.

The observer subscribes with `drop_if_full=True` (`src/alpha/core/event_bus.py:43`) so that if the observer's queue fills (e.g. due to a slow flush), `PipelineOutputEvent` messages are dropped rather than back-pressuring the main pipeline.

### 7. Run ID

**Finding:** No existing run ID concept in the bootstrap engine. Generated at observer startup as `f"{run_mode}-{date}-{uuid4().hex[:8]}"`. Stored on the observer instance and embedded in every observation row and Parquet file name.

### 8. Session and timezone semantics

**Finding:** `calendar.session_date(dt)` returns an ET date (`src/alpha/calendar/base.py:80`). For CME futures (`src/alpha/calendar/cme.py:111`), a bar at 23:59 ET belongs to the next calendar date (CME trade date convention). This matches the convention used by `SetupEngine.session_setup_context()`.

The observer uses `calendar_for_symbol(sym_obj)` (same as ContextEngine, line 186 of context/engine.py) to derive `session_date`. If the symbol is unregistered, it falls back to UTC date as a safe degradation.

---

## Assumptions

The following could not be verified directly from code and are flagged:

| # | Assumption | Depends on | Failure mode |
|---|---|---|---|
| A1 | ORH and ORL are always set/unset together in BarSnapshot | FeatureEngine opening range logic | One row emitted instead of zero for that level type — acceptable |
| A2 | `Bar.timestamp` in `PipelineOutputEvent.bar_snapshot.bar` is the bar open time | `BarEvent` + `BarSnapshot` convention; documented at `models/events.py:53` | `bar_timestamp` would refer to wrong time boundary — affects session_date derivation |
| A3 | `BarBundleEvent.metadata.event_id` is unique per bar per symbol | EventMetadata uuid4 factory at `models/events.py:38` | `source_bar_id` collisions — dedup falls back to deterministic obs_id |
| A4 | `sym_obj.tick_size` is always set for all registered symbols | SymbolRegistry + instruments.py | Falls back to `Decimal("0.25")` — correct for MNQ/MES/NQ/ES, wrong for M2K (0.10) or MYM (1.0) |

---

## What is deferred (Phase 1+)

Do not implement any of the following until Phase 0 data exists and has been inspected on real replay output:

- `LevelEvent` (PROXIMITY_ENTER, CROSS_UP, etc.)
- `LevelInteractionRecord` and episode segmentation
- `LevelState` (mutable runtime projection per level)
- `InteractionOutcome` labeling (forward returns, MFE/MAE, structural outcomes)
- ONH, ONL, PDH, PDL level types
- `OpportunityEngine`
- EMA, delta, TPS, or order flow fields on `LevelBarObservation`
- Any modification to SetupEngine, ThesisEngine, ScoringEngine, RiskEngine, or OrderEngine
- Semantic setup deduplication

---

## Why LevelBarObservation instead of sparse LevelEventType

A sparse event stream (PROXIMITY_ENTER / CROSS_UP / etc.) discards wick geometry. A bar where `low=5809, close=5813, ORL=5811` would fire a single CROSS_DOWN event and lose the fact that the bar closed above the level. From sparse events alone, you cannot reconstruct:

- Maximum excursion below level during the bar (wick depth)
- Whether a cross was wick-only or close confirmation
- How many bars had wick overlap without a closing cross

All of these are necessary inputs to distinguish "aggressive sweep with immediate reclaim" from "brief wick touch." The bar-level observation preserves all four OHLC distances and the overlap flag, leaving nothing irretrievably discarded. Alternative sweep definitions can be computed offline from the same raw data by varying the threshold on `low_distance_ticks` vs `close_distance_ticks`.
