# Bootstrap & Catchup Logic

## Overview

On startup (LIVE / PAPER mode), the runtime must warm up all downstream engines with
historical bars before live data arrives. The bootstrap sequence does this in two phases:

1. **Catchup** — fetch historical bars and emit them through the pipeline (`is_replay=True`)
2. **Live** — start the live feed from just before the historical edge, letting it naturally
   bridge the gap and continue in real time

---

## Data Sources

| Timeframe | Source | Reason |
|-----------|--------|--------|
| M1 | Databento historical API (always fresh) | Small window (3 days), cheap (~6–10s), Parquet cache can be stale if vendor was delayed or process crashed mid-session |
| M5 | Resampled from M1 | Databento has no `ohlcv-5m` schema |
| H1 | Parquet cache + gap-fill from API | Large window (60+ days), expensive to re-fetch (~17s), high tolerance for small gaps |
| D1 | Parquet cache + gap-fill from API | Large window (1.5× warmup_bars days), expensive to re-fetch (~45s), high tolerance for small gaps |

---

## Databento Availability Edges

`get_dataset_range()` returns a single dataset end (M1 granularity, ~10 min lag).
H1 and D1 are derived conservatively from that same call:

| Schema | Edge derivation |
|--------|----------------|
| `ohlcv-1m` | Raw dataset end |
| `ohlcv-1h` | Dataset end floored to hour |
| `ohlcv-1d` | Dataset end floored to day |

One metadata call at startup populates all three edges (`_availability_ends()`).

---

## Catchup Sequence (`CatchupService.run()`)

For each symbol:

1. **Fetch M1** from API: `[m1_end - N_days, m1_end]`
2. **Resample M5** from M1 bars (no API call needed)
3. **Load H1** from Parquet cache, gap-fill missing ranges from API, save gaps back to cache
4. **Load D1** from Parquet cache, gap-fill missing ranges from API, save gaps back to cache
5. **Emit in dependency order**: D1 → H1 → M5 → M1 (all `is_replay=True`)

Returns: `(context_map, m1_end)`

---

## Live Handoff

After catchup completes:

```
replay_start = m1_end - timedelta(minutes=1)
live_feed.set_replay_start(replay_start)
live_feed.start()
```

The live Databento gateway replays the 1-bar overlap window from `replay_start`, then
continues in real time. No buffer/drain phase needed — the pipeline is fully warm before
the first live bar arrives.

---

## `is_replay` Flag

| Value | Set by | Effect |
|-------|--------|--------|
| `True` | All catchup-emitted bars | StorageEngine skips write (bars already in Parquet or freshly saved via `save_bar`); Telegram skips notification |
| `False` | Live feed bars | StorageEngine persists; Telegram notifies on setup state changes |

At the catchup→live transition, `_reconcile_active_setups()` re-publishes any
active setups as `is_replay=False` so they land in Parquet exactly once.
Setup IDs are deterministic (uuid5 of symbol+type+bar_ts), so restarting produces the
same IDs — the upsert overwrites rather than duplicates.

---

## Gap Detection (H1 / D1)

`_missing_ranges()` walks the sorted stored bars and identifies time gaps larger than
one timeframe step. For H1/D1, gaps that span a session boundary (overnight/weekend)
are skipped — the calendar is used to compare `session_key(left)` vs `session_key(right)`.

---

## Warmup Window Sizes

Driven by `settings.historical.*_warmup_bars`:

| Timeframe | Default window |
|-----------|---------------|
| M1 | `max(3, minute1_warmup_bars // 390 + 1)` calendar days |
| H1 | `max(60, hourly_warmup_bars // 23 + 15)` calendar days (23h futures sessions) |
| D1 | `int(daily_warmup_bars * 1.5)` calendar days |

Futures symbols get a tighter D1 cap: `min(d1_start, d1_end - 45 days)` to avoid
fetching pre-roll data that doesn't reflect the current front contract.

---

## Files

| File | Role |
|------|------|
| `src/alpha/engines/bootstrap/engine.py` | Orchestrates lifecycle, calls `CatchupService.run()`, starts live feed |
| `src/alpha/engines/bootstrap/catchup.py` | `CatchupService` — all fetch, resample, emit logic |
| `src/alpha/engines/historical/sources/databento.py` | `availability_end()`, `fetch_bars()`, `_safe_end()` |
| `src/alpha/engines/backfill/engine.py` | Uses `CatchupService.fetch_range()` for standalone backfill CLI |
