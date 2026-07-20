# Alpha Runtime v2 — Claude Working Instructions

## Project Purpose

Intraday futures trading system for MNQ (Micro Nasdaq). Phase 2 complete (Telegram live alerts, shadow mode active). Phase 3 (live execution) not started — do not rush it.

Two parallel tracks:
- **Funded trader path**: prove signals → TopStep/Apex prop firm → trade their capital
- **SaaS track**: productize signal engine

**Do not change live setup, scoring, thesis, or market-state behavior without explicit instruction.**

---

## Architecture

### Engine Pipeline (sequential, ordered)

```
BarEvent / QuoteEvent
    │
    ▼
FeatureEngine         → BarSnapshot (per M1 bar)
    │
    ▼
MarketStateEngine     → regime / session context
    │
    ▼
SetupEngine           → SetupEvent (detected patterns)
    │
    ▼
ScoringEngine         → scored setup candidates
    │
    ▼
ThesisEngine          → trade thesis + Telegram alert
    │
    ▼
RiskEngine / OrderEngine / PositionMonitor   (Phase 3)
```

### Dual-frequency design
- **1m bars**: setup detection, feature computation, EMA/VWAP context
- **1s bars**: entry/exit timing (future — PositionMonitor sprint)

### Key architectural rules (from ARCHITECTURE.md)
- Engines do not own storage — they emit events, StorageEngine persists
- All engines consume normalized contracts only (BarEvent, QuoteEvent etc.) — never vendor types
- BarSnapshot is the FeatureEngine output contract — not a god object
- FeatureEngine: pure observation and geometry. No rule interpretation, no regime judgment.
- MarketStateEngine: interprets observations into regime. Persistence, transitions, context flags live here.
- SetupEngine / ScoringEngine / ThesisEngine: strategy logic. Do not pollute with feature computation.

---

## FeatureEngine — What Belongs Here

**YES — belongs in FeatureEngine / BarSnapshot:**
- Raw EMA values (ema9_1h, ema21_1h, etc.)
- ATR values
- Continuous geometry (distances in ATR units, slopes in ATR/bar)
- Stack direction (structural EMA ordering — observable fact)
- Price location vs ribbon/VWAP (observable)
- Width percentile, width slopes (rolling but still geometric)
- Event flags (vwap_cross_up — a point-in-time observation)
- Watermarks (htf_1h_watermark, htf_5m_watermark)

**NO — does not belong in FeatureEngine:**
- Persistence counters (bullish_stack_persistence etc.) → MarketStateEngine
- Transition counts, full-cross counts → MarketStateEngine / H1RibbonStateTracker
- Context flags (bullish_ribbon_context, chop_context) → MarketStateEngine
- Threshold-classified states that depend on calibrated values → MarketStateEngine
- Any field that requires episode tracking or deduplication → LevelInteractionEngine

---

## EMA Slope Convention (norm3_v1)

All EMA slopes use the same formula across all timeframes:

```python
slope = (ema[t] - ema[t-3]) / (3 * atr30_timeframe)   # units: ATR/bar
```

Policy version: `SLOPE_POLICY_VERSION = "norm3_v1"` (in `src/alpha/features/slope.py`)

**UNCALIBRATED thresholds** (do not treat as validated gates):
- 1M flat threshold: ±0.05 ATR/bar
- 5M flat threshold: ±0.03 ATR/bar
- 1H flat threshold: ±0.02 ATR/bar

When thresholds change, bump the policy version so historical Parquet outputs remain reproducible.

---

## 1H EMA Ribbon (ema_1h_ribbon_v1)

Policy: `EMA_1H_RIBBON_POLICY_VERSION = "ema_1h_ribbon_v1"`

- Ribbon = EMA9 + EMA21 + EMA50 only. SMA200 is a separate structural anchor.
- H1 ATR computed from sealed H1 true ranges — never derived from M1.
- Minimum samples: `ATR30_1H_MIN_SAMPLES = 3`
- Width percentile uses last 60 sealed H1 bars (PIT safe: current bar excluded).
- `h1_close_*` fields: sealed H1 close only — feed rolling history.
- `m1_close_*` fields: live M1 close vs carry-forward ribbon — never contaminate H1 history.
- `htf_1h_watermark`: timestamp of last sealed H1 bar reflected in snapshot.

---

## Data Layout

```
data/
  parquet/          # market data by symbol/date
  replay_cache/     # pre-computed feature snapshots from replay_cache.py
  replay_results/   # replay output (SetupEvents, ThesisEvents)
  research/         # research exports
  backtest_results/
```

Primary symbol: `MNQ-09` (default in replay scripts).

---

## Key Scripts

| Script | Purpose |
|--------|---------|
| `scripts/replay_day.py` | Replay a single day, emit events |
| `scripts/replay_cache.py` | Pre-compute BarSnapshot JSON cache for research |
| `scripts/research_replay.py` | Research-mode replay with label generation |
| `scripts/backtest.py` | Backtest over date range |
| `scripts/export_research_bundle.py` | Export Parquet bundles for analysis |

Default usage:
```bash
python scripts/replay_day.py --symbol MNQ-09 --date 2026-07-03
python scripts/replay_cache.py --symbol MNQ-09 --date 2026-07-03
```

---

## Testing Rules

- **Run tests before every commit**: `pytest tests/unit/`
- New FeatureEngine features require unit tests with known-input assertions (not just "no crash")
- Tests must use controlled synthetic bar inputs — do not depend on real market data files
- PIT safety tests: verify current bar is excluded from its own rolling window
- Watermark tests: verify both H1-before-M1 and M1-before-H1 event orderings at hourly boundary
- Do not use `or True` to bypass assertions in tests

---

## Commit Rules

- **Always commit after each logical change** — do not batch unrelated changes into one commit
- Commit message: what changed + why, not just what
- Use `--no-ff` when merging feature branches to master to preserve topology

---

## Branch Convention

```
master                        ← stable, always tested
feature/<thing>               ← active feature work
refactor/<thing>              ← refactoring only
fix/<thing>                   ← bug fixes
```

Current active: `feature/volume-profile-levels`

---

## What Is Calibrated vs Uncalibrated

| Feature | Status |
|---------|--------|
| norm3 slope values (continuous) | Research-ready |
| 1M flat threshold ±0.05 | UNCALIBRATED |
| 5M flat threshold ±0.03 | UNCALIBRATED (provisional) |
| 1H flat threshold ±0.02 | UNCALIBRATED |
| slope_alignment (bullish/bearish/flat/mixed) | Descriptive, not validated |
| ema_mtf_strength / dispersion / accel | Not in live logic |
| SetupEngine / ScoringEngine direction gates | Still consume uncalibrated direction |

---

## Things Claude Should Never Do

- Modify SetupEngine, ScoringEngine, ThesisEngine, or MarketStateEngine logic without explicit instruction
- Add stateful/interpretation fields to FeatureEngine (persistence counters, context flags, regime outputs)
- Add error handling or validation for internal code paths that can't fail
- Add docstrings or comments to code that wasn't changed
- Speculative abstractions — only build what the current task requires
- Skip `pytest tests/unit/` before committing
- Use `--no-verify` to bypass hooks
