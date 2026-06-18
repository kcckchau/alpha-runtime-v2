# Setup Detection Pipeline

## Overview

```
BarEvent → FeatureEngine → MarketStateEngine → SetupEngine → ScoringEngine
                ↓                  ↓                ↓
          BarSnapshot          MarketState        Setup
                                                (FORMING→CONFIRMED)
                                                      ↓
                                               Score + Grade
```

Every 1-minute bar flows through four sequential stages before a scored, actionable setup is produced. Engines communicate via an async pub/sub EventBus; each stage subscribes to the previous stage's output event.

---

## 1. Input: BarEvent

Raw OHLCV data from one of three sources:

| Source | Description |
|---|---|
| Live IBKR adapter | Real-time 1m bars from Interactive Brokers |
| Historical adapter | Parquet / Polygon replay for backtesting |
| Date replay (`date_replay.py`) | Structured historical replay with warmup phase |

**Key fields:**

| Field | Type | Notes |
|---|---|---|
| `symbol` | str | Ticker |
| `timestamp` | datetime | Bar open time |
| `open / high / low / close` | Decimal | OHLC prices |
| `volume` | int | Bar volume |
| `is_partial` | bool | True for in-progress real-time bars |
| `is_replay` | bool | Distinguishes live vs historical |
| `vwap` | Decimal \| None | Optional VWAP from data source |

---

## 2. FeatureEngine → BarSnapshot

Maintains rolling per-symbol state. On each bar it computes ~92 fields and emits a `BarSnapshot`.

### Key BarSnapshot fields

**VWAP**

| Field | Description |
|---|---|
| `vwap` | Cumulative session VWAP = Σ(typical_price × volume) / Σ(volume) |
| `vwap_deviation_pct` | (close − vwap) / vwap × 100 |
| `vwap_slope` | % rate-of-change vs prior bar |
| `vwap_slope_direction` | "up" / "flat" / "down" (flat = \|slope\| ≤ 0.002%) |
| `vwap_upper_band / lower_band` | ±1 std dev bands |

**EMAs**

| Field | Description |
|---|---|
| `ema_9`, `ema_20`, `ema_50` | Exponential moving averages |
| `ema_9_slope`, `ema_20_slope` | % rate-of-change vs prior bar |
| `ema_9_slope_direction` | "up" / "flat" / "down" (flat = \|slope\| ≤ 0.005%) |
| `ema_20_slope_direction` | Same thresholds as EMA9 |

**Volatility & Volume**

| Field | Description |
|---|---|
| `atr_14` | 14-bar Average True Range (stop-sizing benchmark) |
| `relative_volume` | Bar volume vs 20-bar average |
| `cumulative_volume` | Session total volume (RTH only) |

**VWAP structure flags**

| Field | Description |
|---|---|
| `bars_above_vwap`, `bars_below_vwap` | Consecutive bars on each side |
| `vwap_cross_up`, `vwap_cross_down` | One-time cross triggers |
| `vwap_cross_up_after_bars` | Bars spent below before the cross |
| `swept_below_vwap` | Low < VWAP but close ≥ VWAP (wick only) |
| `vwap_deviation_shrinking` | Distance to VWAP decreased vs prior bar |

**Bar structure flags**

| Field | Description |
|---|---|
| `is_higher_high`, `is_lower_low` | Bar vs prior bar comparison |
| `is_lower_high` | This bar's high < prior bar's high |
| `recent_lower_low` | A lower low was made within the last 10 bars |
| `bar_close_position_pct` | (close − low) / (high − low); 0 = bottom, 1 = top |
| `is_above_vwap`, `is_above_ema20` | Boolean position flags |

**Session & ORB**

| Field | Description |
|---|---|
| `session_phase` | PRE_MARKET / OPENING_RANGE / EARLY / MID / POWER_HOUR / CLOSING |
| `bars_since_open` | RTH bar count |
| `is_new_hod`, `is_new_lod` | New session high/low on this bar |
| `intraday_high`, `intraday_low` | Running session extremes |
| `orb_high`, `orb_low`, `orb_range` | Opening range levels (default 15m) |
| `orb_state` | NOT_SET / BREAKOUT_UP / BREAKOUT_DOWN / INSIDE |
| `orb_cross_down`, `orb_cross_up` | One-time triggers on ORB level break |
| `bars_since_orb_breakdown` | Bars elapsed since initial ORB break |
| `swept_orl` | Low < orb_low (wicked below opening range low) |

---

## 3. MarketStateEngine → MarketState

Classifies session structure on every bar. Key fields:

**Trend**

| Field | Logic |
|---|---|
| `trend` | EMA9 > EMA20 and close > EMA9 → TRENDING_UP; EMA9 < EMA20 and close < EMA9 → TRENDING_DOWN; else CHOPPY |
| `trend_strength` | min(1.0, \|vwap_deviation_pct\| / 5.0) |
| `trend_bars` | Consecutive bars confirming current trend |

**VWAP regime**

| `vwap_state` | Condition |
|---|---|
| RECLAIMING | Cross-up event this bar |
| REJECTING | Cross-down event this bar |
| ABOVE / BELOW | Default based on close vs VWAP |

**Day type (locks once per session)**

| Field | Description |
|---|---|
| `day_type` | TREND_UP / TREND_DOWN / RANGE / BALANCED |
| `day_type_status` | FORMING / LOCKED_HEALTHY / STRESSED / INVALIDATED |

Gate conditions before locking: ≥30 RTH bars elapsed, ORB established, 3 consecutive bars agreeing, confidence ≥ 0.55. Once locked, stays for the session.

**Live bias (recalculated every bar, never locks)**

`live_bias`: BULLISH / BEARISH / TRANSITIONING_BULLISH / TRANSITIONING_BEARISH / NEUTRAL / UNKNOWN

Primary driver: trend × VWAP state combination. Used by ScoringEngine for cap logic.

**Trade permission**

| Field | Source |
|---|---|
| `trade_long_allowed` | Derived from day_type_status + live_bias |
| `trade_short_allowed` | Same |
| `trade_permission_reason` | Human-readable string |

**Other**

| Field | Description |
|---|---|
| `orb_volume_confirmed` | Volume confirms ORB breakout direction |
| `structure_score` | 0–1 clean price structure metric |
| `confidence` | 0–1 day type conviction |

---

## 4. SetupEngine: 12 Detectors

Each bar, SetupEngine reads the latest BarSnapshot and MarketState synchronously and runs all detectors. Active setups advance through a state machine.

### State machine

```
FORMING → CONFIRMED → TRIGGERED  (terminal)
   ↓           ↓
FAILED    INVALIDATED             (terminal)
              EXPIRED             (terminal)
```

### Quality gates applied at CONFIRMED

- **Risk-width gate:** if `|entry − stop| > 1.5× ATR-14` (or `> 0.5%` of entry as fallback) → invalidated
- **10-bar cooldown:** after any setup closes, the same type cannot reopen on the same symbol for 10 bars

### Short setups

| Setup Type | FORMING Conditions | CONFIRMED Conditions | Entry | Stop |
|---|---|---|---|---|
| `VWAP_REJECTION` | Cross below VWAP from ≥2 bars above; >0.05% below; lower half close | Hold below VWAP; RVOL ≥ 1.0; no higher high | Forming bar low | Forming bar high |
| `VWAP_FAILED_RECLAIM_SHORT` | ≥3 bars below VWAP; high ≥ VWAP × 0.9995 (tested VWAP); weak close; EMA9 ≤ EMA20; VWAP slope not rising; EMA9 slope not rising | Hold below VWAP; no new high above forming; `bar.low < forming.low`; RVOL ≥ 0.8 | Forming bar low | Forming bar high |
| `TREND_PULLBACK_SHORT` | ≥5 bars below VWAP; EMA9 < EMA20; EMA9 + EMA20 slopes not rising; `recent_lower_low`; not making new LOD; bar high within 0.3% of EMA9; close < EMA9 | Lower high; `bar.low < forming.low`; still below VWAP/EMA9/EMA20; RVOL ≥ 0.8 | Forming bar low | Forming bar high |
| `ORB_BREAKDOWN` | `orb_cross_down`; below VWAP; close < OR low | Hold below OR low; RVOL ≥ 1.0 | Forming bar close | OR low × 1.001 |

### Long setups

| Setup Type | FORMING Conditions | CONFIRMED Conditions | Entry | Stop |
|---|---|---|---|---|
| `VWAP_UNDERCUT_RECLAIM` | 1–5 bars below VWAP; low ≤0.15% below VWAP; close in upper half | VWAP cross up; RVOL ≥ 1.0; upper half close; no lower low | VWAP | Forming bar low × 0.9995 |
| `SWEEP_RECLAIM` | Swept OR low (wicked below); closed back above; RVOL ≥ 1.1 | Hold above OR low 1–5 bars; VWAP deviation shrinking or above VWAP; no lower low; RVOL ≥ 0.8 | Confirmation bar high | Forming bar low × 0.9995 |
| `FAKE_BREAKDOWN` | ≥1 bar below VWAP first; swept OR low ≤0.3%; strong close back above; RVOL ≥ 1.2 | Full VWAP reclaim; close > OR mid; close ≥ EMA9; no lower low; RVOL ≥ 1.2 | Close on reclaim bar | Forming bar low × 0.9995 |
| `DEEP_EXHAUSTION_RECLAIM` | VWAP deviation ≤ −0.35%; new session low; RVOL ≥ 1.5; close off lows (>35% of range) | ≥3 bars; no new low; EMA9 reclaim; VWAP deviation shrinking | Confirmation bar high | Forming bar low × 0.9995 |
| `VWAP_RECLAIM` | Cross above VWAP from ≥2 bars below; >0.05% above; upper half close | Hold above VWAP; RVOL ≥ 1.0; no lower low | Forming bar high | Forming bar low |
| `HOD_BREAKOUT` | Session high > ORB high; above VWAP; higher highs; close within 0.2% of HOD | New HOD; RVOL ≥ 1.2 | Intraday high | Confirmation bar low |
| `TREND_PULLBACK` (long) | ≥5 bars above VWAP; pulling back toward VWAP; within 0.5% of VWAP | Within 0.25% of VWAP; still above; RVOL ≥ 0.8; no lower low | VWAP | Confirmation bar low |
| `ORB_BREAKOUT` | — | — | — | — |

> `ORB_BREAKOUT` is not yet implemented (always returns False).

### Target calculation

Target uses a risk multiplier (R) based on session narrative:

| Day type / Live bias | Multiplier |
|---|---|
| Trend-aligned | 4R |
| Counter-trend | 2R |
| Range day | 1.5R |
| Balanced | 3R |

Live bias (per-bar) takes precedence over locked day_type for target calculation.

---

## 5. ScoringEngine: Point-based Rubric

Fires on CONFIRMED setups only. Uses **two bar snapshots**:

- `setup.bar_snapshot` — forming bar snapshot (structural conditions, slope directions)
- `confirm_snap` — current bar from FeatureEngine at event time (RVOL, close position, ATR)

### Grade thresholds

| Score | Grade | Trading guidance |
|---|---|---|
| 0–2 | B | Observe only |
| 3–4 | A | Small size |
| 5–6 | A+ | Normal size |
| ≥7 | SSS | Full-size / primary trade |

### Short rubric

**Base scores:**

| Setup Type | Base |
|---|---|
| `VWAP_REJECTION` | 1 |
| `VWAP_FAILED_RECLAIM_SHORT` | 3 |
| `TREND_PULLBACK_SHORT` | 3 |
| `ORB_BREAKDOWN` | 2 |

**Add-ons (+1 each):**

| Condition | Check |
|---|---|
| VWAP slope down | `vwap_slope_direction == "down"` |
| EMA9 slope down | `ema_9_slope_direction == "down"` |
| EMA20 slope down | `ema_20_slope_direction == "down"` |
| Recent lower low | `recent_lower_low == True` |
| Live bias bearish | `live_bias` in {BEARISH, TRANSITIONING_BEARISH} |
| Market trending down | `trend == TRENDING_DOWN` |
| Confirm bar lower 40% | `bar_close_position_pct ≤ 0.40` |
| RVOL ≥ 1.0 | — |
| RVOL ≥ 1.3 | Stacks with ≥1.0 for +2 total |
| Tight risk width | `\|entry − stop\| ≤ 1× ATR-14` |

**Penalties:**

| Condition | Points | Check |
|---|---|---|
| VWAP slope rising | −1 | `vwap_slope_direction == "up"` |
| EMA20 slope rising | −1 | `ema_20_slope_direction == "up"` |
| Live bias bullish | −2 | `live_bias == BULLISH` |
| Chasing extension | −1 | `vwap_deviation_pct < −0.50` |
| Market not bearish | −1 | `live_bias` not in {BEARISH, TRANSITIONING_BEARISH} |

**Hard caps:**

| Rule | Cap |
|---|---|
| `VWAP_REJECTION` (structurally weaker first cross) | Max A |
| Any short when `live_bias == BULLISH` | Max A |

### Long rubric

**Base score: 3**

**Add-ons (+1 each):**

| Condition | Check |
|---|---|
| Trend aligned bullish | `trend == TRENDING_UP` and `is_above_vwap` |
| Price above VWAP | `is_above_vwap` |
| Price above EMA20 | `is_above_ema20` |
| Live bias bullish | `live_bias` in {BULLISH, TRANSITIONING_BULLISH} |
| RVOL ≥ 1.0 | — |
| RVOL ≥ 1.5 | Stacks with ≥1.0 for +2 total |
| ORB volume confirmed | `orb_volume_confirmed` |
| Tight risk width | `\|entry − stop\| ≤ 1× ATR-14` |

**Penalties:**

| Condition | Points | Check |
|---|---|---|
| Live bias bearish | −2 | `live_bias == BEARISH` |
| Market trending down | −1 | `trend == TRENDING_DOWN` |
| Chasing extension up | −1 | `vwap_deviation_pct > 0.50` |
| Market not bullish | −1 | `live_bias` not in {BULLISH, TRANSITIONING_BULLISH} |

**Hard cap:** Any long when `live_bias == BEARISH` → max A

### Output

Score, grade, and reason lists are patched onto the confirmed Setup via `SetupEngine.patch_setup_score()`:

- `score` (int) — raw point total
- `grade` (SetupGrade) — C / B / A / A+ / SSS
- `score_reasons` (list[str]) — human-readable +point explanations
- `score_penalties` (list[str]) — human-readable −point explanations

---

## Timeframe note

Only **1-minute bars** flow through this detection pipeline. 5m / H1 / D1 bars are stored in Parquet but are not currently wired into the detection or scoring stages. The 5m structural bias is approximated by:

- `bars_below_vwap >= N` thresholds in setup detectors
- `recent_lower_low` (lower low within last 10 bars)
- MarketState `trend` and `day_type` classification

A real 5m structural bias feed is a planned future enhancement.

---

## Event bus subscription order

Subscriptions are registered in bootstrap order. EventBus guarantees FIFO delivery per subscriber, so each stage always sees a fully updated prior stage before processing.

```
1. FeatureEngine    — subscribes to BAR events
2. MarketStateEngine — subscribes to BAR events
3. SetupEngine      — subscribes to BAR events
4. ScoringEngine    — subscribes to SETUP events (CONFIRMED only)
```
