from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, SessionPhase
from alpha.models.flow import BarFlowContext


class BarSnapshot(BaseModel):
    """Output contract of the Feature Engine for a single bar across all features."""

    symbol: str
    timestamp: datetime
    timeframe: BarTimeframe
    bar: Bar

    # ── VWAP ────────────────────────────────────────────────────────────────
    vwap: Decimal
    full_session_vwap: Decimal | None = None   # resets at CME session boundary (18:00 ET prior day)
    rth_vwap: Decimal | None = None            # resets at RTH open (09:30 ET); None before first RTH bar
    vwap_upper_band: Decimal | None = None      # +1 std dev
    vwap_lower_band: Decimal | None = None      # -1 std dev
    vwap_deviation_pct: float = 0.0             # (close - vwap) / vwap * 100

    # ── Volume ───────────────────────────────────────────────────────────────
    cumulative_volume: int = 0
    relative_volume: float | None = None        # vs historical avg at same time-of-day

    # ── Opening Range ─────────────────────────────────────────────────────────────
    or_high: Decimal | None = None
    or_low: Decimal | None = None
    or_range: Decimal | None = None
    or_established: bool = False          # True once the opening range window has closed
    or_position: str | None = None        # "ABOVE" | "INSIDE" | "BELOW" — where price is relative to OR

    # ── Session context ──────────────────────────────────────────────────────
    session_phase: SessionPhase = SessionPhase.CLOSED
    bars_since_open: int = 0

    # ── Trend indicators — 1m ────────────────────────────────────────────────
    ema_9: Decimal | None = None
    ema_21: Decimal | None = None             # EMA21 (user's primary 1m fast EMA)
    ema_50: Decimal | None = None
    ema9_1m_slope_norm_3: float | None = None   # (EMA9[t] - EMA9[t-3]) / (3 * atr30_1m); ATR/bar; None until 4 EMA values
    ema9_1m_slope_direction: str | None = None  # "up" / "flat" / "down" (classify_slope, norm3_v1)
    ema21_1m_slope_norm_3: float | None = None  # (EMA21[t] - EMA21[t-3]) / (3 * atr30_1m)
    ema21_1m_slope_direction: str | None = None
    vwap_slope: float | None = None           # % rate-of-change of VWAP vs prior bar
    vwap_slope_direction: str | None = None   # "up" / "flat" / "down"  (flat = |slope| ≤ 0.002%)

    # ── Trend indicators — 5m (carry-forward: updated on each M5 bar) ────────
    ema9_5m: Decimal | None = None            # 5m EMA9
    ema21_5m: Decimal | None = None           # 5m EMA21
    atr30_5m: Decimal | None = None           # 5m ATR30; None until atr30_5m_samples >= ATR30_5M_MIN_SAMPLES
    atr30_5m_samples: int = 0                 # true-range samples accumulated (research/audit field)
    ema9_5m_slope_norm_3: float | None = None   # (EMA9[t] - EMA9[t-3]) / (3 * atr30_5m); ATR/bar
    ema9_5m_slope_direction: str | None = None
    ema21_5m_slope_norm_3: float | None = None  # (EMA21[t] - EMA21[t-3]) / (3 * atr30_5m)
    ema21_5m_slope_direction: str | None = None
    htf_5m_watermark: datetime | None = None  # sealed 5m bar timestamp reflected in this snapshot (PIT audit)

    # ── Trend indicators — 1h (carry-forward: updated on each H1 bar) ────────
    # SMA200 requires 200 H1 bars (~8.5 trading days); will be None until warm.
    # All fields below reflect the last sealed H1 bar and are carried forward unchanged
    # until the next H1 bar seals. The H1 bar that coincides with the current M1
    # timestamp may not yet be reflected — see htf_1h_watermark.
    ema9_1h: Decimal | None = None            # 1h EMA9
    ema21_1h: Decimal | None = None           # 1h EMA21
    ema50_1h: Decimal | None = None           # 1h EMA50
    sma200_1h: Decimal | None = None          # 1h SMA200

    # 1H ATR normalizer (computed from sealed 1H true ranges, independent of M1 ATR)
    atr30_1h: Decimal | None = None           # None until atr30_1h_samples >= ATR30_1H_MIN_SAMPLES
    atr30_1h_samples: int = 0                 # true-range samples accumulated

    # 1H norm3 slopes: (ema[t] - ema[t-3]) / (3 * atr30_1h); units: ATR/1H-bar
    # Policy: EMA_1H_SLOPE_POLICY_VERSION = "ema_1h_norm3_v1"; UNCALIBRATED thresholds.
    # None until 4 EMA history values AND atr30_1h is warm.
    ema9_1h_slope_norm_3: float | None = None
    ema9_1h_slope_direction: str | None = None   # "up" / "flat" / "down" (SLOPE_FLAT_THRESHOLD_1H)
    ema21_1h_slope_norm_3: float | None = None
    ema21_1h_slope_direction: str | None = None
    ema50_1h_slope_norm_3: float | None = None
    ema50_1h_slope_direction: str | None = None

    # ── 1H EMA ribbon geometry ────────────────────────────────────────────────
    # Active ribbon = EMA9 + EMA21 + EMA50 only. SMA200 is a separate structural anchor.
    # Policy: EMA_1H_RIBBON_POLICY_VERSION = "ema_1h_ribbon_v1"
    # All ribbon fields are None until all three EMAs and atr30_1h are warm.
    ema_ribbon_low_1h: Decimal | None = None       # min(ema9, ema21, ema50)
    ema_ribbon_high_1h: Decimal | None = None      # max(ema9, ema21, ema50)
    ema_ribbon_center_1h: Decimal | None = None    # mean(ema9, ema21, ema50)
    ema_ribbon_width_1h_atr: float | None = None   # (ribbon_high - ribbon_low) / atr30_1h

    # Signed pairwise EMA distances in ATR units (fast - slow) / atr30_1h
    # Positive = fast above slow (bullish ordering). Negative = fast below slow.
    ema9_21_distance_1h_atr: float | None = None
    ema21_50_distance_1h_atr: float | None = None
    ema9_50_distance_1h_atr: float | None = None

    # ── 1H stack and slope alignment ──────────────────────────────────────────
    # Stack = price ordering of EMA values (structure).
    # Alignment = direction of slopes (momentum). These are independent dimensions.
    ema_stack_direction_1h: str | None = None   # "bullish" (9>21>50) | "bearish" (9<21<50) | "mixed"
    ema_slope_alignment_1h: str | None = None   # "bullish" (all up) | "bearish" (all down) | "flat" | "mixed"

    # ── H1-sealed price location vs ribbon ────────────────────────────────────
    # Computed from the H1 sealed bar's close. Updated only on H1 bar events.
    # These fields exclusively feed rolling history, persistence, and cross counts.
    h1_close_location_vs_ema_ribbon_1h: str | None = None      # "above" | "inside" | "below"
    h1_close_to_ema_ribbon_1h_atr: float | None = None         # 0.0 if inside; signed dist to nearest edge
    h1_close_to_ema_ribbon_center_1h_atr: float | None = None  # (h1_close - ribbon_center) / atr30_1h

    # ── M1-close live location vs carry-forward ribbon ────────────────────────
    # Recomputed on every M1 snapshot from current bar's close vs last sealed H1 ribbon.
    # Must NOT be appended to sealed-H1 rolling history.
    m1_close_location_vs_ema_ribbon_1h: str | None = None
    m1_close_to_ema_ribbon_1h_atr: float | None = None
    m1_close_to_ema_ribbon_center_1h_atr: float | None = None

    # ── SMA200 structural context (separate from ribbon) ──────────────────────
    ema_ribbon_center_to_sma200_1h_atr: float | None = None    # (ribbon_center - sma200) / atr30_1h
    price_to_sma200_1h_atr: float | None = None                # (m1_close - sma200) / atr30_1h

    # ── Rolling ribbon history (sealed H1 bars only) ──────────────────────────
    # Based on h1_close_location and ribbon width at H1 seal time. Point-in-time safe:
    # percentile and slopes exclude the current bar.
    ema_ribbon_width_percentile_60h: float | None = None   # percentile rank in 60-bar rolling window
    ema_ribbon_width_slope_3h: float | None = None         # (width[t] - width[t-3]) / 3
    ema_ribbon_width_slope_6h: float | None = None         # (width[t] - width[t-6]) / 6
    ema_bullish_stack_persistence_1h_bars: int = 0         # consecutive H1 bars with bullish stack
    ema_bearish_stack_persistence_1h_bars: int = 0
    price_inside_ribbon_persistence_1h_bars: int = 0       # consecutive H1 bars with h1_close inside ribbon

    # Cross / transition counts (sealed H1 bars, last 6 sealed bars = indices [t-6..t-1])
    # transition_count: any state change among ABOVE / INSIDE / BELOW
    # full_cross_count: complete ABOVE ↔ BELOW change (may pass through INSIDE)
    price_ribbon_location_transition_count_6h: int = 0
    price_ribbon_full_cross_count_6h: int = 0

    # ── Ribbon width state ────────────────────────────────────────────────────
    # Based on rolling width percentile. Thresholds: RIBBON_COMPRESSED_PERCENTILE / EXPANDED.
    ema_ribbon_width_state_1h: str | None = None   # "compressed" | "normal" | "expanded"

    # ── Derived ribbon contexts (research-only, not wired into live setup logic) ──
    bullish_ema_ribbon_context_1h: bool = False
    bearish_ema_ribbon_context_1h: bool = False
    chop_ema_ribbon_context_1h: bool = False

    # ── H1 watermark ─────────────────────────────────────────────────────────
    # Timestamp of the last sealed H1 bar reflected in this snapshot.
    # The H1 bar whose timestamp equals the current M1 bar may NOT be reflected
    # (event ordering at the hourly boundary is not guaranteed in live mode).
    htf_1h_watermark: datetime | None = None

    # ── Trend indicators — 1D (carry-forward: updated on each D1 bar) ────────
    ema10_1d: Decimal | None = None           # 1d EMA10
    ema20_1d: Decimal | None = None           # 1d EMA20

    # ── Volatility ────────────────────────────────────────────────────────────
    atr_14: Decimal | None = None

    # ── Microstructure (populated when quote feed is active) ─────────────────
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    bid_ask_spread: Decimal | None = None
    bid_ask_spread_pct: float | None = None

    # ── Relative strength ─────────────────────────────────────────────────────
    rs_vs_spy: float | None = None              # % gain relative to SPY for session
    rs_vs_sector: float | None = None

    # ── Computed flags ────────────────────────────────────────────────────────
    is_above_vwap: bool = False
    is_extended: bool = False                   # price far from VWAP (>2 ATR)

    # ── Setup detection features ──────────────────────────────────────────────
    bars_above_vwap: int = 0                    # consecutive bars closing above VWAP
    bars_below_vwap: int = 0                    # consecutive bars closing below VWAP
    vwap_cross_up: bool = False                 # this bar crossed above VWAP
    vwap_cross_up_after_bars: int = 0           # how many consecutive bars were below VWAP before the cross
    vwap_cross_down: bool = False               # this bar crossed below VWAP
    vwap_cross_down_after_bars: int = 0         # how many consecutive bars were above VWAP before the cross
    vwap_deviation_shrinking: bool = False      # distance to VWAP decreased vs prior bar (from above)
    bar_close_position_pct: float | None = None  # (close - low) / (high - low)
    intraday_high: Decimal | None = None        # session high so far
    intraday_low: Decimal | None = None         # session low so far
    is_new_hod: bool = False                    # this bar set a new session high
    is_new_lod: bool = False                    # this bar set a new session low (full Globex session)
    is_new_rth_lod: bool = False                # this bar set a new RTH low (resets at 09:30, ignores overnight)
    is_higher_high: bool = False                # this bar's high > prior bar's high
    is_lower_low: bool = False                  # this bar's low < prior bar's low
    is_lower_high: bool = False                 # this bar's high < prior bar's high
    recent_lower_low: bool = False              # a lower low was made within the last 10 bars (session)
    or_mid: Decimal | None = None               # (or_high + or_low) / 2
    swept_below_vwap: bool = False              # low < vwap but close >= vwap
    swept_or_low: bool = False                  # low < or_low

    # ── OR cross flags (populated by Feature Engine) ──────────────────────────
    or_cross_down: bool = False               # first bar that closes below or_low (one-time trigger)
    or_cross_up: bool = False                 # first bar that closes back above or_low after breakdown
    bars_since_or_breakdown: int = 0          # bars elapsed since initial or_cross_down (0 = not yet broken)

    # ── Additional volatility ─────────────────────────────────────────────────
    vwap_distance_atr: float | None = None   # (close - vwap) / atr_30; None until atr_30 warms
    atr_30: Decimal | None = None           # M1 30-period ATR

    # ── EMA slope acceleration (norm3_v1; UNCALIBRATED) ──────────────────────
    # slope_accel = slope_norm_3[t] - slope_norm_3[t-1]; units: ATR/bar²
    # None until two consecutive non-None slopes are available.
    ema9_1m_slope_accel_norm: float | None = None
    ema21_1m_slope_accel_norm: float | None = None
    ema9_5m_slope_accel_norm: float | None = None   # carry-forward from last sealed 5m bar
    ema21_5m_slope_accel_norm: float | None = None

    # ── EMA multi-timeframe alignment ─────────────────────────────────────────
    # Raw sign primitive (< 0 / > 0), NOT the calibrated direction from classify_slope.
    # A tiny negative slope triggers bearish_alignment — does NOT imply meaningful
    # directional conviction. Use ema_mtf_strength + ema_slope_dispersion for that.
    # None-safe: flags are False when any required slope is None.
    ema_bearish_alignment_1m: bool = False   # ema9_1m_slope < 0 and ema21_1m_slope < 0
    ema_bearish_alignment_5m: bool = False   # ema9_5m_slope < 0 and ema21_5m_slope < 0
    ema_bullish_alignment_1m: bool = False
    ema_bullish_alignment_5m: bool = False
    ema_mtf_all_bearish: bool = False        # all 4 norm3 slopes negative
    ema_mtf_all_bullish: bool = False        # all 4 norm3 slopes positive

    # ── EMA MTF directional strength ──────────────────────────────────────────
    # Signed mean of all 4 norm3 slopes when fully aligned (ema_mtf_all_bearish/bullish).
    # Negative = bearish alignment, positive = bullish alignment.
    # None when not fully aligned or any slope is None.
    ema_mtf_strength: float | None = None

    # ── EMA slope dispersion ──────────────────────────────────────────────────
    # Population std dev of the 4 norm3 slopes; None if any is None.
    # Low = coherent directional move; high = mixed/divergent timeframe signals.
    ema_slope_dispersion: float | None = None

    # ── EMA MTF persistence ───────────────────────────────────────────────────
    # Consecutive bars where ema_mtf_all_bearish / ema_mtf_all_bullish was True.
    ema_bearish_persistence_bars: int = 0
    ema_bullish_persistence_bars: int = 0

    # ── VWAP point-in-time geometry ───────────────────────────────────────────
    # Instantaneous bar measurements — no episode state machine here.
    # Approach/test/cross episode semantics belong in LevelInteractionEngine.
    vwap_relative_side: str | None = None               # "above" | "below" | None (vwap not yet warm)
    bar_high_distance_to_vwap_atr: float | None = None  # (high - vwap) / atr_30; positive = bar top above VWAP
    bar_low_distance_to_vwap_atr: float | None = None   # (low - vwap) / atr_30; negative = bar bottom below VWAP
    vwap_approach_velocity_atr: float | None = None     # vwap_dist_atr[t] - vwap_dist_atr[t-1]; ATR/bar; None first bar

    # ── Candle geometry ───────────────────────────────────────────────────────
    bar_body_pct: float | None = None               # |close - open| / (high - low); 0 = doji, 1 = no wicks
    upper_wick_pct: float | None = None             # (high - max(open,close)) / (high - low)
    lower_wick_pct: float | None = None             # (min(open,close) - low) / (high - low)
    bar_range_atr: float | None = None              # (high - low) / atr_30; ATR multiples
    is_inside_bar: bool = False                     # high <= prev_high and low >= prev_low
    is_outside_bar: bool = False                    # high > prev_high and low < prev_low (engulfing range)

    # ── RTH candle-range distribution (so far today, RTH bars only) ───────────
    rth_median_1m_range: Decimal | None = None  # median 1m bar range this RTH session
    rth_p75_1m_range: Decimal | None = None     # 75th pct — "large but normal" candle
    rth_p90_1m_range: Decimal | None = None     # 90th pct — "abnormally large" candle

    # ── Intrabar flow context (populated by BarFlowAggregator) ───────────────
    # None when running without full-signals data (historical bars, no trades/quotes cache).
    flow: BarFlowContext | None = None
