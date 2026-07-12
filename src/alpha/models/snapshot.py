from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, ORBState, SessionPhase
from alpha.models.flow import BarFlowContext


class BarSnapshot(BaseModel):
    """Output contract of the Feature Engine for a single bar across all features."""

    symbol: str
    timestamp: datetime
    timeframe: BarTimeframe
    bar: Bar

    # ── VWAP ────────────────────────────────────────────────────────────────
    vwap: Decimal
    vwap_upper_band: Decimal | None = None      # +1 std dev
    vwap_lower_band: Decimal | None = None      # -1 std dev
    vwap_deviation_pct: float = 0.0             # (close - vwap) / vwap * 100

    # ── Volume ───────────────────────────────────────────────────────────────
    cumulative_volume: int = 0
    relative_volume: float | None = None        # vs historical avg at same time-of-day

    # ── Opening Range (ORB) ───────────────────────────────────────────────────
    orb_high: Decimal | None = None
    orb_low: Decimal | None = None
    orb_range: Decimal | None = None
    orb_state: ORBState = ORBState.NOT_SET

    # ── Session context ──────────────────────────────────────────────────────
    session_phase: SessionPhase = SessionPhase.CLOSED
    bars_since_open: int = 0

    # ── Trend indicators — 1m ────────────────────────────────────────────────
    ema_9: Decimal | None = None
    ema_20: Decimal | None = None
    ema_21: Decimal | None = None             # EMA21 (user's primary 1m fast EMA)
    ema_50: Decimal | None = None
    ema_9_slope: float | None = None          # % rate-of-change of EMA9 vs prior bar (positive = rising)
    ema_9_slope_direction: str | None = None  # "up" / "flat" / "down"  (flat = |slope| ≤ 0.005%)
    ema_20_slope: float | None = None         # % rate-of-change of EMA20 vs prior bar
    ema_20_slope_direction: str | None = None  # "up" / "flat" / "down"
    vwap_slope: float | None = None           # % rate-of-change of VWAP vs prior bar
    vwap_slope_direction: str | None = None   # "up" / "flat" / "down"  (flat = |slope| ≤ 0.002%)

    # ── Trend indicators — 5m (carry-forward: updated on each M5 bar) ────────
    ema9_5m: Decimal | None = None            # 5m EMA9
    ema21_5m: Decimal | None = None           # 5m EMA21
    ema9_5m_slope: float | None = None        # % rate-of-change of 5m EMA9 vs prior 5m bar
    ema9_5m_slope_direction: str | None = None
    ema21_5m_slope: float | None = None       # % rate-of-change of 5m EMA21 vs prior 5m bar
    ema21_5m_slope_direction: str | None = None
    is_bull_stack_5m: bool | None = None      # EMA9 > EMA21
    is_bear_stack_5m: bool | None = None      # EMA9 < EMA21

    # ── Trend indicators — 1h (carry-forward: updated on each H1 bar) ────────
    # SMA200 requires 200 H1 bars (~8.5 trading days); will be None until warm.
    ema9_1h: Decimal | None = None            # 1h EMA9
    ema21_1h: Decimal | None = None           # 1h EMA21
    ema50_1h: Decimal | None = None           # 1h EMA50
    sma200_1h: Decimal | None = None          # 1h SMA200
    ema9_1h_slope: float | None = None        # % rate-of-change of 1h EMA9 vs prior 1h bar
    ema9_1h_slope_direction: str | None = None
    ema21_1h_slope: float | None = None       # % rate-of-change of 1h EMA21 vs prior 1h bar
    ema21_1h_slope_direction: str | None = None
    ema50_1h_slope: float | None = None       # % rate-of-change of 1h EMA50 vs prior 1h bar
    ema50_1h_slope_direction: str | None = None
    is_bull_stack_1h: bool | None = None      # EMA9 > EMA21 > EMA50 (and > SMA200 when warmed)
    is_bear_stack_1h: bool | None = None      # EMA9 < EMA21 < EMA50 (and < SMA200 when warmed)

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
    is_above_ema20: bool | None = None
    is_extended: bool = False                   # price far from VWAP (>2 std dev)

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
    or_mid: Decimal | None = None               # (orb_high + orb_low) / 2
    swept_below_vwap: bool = False              # low < vwap but close >= vwap
    swept_orl: bool = False                     # low < orb_low

    # ── ORB cross flags (populated by Feature Engine) ─────────────────────────
    orb_cross_down: bool = False               # first bar that closes below orb_low (one-time trigger)
    orb_cross_up: bool = False                 # first bar that closes back above orb_low after breakdown
    bars_since_orb_breakdown: int = 0          # bars elapsed since initial orb_cross_down (0 = not yet broken)

    # ── VWAP interaction memory ───────────────────────────────────────────────
    vwap_touch_count: int = 0              # VWAP cross/sweep events this session
    last_vwap_outcome: str | None = None   # "swept","reclaimed","broken","rejected"
    bars_since_last_vwap_touch: int = 0

    # ── Additional volatility ─────────────────────────────────────────────────
    atr_30: Decimal | None = None           # M1 30-period ATR
    ema_9_slope_accel: float | None = None  # slope acceleration (d²EMA9/dt²)

    # ── RTH candle-range distribution (so far today, RTH bars only) ───────────
    rth_median_1m_range: Decimal | None = None  # median 1m bar range this RTH session
    rth_p75_1m_range: Decimal | None = None     # 75th pct — "large but normal" candle
    rth_p90_1m_range: Decimal | None = None     # 90th pct — "abnormally large" candle

    # ── Intrabar flow context (populated by BarFlowAggregator) ───────────────
    # None when running without full-signals data (historical bars, no trades/quotes cache).
    flow: BarFlowContext | None = None
