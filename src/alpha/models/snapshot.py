from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, ORBState, SessionPhase


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

    # ── Trend indicators ─────────────────────────────────────────────────────
    ema_9: Decimal | None = None
    ema_20: Decimal | None = None
    ema_50: Decimal | None = None

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
    is_new_lod: bool = False                    # this bar set a new session low
    is_higher_high: bool = False                # this bar's high > prior bar's high
    is_lower_low: bool = False                  # this bar's low < prior bar's low
    or_mid: Decimal | None = None               # (orb_high + orb_low) / 2
    swept_below_vwap: bool = False              # low < vwap but close >= vwap
    swept_orl: bool = False                     # low < orb_low
