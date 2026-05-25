from datetime import datetime

from pydantic import BaseModel, Field

from alpha.models.enums import ORBState, SessionPhase, TrendState, VWAPState


class MarketState(BaseModel):
    """Output contract of the Market State Engine."""

    symbol: str
    timestamp: datetime

    # ── Trend ─────────────────────────────────────────────────────────────────
    trend: TrendState = TrendState.UNKNOWN
    trend_strength: float = 0.0           # 0.0–1.0
    trend_bars: int = 0                   # consecutive bars confirming trend

    # ── VWAP regime ──────────────────────────────────────────────────────────
    vwap_state: VWAPState = VWAPState.BELOW
    vwap_tests: int = 0                   # # of times price tested VWAP today
    vwap_holds: int = 0                   # # of times price held at VWAP

    # ── ORB regime ────────────────────────────────────────────────────────────
    orb_state: ORBState = ORBState.NOT_SET
    orb_breakout_clean: bool = False      # single-bar clean break
    orb_volume_confirmed: bool = False

    # ── Session ──────────────────────────────────────────────────────────────
    session_phase: SessionPhase = SessionPhase.CLOSED
    is_low_float: bool = False
    is_extended: bool = False             # price extended from VWAP

    # ── Lead-lag ─────────────────────────────────────────────────────────────
    lead_symbol: str | None = None        # e.g. "SPY" if this symbol lags SPY
    lead_lag_correlation: float | None = None

    # ── Overall quality score ─────────────────────────────────────────────────
    structure_score: float = 0.0          # 0.0–1.0; how clean is price structure?
    confidence: float = 0.0              # 0.0–1.0; how certain is this classification?

    # ── Context flags ─────────────────────────────────────────────────────────
    is_news_driven: bool = False
    is_high_volume_day: bool = False
    flags: list[str] = Field(default_factory=list)
