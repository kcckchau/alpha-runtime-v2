"""
Bar-level flow context produced by BarFlowAggregator.

One BarFlowContext is sealed per 1-minute bar close and contains:
  - Aggregated trade-tape metrics (delta, large trades, velocity)
  - Bid/ask imbalance time-weighted from mbp-1 quotes
  - Split-bar delta (sweep phase vs recovery phase, derived from 1s bars)
  - Absorption signal (large sell volume at low with price hold)
  - V-reversal classification flag

BarBundle pairs a BarEvent with its sealed BarFlowContext so the sequential
pipeline receives both in a single unit.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class SplitBarDelta(BaseModel):
    """
    Intrabar delta split at the 1-minute bar's extreme.

    For a bar that swept a low:
      sweep_phase:   open → bar low   (negative delta expected — sell pressure)
      recovery_phase: bar low → close  (positive delta expected — buyers absorbing)

    Populated only when the bar made a new intraday or structural low; None otherwise.
    """

    model_config = {"frozen": True}

    # Timestamps of the 1s bar where the low was hit
    sweep_low_ts: datetime | None = None

    # Trade volume and signed delta during each phase
    sweep_volume: int = 0           # total contracts during sweep phase (open → low)
    sweep_delta: int = 0            # buy_vol - sell_vol during sweep (usually negative)
    recovery_volume: int = 0        # total contracts during recovery phase (low → close)
    recovery_delta: int = 0         # buy_vol - sell_vol during recovery (usually positive)

    # Duration of each phase in seconds (from 1s bar counts)
    sweep_seconds: int = 0
    recovery_seconds: int = 0

    # Derived ratios (None when sweep_volume == 0)
    recovery_ratio: float | None = None   # abs(recovery_delta) / abs(sweep_delta) — >0.5 = buyers won
    recovery_speed: float | None = None   # recovery_delta / recovery_seconds (delta/sec)


class AbsorptionSignal(BaseModel):
    """
    Detected when large sell volume prints at or near the bar low but price does not extend.

    Absorption means passive buyers absorbed aggressive selling — a precondition for a
    genuine reversal rather than a V-snap-back.
    """

    model_config = {"frozen": True}

    detected: bool = False
    price_level: Decimal | None = None        # price where absorption occurred
    sell_volume_at_low: int = 0               # sell-side volume within 1 tick of bar low
    buy_volume_response: int = 0              # buy-side volume in next N seconds after sell cluster
    ticks_held: int = 0                       # how many 1s bars price held at/above that level
    confidence: float = 0.0                   # 0.0–1.0; based on volume ratio + hold duration


class BarFlowContext(BaseModel):
    """
    Full intrabar flow context for a sealed 1-minute bar.
    Produced by BarFlowAggregator once the bar closes.
    """

    model_config = {"frozen": True}

    # ── Trade tape summary ────────────────────────────────────────────────────
    total_volume: int = 0
    buy_volume: int = 0
    sell_volume: int = 0
    unknown_volume: int = 0
    delta: int = 0                         # buy_vol - sell_vol (signed)
    delta_pct: float | None = None         # delta / total_volume * 100
    large_trade_count: int = 0             # trades >= large_trade_threshold contracts
    large_buy_count: int = 0
    large_sell_count: int = 0
    trade_count: int = 0
    trade_velocity: float | None = None    # trades per second across the bar window
    avg_trade_size: float | None = None    # total_volume / trade_count

    # ── Quote / bid-ask imbalance ─────────────────────────────────────────────
    # Time-weighted average over all quote updates in the 1m window
    twap_bid_size: float | None = None     # time-weighted avg bid size (level 1)
    twap_ask_size: float | None = None     # time-weighted avg ask size (level 1)
    bid_ask_imbalance: float | None = None # twap_bid / (twap_bid + twap_ask); >0.6 = bid-heavy
    quote_count: int = 0                   # number of mbp-1 updates in window

    # ── Split-bar delta (populated when bar swept a structural low) ───────────
    split: SplitBarDelta | None = None

    # ── Absorption ────────────────────────────────────────────────────────────
    absorption: AbsorptionSignal = Field(default_factory=AbsorptionSignal)

    # ── Classification flags ──────────────────────────────────────────────────
    is_v_reversal: bool = False
    """
    True when: recovery_ratio < 0.3 AND recovery_seconds < 5 AND NOT absorption.detected.
    This bar closed green (or near-green) but the reversal was a snap-back with no real buyers —
    SetupEngine should reduce confidence or skip.
    """

    is_genuine_sweep_reversal: bool = False
    """
    True when: sweep_volume > avg_bar_volume * 1.2 AND absorption.detected
               AND recovery_ratio > 0.5 AND large_buy_count >= 2.
    Strong evidence of institutional accumulation at the sweep low.
    """

    # ── Metadata ──────────────────────────────────────────────────────────────
    large_trade_threshold: int = 10        # contracts; configurable per symbol
    has_trade_data: bool = False           # False when trades were not available (replay gap)
    has_quote_data: bool = False           # False when quotes were not available
    has_1s_bars: bool = False              # False when 1s bar resolution not available
