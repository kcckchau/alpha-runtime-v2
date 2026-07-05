"""
ContextSnapshot — session-level key levels and distances.

The single "where are we?" reference for all downstream engines.
Updated on every M1 bar by ContextEngine.

Distances are signed: positive = current price is ABOVE the level.
War zone = the named key level closest to current price.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class ContextSnapshot(BaseModel):
    """Session-level key levels and distances for a single bar."""

    symbol: str
    timestamp: datetime

    # ── Overnight (Globex): 18:00 ET prior day → 09:30 ET current day ─────────
    onh: Decimal | None = None    # overnight high
    onl: Decimal | None = None    # overnight low

    # ── Previous RTH ──────────────────────────────────────────────────────────
    pdh: Decimal | None = None              # previous RTH high
    pdl: Decimal | None = None              # previous RTH low
    prev_rth_close: Decimal | None = None   # previous RTH closing price

    # ── Current RTH ───────────────────────────────────────────────────────────
    rth_open: Decimal | None = None         # first RTH bar's open price

    # ── Gap (RTH open vs prev RTH close) ──────────────────────────────────────
    gap_points: float | None = None         # rth_open - prev_rth_close  (neg = gap down)
    gap_pct: float | None = None            # gap_points / prev_rth_close * 100
    gap_midpoint: Decimal | None = None     # (rth_open + prev_rth_close) / 2

    # ── Opening Range (pulled from BarSnapshot for convenience) ───────────────
    orb_high: Decimal | None = None
    orb_low: Decimal | None = None
    or_mid: Decimal | None = None

    # ── Nearest war zone ──────────────────────────────────────────────────────
    nearest_war_zone: str | None = None            # "ONL" / "PDL" / "VWAP" / …
    nearest_war_zone_price: Decimal | None = None
    nearest_war_zone_dist: float | None = None     # absolute distance in points

    # ── Signed distances from current close ───────────────────────────────────
    # Positive = price is above the level (room below); negative = price is below
    dist_to_onl: float | None = None
    dist_to_onh: float | None = None
    dist_to_pdl: float | None = None
    dist_to_pdh: float | None = None
    dist_to_vwap: float | None = None
    dist_to_5m21: float | None = None
    dist_to_orb_high: float | None = None
    dist_to_orb_low: float | None = None
    dist_to_prev_close: float | None = None
