"""
Canonical geometry computation for level-bar relationships.

VWAP geometry note: all distances computed against end-of-bar VWAP
(cumulative VWAP updated through bar close). VWAP moved during the bar;
geometry does not represent intrabar relationship to a fixed price.
Sampling convention encoded in GEOMETRY_VERSION.

Side classification uses the computed integer tick distance:
  distance == 0 → ON
  distance >  0 → ABOVE
  distance <  0 → BELOW
This guarantees ABOVE/BELOW/ON is always consistent with the distance value.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

GEOMETRY_VERSION = "lbo_geometry_v1"


@dataclass(frozen=True)
class BarLevelGeometry:
    open_distance_ticks: int
    high_distance_ticks: int
    low_distance_ticks: int
    close_distance_ticks: int

    open_side: str    # "above" | "below" | "on"
    close_side: str
    range_spans_level: bool    # low_distance_ticks <= 0 <= high_distance_ticks
    crossed_within_bar: bool   # range_spans_level AND close != open side (order unknown)

    proximity_band_ticks: int  # for reference, same as existing level_observer band
    geometry_version: str = GEOMETRY_VERSION


def _tick_distance(price: Decimal, level: Decimal, tick_size: Decimal, *, vwap: bool) -> int:
    """
    Compute signed tick distance: (price - level) / tick_size.
    VWAP: ROUND_HALF_UP (level not tick-aligned).
    ORH/ORL: exact integer division (prices are tick-aligned).
    """
    raw = (price - level) / tick_size
    if vwap:
        return int(raw.to_integral_value(rounding=ROUND_HALF_UP))
    else:
        result = int(raw)
        return result


def _side(distance_ticks: int) -> str:
    if distance_ticks > 0:
        return "above"
    if distance_ticks < 0:
        return "below"
    return "on"


def _proximity_band(atr_14: Decimal | None, tick_size: Decimal) -> int:
    """20% of ATR-14 in ticks. Fallback 10 ticks."""
    _FACTOR = Decimal("0.20")
    _FALLBACK = 10
    if atr_14 is None or atr_14 == 0:
        return _FALLBACK
    raw = (atr_14 * _FACTOR / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
    return max(1, int(raw))


def compute_geometry(
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
    level_value: Decimal,
    tick_size: Decimal,
    atr_14: Decimal | None,
    *,
    is_vwap: bool,
) -> BarLevelGeometry:
    """
    Compute the geometric relationship between one M1 bar and one level.
    This is the single source of truth for all distance and side calculations.
    """
    open_d  = _tick_distance(open_price,  level_value, tick_size, vwap=is_vwap)
    high_d  = _tick_distance(high_price,  level_value, tick_size, vwap=is_vwap)
    low_d   = _tick_distance(low_price,   level_value, tick_size, vwap=is_vwap)
    close_d = _tick_distance(close_price, level_value, tick_size, vwap=is_vwap)

    open_side  = _side(open_d)
    close_side = _side(close_d)

    range_spans = low_d <= 0 <= high_d
    crossed     = range_spans and open_side != "on" and close_side != "on"

    prox_band = _proximity_band(atr_14, tick_size)

    return BarLevelGeometry(
        open_distance_ticks=open_d,
        high_distance_ticks=high_d,
        low_distance_ticks=low_d,
        close_distance_ticks=close_d,
        open_side=open_side,
        close_side=close_side,
        range_spans_level=range_spans,
        crossed_within_bar=crossed,
        proximity_band_ticks=prox_band,
    )
