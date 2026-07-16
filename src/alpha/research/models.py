"""
Phase 0 schema: LevelBarObservation.

One row per (M1 bar × reference level). Raw structural geometry only —
no episode segmentation, no sweep/acceptance/rejection labels, no derived
interaction records. Those are computed offline from this dataset.

Design rules:
  - PriceSide.ON = distance_ticks == 0 exactly (not "within N ticks")
  - obs_id is deterministic: uuid5(NAMESPACE, run_id:symbol:bar_ts:level_type)
    so that crash recovery and duplicate bars produce identical rows
  - VWAP distances use ROUND_HALF_UP (VWAP is not tick-aligned)
  - ORH/ORL distances use exact integer arithmetic (prices are tick-aligned)
  - EMA, delta, flow fields are NOT here — join via bar_timestamp + symbol
    from the existing BarSnapshot / BarFlowContext datasets
"""

from __future__ import annotations

from decimal import Decimal
from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid5, NAMESPACE_DNS

from pydantic import BaseModel

# Stable namespace for all LevelBarObservation IDs.
# Changing this value invalidates all existing obs_ids — bump schema_version too.
_LBO_NAMESPACE = uuid5(NAMESPACE_DNS, "alpha.research.level_bar_observation.v1")


class ReferenceLevelType(StrEnum):
    VWAP     = "vwap"        # full-session VWAP (snap.vwap) — resets at 18:00 ET
    RTH_VWAP = "rth_vwap"    # RTH-only VWAP (snap.rth_vwap) — resets at 09:30 ET, RTH phases only
    ORH      = "orh"
    ORL      = "orl"
    # ONH, ONL, PDH, PDL — Phase 1. Not implemented here.


class PriceSide(StrEnum):
    ABOVE = "above"
    BELOW = "below"
    ON    = "on"    # distance_ticks == 0 exactly


class RunMode(StrEnum):
    LIVE     = "live"
    REPLAY   = "replay"
    BACKFILL = "backfill"


def make_obs_id(run_id: str, symbol: str, bar_timestamp: datetime, level_type: ReferenceLevelType) -> UUID:
    """Deterministic UUID5 for a LevelBarObservation.

    Same inputs always produce the same ID regardless of when or where the
    observation is generated — enables natural dedup and crash-safe replay.
    """
    key = f"{run_id}:{symbol}:{bar_timestamp.isoformat()}:{level_type}"
    return uuid5(_LBO_NAMESPACE, key)


class LevelBarObservation(BaseModel):
    """
    Structural geometry of one M1 bar relative to one reference level.

    Dedup key: (run_id, symbol, bar_timestamp, level_type, schema_version).
    obs_id is deterministic — same key → same obs_id.

    What is deliberately absent:
      EMA9, EMA21, delta, TPS — join from BarSnapshot/BarFlowContext by
      (symbol, bar_timestamp). Keeping them separate preserves schema stability
      and allows context enrichment to be versioned independently.
    """

    # ── Identity ──────────────────────────────────────────────────────────────
    obs_id:        UUID          # deterministic uuid5 — see make_obs_id()
    run_id:        str           # unique execution instance: "{mode}-{date}-{hex8}"
    run_mode:      RunMode
    source_bar_id: str | None    # str(BarBundleEvent.metadata.event_id), None if unavailable

    # ── Coordinates ───────────────────────────────────────────────────────────
    symbol:        str
    session_date:  str           # "YYYY-MM-DD" in ET, from calendar.session_date()
    bar_timestamp: datetime      # bar open time, UTC — matches BarBundleEvent.timestamp

    # ── Level ─────────────────────────────────────────────────────────────────
    level_type:    ReferenceLevelType
    level_value:   Decimal       # exact value at observation time

    # ── Bar geometry relative to level ────────────────────────────────────────
    # Signed: positive = price is above level, negative = price is below level.
    # VWAP rows: ROUND_HALF_UP applied (VWAP is not tick-aligned).
    # ORH/ORL rows: exact integer (prices are tick-aligned).
    open_distance_ticks:  int    # (bar.open  - level) / tick_size
    high_distance_ticks:  int    # (bar.high  - level) / tick_size
    low_distance_ticks:   int    # (bar.low   - level) / tick_size
    close_distance_ticks: int    # (bar.close - level) / tick_size

    # ── Structural flags ──────────────────────────────────────────────────────
    # Derived deterministically from geometry — NOT interpretive labels.
    bar_overlaps_level: bool     # low_distance_ticks <= 0 <= high_distance_ticks
    open_side:          PriceSide
    close_side:         PriceSide

    # ── Volatility context ────────────────────────────────────────────────────
    proximity_band_ticks:         int      # ATR-derived band; value: max(1, round(atr * factor / tick))
    volatility_reference:         Decimal  # ATR value used (atr_14 from BarSnapshot)
    volatility_definition_version: str     # "atr14_m1_v1" — bump if formula changes

    tick_size: Decimal

    # ── Session context ───────────────────────────────────────────────────────
    session_phase: str           # SessionPhase value at this bar
    orb_state:     str           # ORBState value — captures whether ORH/ORL are formed

    # ── Versioning ────────────────────────────────────────────────────────────
    schema_version:   str = "lbo_v1"
    producer_version: str        # git commit SHA injected at startup, or "dev"

    # ── Data quality ──────────────────────────────────────────────────────────
    data_quality: str            # DataQualityState value from IngestionMonitor
