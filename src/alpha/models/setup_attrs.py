"""
Setup attribute mapping — canonical source of truth for setup taxonomy.

Maps each SetupType to its structural attributes:
  family       — the tradeable structure (independent of anchor/direction)
  side         — BUY (long) or SELL (short)
  anchor       — the reference level the setup is organized around
  interaction  — how price interacted with the anchor (the catalyst)
  display_name — human-facing label shown in the UI

This replaces the ad-hoc _side_for_setup_type / _level_tag_for_setup_type
methods that were scattered across SetupEngine.

Adding a new setup type: add one entry here. Everything downstream
(scoring, storage, display, analytics) reads from this mapping.
"""

from __future__ import annotations

from dataclasses import dataclass

from alpha.models.enums import (
    AnchorLevel,
    LevelInteraction,
    OrderSide,
    SetupFamily,
    SetupType,
)


@dataclass(frozen=True)
class SetupAttrs:
    family:       SetupFamily
    side:         OrderSide
    anchor:       AnchorLevel
    interaction:  LevelInteraction
    display_name: str


# ── Canonical mapping ─────────────────────────────────────────────────────────
#
# Note on duplicates within the same family/anchor/interaction:
#   SWEEP_RECLAIM and FAKE_BREAKDOWN share the same attributes. The difference
#   is quality (FAKE_BREAKDOWN is SSS-candidate because it requires OR sweep +
#   VWAP reclaim simultaneously). The structural_grade on the Setup model
#   captures that distinction; the taxonomy attributes are identical.
#
#   VWAP_RECLAIM and VWAP_UNDERCUT_RECLAIM differ in depth: UNDERCUT has a
#   genuine wick below (SWEEP), RECLAIM just crosses back (RECLAIM). Different
#   interaction primitives, same family.

SETUP_TYPE_ATTRS: dict[SetupType, SetupAttrs] = {
    # ── Long / Bullish ────────────────────────────────────────────────────────
    SetupType.VWAP_RECLAIM: SetupAttrs(
        family=SetupFamily.PULLBACK_CONTINUATION,
        side=OrderSide.BUY,
        anchor=AnchorLevel.VWAP,
        interaction=LevelInteraction.RECLAIM,
        display_name="VWAP Reclaim Long",
    ),
    SetupType.VWAP_UNDERCUT_RECLAIM: SetupAttrs(
        family=SetupFamily.PULLBACK_CONTINUATION,
        side=OrderSide.BUY,
        anchor=AnchorLevel.VWAP,
        interaction=LevelInteraction.SWEEP,
        display_name="VWAP Undercut Reclaim Long",
    ),
    SetupType.SWEEP_RECLAIM: SetupAttrs(
        family=SetupFamily.FAILED_AUCTION_REVERSAL,
        side=OrderSide.BUY,
        anchor=AnchorLevel.ORL,
        interaction=LevelInteraction.SWEEP,
        display_name="OR Low Sweep Reclaim Long",
    ),
    SetupType.FAKE_BREAKDOWN: SetupAttrs(
        family=SetupFamily.FAILED_AUCTION_REVERSAL,
        side=OrderSide.BUY,
        anchor=AnchorLevel.ORL,
        interaction=LevelInteraction.SWEEP,
        display_name="Fake Breakdown Long",
    ),
    SetupType.DEEP_EXHAUSTION_RECLAIM: SetupAttrs(
        family=SetupFamily.EXHAUSTION_REVERSAL,
        side=OrderSide.BUY,
        anchor=AnchorLevel.VWAP,
        interaction=LevelInteraction.SWEEP,
        display_name="Deep Exhaustion Reclaim Long",
    ),
    SetupType.HOD_BREAKOUT: SetupAttrs(
        family=SetupFamily.BREAKOUT_CONTINUATION,
        side=OrderSide.BUY,
        anchor=AnchorLevel.HOD,
        interaction=LevelInteraction.ACCEPT,
        display_name="HOD Breakout Long",
    ),
    SetupType.TREND_PULLBACK: SetupAttrs(
        family=SetupFamily.PULLBACK_CONTINUATION,
        side=OrderSide.BUY,
        anchor=AnchorLevel.VWAP,
        interaction=LevelInteraction.HOLD,
        display_name="Trend Pullback Long",
    ),
    SetupType.ORB_BREAKOUT: SetupAttrs(
        family=SetupFamily.BREAKOUT_CONTINUATION,
        side=OrderSide.BUY,
        anchor=AnchorLevel.ORH,
        interaction=LevelInteraction.ACCEPT,
        display_name="ORB Breakout Long",
    ),
    SetupType.RELATIVE_STRENGTH_BREAKOUT: SetupAttrs(
        family=SetupFamily.BREAKOUT_CONTINUATION,
        side=OrderSide.BUY,
        anchor=AnchorLevel.HOD,
        interaction=LevelInteraction.ACCEPT,
        display_name="Relative Strength Breakout Long",
    ),
    SetupType.ONL_SWEEP_RECLAIM_LONG: SetupAttrs(
        family=SetupFamily.FAILED_AUCTION_REVERSAL,
        side=OrderSide.BUY,
        anchor=AnchorLevel.ONL,
        interaction=LevelInteraction.SWEEP,
        display_name="ONL Sweep Reclaim Long",
    ),
    SetupType.DOUBLE_BOTTOM_RECLAIM_LONG: SetupAttrs(
        family=SetupFamily.RANGE_REVERSAL,
        side=OrderSide.BUY,
        anchor=AnchorLevel.STRUCTURAL,
        interaction=LevelInteraction.HOLD,
        display_name="Double Bottom Reclaim Long",
    ),
    # ── Short / Bearish ───────────────────────────────────────────────────────
    SetupType.VWAP_REJECTION: SetupAttrs(
        family=SetupFamily.FAILED_AUCTION_REVERSAL,
        side=OrderSide.SELL,
        anchor=AnchorLevel.VWAP,
        interaction=LevelInteraction.REJECT,
        display_name="VWAP Rejection Short",
    ),
    SetupType.VWAP_FAILED_RECLAIM_SHORT: SetupAttrs(
        family=SetupFamily.FAILED_RECLAIM,
        side=OrderSide.SELL,
        anchor=AnchorLevel.VWAP,
        interaction=LevelInteraction.FAILED_RECLAIM,
        display_name="VWAP Failed Reclaim Short",
    ),
    SetupType.TREND_PULLBACK_SHORT: SetupAttrs(
        family=SetupFamily.PULLBACK_CONTINUATION,
        side=OrderSide.SELL,
        anchor=AnchorLevel.EMA9,
        interaction=LevelInteraction.REJECT,
        display_name="Trend Pullback Short",
    ),
    SetupType.ORB_BREAKDOWN: SetupAttrs(
        family=SetupFamily.BREAKOUT_CONTINUATION,
        side=OrderSide.SELL,
        anchor=AnchorLevel.ORL,
        interaction=LevelInteraction.ACCEPT,
        display_name="ORB Breakdown Short",
    ),
}


def attrs_for(setup_type: SetupType) -> SetupAttrs | None:
    """Return structured attributes for a SetupType, or None if not mapped."""
    return SETUP_TYPE_ATTRS.get(setup_type)
