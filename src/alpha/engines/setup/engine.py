"""
Engine 6 — Setup Engine

Responsibilities:
  - Detect setup candidates from BarSnapshot + MarketState
  - Manage setup lifecycle state machine per symbol
  - Emit SetupEvent on every state transition

Setup lifecycle:
  FORMING → CONFIRMED → TRIGGERED → (terminal)
      ↓           ↓
   FAILED     INVALIDATED
              EXPIRED

Each setup type has its own detector (FORMING condition) and advance method
(CONFIRMED / INVALIDATED / TRIGGERED / FAILED transitions).

Input:  BarEvent (trigger), BarSnapshot + MarketState (from feature/market_state engines)
Output: SetupEvent
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import ClassVar
from uuid import UUID

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import (
    AssetClass,
    BarTimeframe,
    EventType,
    HealthStatus,
    ORBState,
    OrderSide,
    SessionPhase,
    SetupGrade,
    SetupState,
    SetupType,
)
from alpha.models.events import AnyEvent, BarBundleEvent, BarEvent, SetupEvent
from alpha.models.market_state import MarketState
from alpha.models.setup import SessionSetupContext, Setup, SetupHistoryEntry
from alpha.models.setup_attrs import attrs_for
from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)

# Minimum bars between closing a setup and reopening the same type on the same symbol.
# On 1-minute bars this equals a 10-minute cooldown.  Prevents rapid-fire churn on
# strong trend days where the same ORB/VWAP setup would otherwise reopen every bar.
_COOLDOWN_BARS = 10

_RTH_PHASES = frozenset({
    SessionPhase.OPENING_RANGE,
    SessionPhase.EARLY,
    SessionPhase.MID,
    SessionPhase.POWER_HOUR,
    SessionPhase.CLOSING,
})

# SSS grade is assigned at detection time only for setups whose structural
# definition alone justifies the highest grade.  All other types receive
# grade=None at detection and earn their grade from the ScoringEngine rubric.
#
# FAKE_BREAKDOWN is the sole SSS-at-detection type because it requires both
# an OR-low structural sweep AND a confirmed VWAP reclaim — two independent
# confluence signals that no other setup demands simultaneously.
_SSS_TYPES = frozenset({
    SetupType.FAKE_BREAKDOWN,
})


class SetupEngine(BaseEngine):
    """
    Detects and tracks trade setup lifecycles.

    Depends on FeatureEngine and MarketStateEngine being initialized.
    Set those via dependency injection before start().
    """

    def __init__(
        self,
        settings: AlphaSettings,
        event_bus: EventBus,
        registry: SymbolRegistry,
    ) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._registry = registry
        self._feature_engine: object | None = None
        self._market_state_engine: object | None = None
        self._context_engine: object | None = None
        # Active setups: symbol → {setup_id → Setup}
        self._active: dict[str, dict[UUID, Setup]] = {}
        self._session_contexts: dict[str, SessionSetupContext] = {}
        self._prev_session_contexts: dict[str, SessionSetupContext] = {}
        self._session_keys: dict[str, str] = {}
        self._symbol_calendars: dict[str, SessionCalendar] = {}
        # Bars-in-forming counter for invalidation timers
        self._forming_bars: dict[UUID, int] = {}
        # Cooldown: symbol → {SetupType → bar-count-at-last-close}
        # Prevents the same setup type from re-opening within _COOLDOWN_BARS of closing.
        self._last_close_bar: dict[str, dict["SetupType", int]] = {}
        self._bar_counts: dict[str, int] = {}  # rolling bar count per symbol per session
        self._bar_count_session: dict[str, str] = {}
        # ONL sweep state machine: symbol → {"sweep_low": Decimal, "bar_count": int} | None
        self._onl_sweep_state: dict[str, dict | None] = {}
        # Double-bottom candidate tracking: symbol → (first_bottom_low, bar_count)
        self._dbl_first_bottom: dict[str, tuple[Decimal, int] | None] = {}
        self._setups_detected: int = 0
        self._setups_triggered: int = 0
        self._pipeline_mode: bool = False  # set True by BarPipeline before engine.start()

    @property
    def name(self) -> str:
        return "SetupEngine"

    def set_feature_engine(self, engine: object) -> None:
        self._feature_engine = engine

    def set_market_state_engine(self, engine: object) -> None:
        self._market_state_engine = engine

    def set_context_engine(self, engine: object) -> None:
        self._context_engine = engine

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        for symbol in self._registry.all():
            ticker = symbol.ticker
            self._active[ticker] = {}
            self._symbol_calendars[ticker] = calendar_for_symbol(symbol)

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.BAR, self._handle_bar)

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        total_active = sum(len(v) for v in self._active.values())
        return EngineHealth(
            HealthStatus.HEALTHY,
            self.name,
            {
                "active_setups": total_active,
                "detected_total": self._setups_detected,
                "triggered_total": self._setups_triggered,
            },
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def active_setups(self, symbol: str) -> list[Setup]:
        return list(self._active.get(symbol, {}).values())

    def session_setup_context(self, symbol: str) -> SessionSetupContext | None:
        return self._session_contexts.get(symbol)

    def patch_setup_score(
        self,
        symbol: str,
        setup_id: "UUID",
        score: float,
        grade: "SetupGrade",
        conditions_met: list[str],
        conditions_missing: list[str],
        score_reasons: list[str],
        score_penalties: list[str],
    ) -> bool:
        """Update a confirmed setup's score/grade/reasons in-place.

        Called by ScoringEngine after computing the score.  Returns True if
        the setup was found and updated, False if it is no longer active.
        """
        active = self._active.get(symbol, {})
        if setup_id not in active:
            return False
        patched = active[setup_id].model_copy(update={
            "score": score,
            "grade": grade,
            "conditions_met": conditions_met,
            "conditions_missing": conditions_missing,
            "score_reasons": score_reasons,
            "score_penalties": score_penalties,
        })
        active[setup_id] = patched
        self._record_setup(symbol, patched)
        logger.info(
            "Setup scored: %s %s score=%.0f grade=%s",
            symbol, patched.setup_type, score, grade,
        )
        return True

    def prev_session_setup_context(self, symbol: str) -> SessionSetupContext | None:
        return self._prev_session_contexts.get(symbol)

    # ── Public pipeline API ───────────────────────────────────────────────────

    async def process_bar(
        self,
        snap: BarSnapshot,
        market_state: MarketState,
        thesis,                     # ActiveThesis | None — loosely typed to avoid circular import
        bundle: BarBundleEvent,
    ) -> list:
        """
        Stage 4 of the sequential pipeline.

        Called by BarPipeline with snapshot, market state, and thesis already computed.
        Returns list of active Setup objects after scanning and updating.
        """
        symbol = bundle.symbol
        bar = bundle.to_bar_event()
        await self._roll_session_if_needed(symbol, bar.timestamp, bar)
        session_key = self._session_keys.get(symbol, "")

        if self._bar_count_session.get(symbol) != session_key:
            self._bar_count_session[symbol] = session_key
            self._bar_counts[symbol] = 0
            self._last_close_bar.pop(symbol, None)
            self._onl_sweep_state.pop(symbol, None)
            self._dbl_first_bottom.pop(symbol, None)
        self._bar_counts[symbol] = self._bar_counts.get(symbol, 0) + 1

        await self._scan_for_setups(symbol, snap, market_state, bar)
        await self._update_active_setups(symbol, snap, market_state, bar)
        return self.active_setups(symbol)

    # ── Handler ───────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return
        if event.timeframe != BarTimeframe.M1:
            return  # S1 and other sub-minute bars are for push WS only
        if self._pipeline_mode:
            return  # BarPipeline owns M1 processing

        symbol = event.symbol
        # Roll session BEFORE reading session_key so the bar counter compares
        # against the current (possibly just-rolled) session, not the prior one.
        await self._roll_session_if_needed(symbol, event.timestamp, event)
        session_key = self._session_keys.get(symbol, "")

        # Increment per-symbol bar counter; reset on new session.
        if self._bar_count_session.get(symbol) != session_key:
            self._bar_count_session[symbol] = session_key
            self._bar_counts[symbol] = 0
            self._last_close_bar.pop(symbol, None)
            self._onl_sweep_state.pop(symbol, None)
            self._dbl_first_bottom.pop(symbol, None)
        self._bar_counts[symbol] = self._bar_counts.get(symbol, 0) + 1

        snapshot = self._get_snapshot(symbol)
        market_state = self._get_market_state(symbol)
        if snapshot is None or market_state is None:
            self._log_scan_detail(
                "Setup scan skipped: %s missing_context snapshot=%s market_state=%s",
                symbol,
                snapshot is not None,
                market_state is not None,
            )
            return

        await self._scan_for_setups(symbol, snapshot, market_state, event)
        await self._update_active_setups(symbol, snapshot, market_state, event)

    # ── Detection ─────────────────────────────────────────────────────────────

    async def _scan_for_setups(
        self,
        symbol: str,
        snapshot: BarSnapshot,
        market_state: MarketState,
        trigger: BarEvent,
    ) -> None:
        detectors = [
            # Structural-sweep family (long bias, ordered loosest → strictest)
            (SetupType.VWAP_UNDERCUT_RECLAIM, self._detect_vwap_undercut_reclaim),
            (SetupType.SWEEP_RECLAIM, self._detect_sweep_reclaim),
            (SetupType.FAKE_BREAKDOWN, self._detect_fake_breakdown),
            (SetupType.DEEP_EXHAUSTION_RECLAIM, self._detect_deep_exhaustion_reclaim),
            # Momentum / trend continuation
            (SetupType.HOD_BREAKOUT, self._detect_hod_breakout),
            (SetupType.TREND_PULLBACK, self._detect_trend_pullback),
            # VWAP cross family
            (SetupType.VWAP_RECLAIM, self._detect_vwap_reclaim),
            (SetupType.VWAP_REJECTION, self._detect_vwap_rejection),
            (SetupType.VWAP_FAILED_RECLAIM_SHORT, self._detect_vwap_failed_reclaim_short),
            (SetupType.TREND_PULLBACK_SHORT, self._detect_trend_pullback_short),
            # ORB family
            (SetupType.ORB_BREAKOUT, self._detect_orb_breakout),
            (SetupType.ORB_BREAKDOWN, self._detect_orb_breakdown),
            # Failed-breakdown / key-level reclaim family
            (SetupType.ONL_SWEEP_RECLAIM_LONG, self._detect_onl_sweep_reclaim_long),
            (SetupType.DOUBLE_BOTTOM_RECLAIM_LONG, self._detect_double_bottom_reclaim_long),
        ]
        current_bar = self._bar_counts.get(symbol, 0)
        for setup_type, detector in detectors:
            if self._has_active(symbol, setup_type):
                self._log_scan_detail(
                    "Setup scan skipped: %s %s reason=already_active",
                    symbol,
                    setup_type,
                )
                continue
            last_close = self._last_close_bar.get(symbol, {}).get(setup_type)
            if last_close is not None and (current_bar - last_close) < _COOLDOWN_BARS:
                self._log_scan_detail(
                    "Setup scan skipped: %s %s reason=cooldown bars_remaining=%d",
                    symbol,
                    setup_type,
                    _COOLDOWN_BARS - (current_bar - last_close),
                )
                continue
            if not self._session_allows_detection(symbol, snapshot):
                self._log_scan_detail(
                    "Setup scan blocked: %s %s reason=session_phase_not_allowed phase=%s",
                    symbol,
                    setup_type,
                    snapshot.session_phase,
                )
                continue
            reason = self._detector_reason(setup_type, snapshot, market_state)
            if reason is None:
                await self._open_setup(symbol, setup_type, snapshot, market_state, trigger)
            else:
                self._log_scan_detail(
                    "Setup candidate rejected: %s %s reason=%s phase=%s",
                    symbol,
                    setup_type,
                    reason,
                    snapshot.session_phase,
                )

    def _has_active(self, symbol: str, setup_type: SetupType) -> bool:
        return any(
            s.setup_type == setup_type
            for s in self._active.get(symbol, {}).values()
        )

    # ── Structural-sweep family detectors ─────────────────────────────────────
    #
    # Four setups share the "sweep → reclaim" theme but differ in level,
    # depth, and required confirmation:
    #
    #  VWAP_UNDERCUT_RECLAIM  shallow VWAP dip (≤0.15%), quick reclaim        Grade A
    #  SWEEP_RECLAIM          swept OR low, closed back above it               Grade A
    #  FAKE_BREAKDOWN         swept OR low + must reclaim VWAP (strict)        SSS candidate
    #  DEEP_EXHAUSTION_RECLAIM deep VWAP deviation, capitulation + no new low  Grade A-

    # ── 1. VWAP_UNDERCUT_RECLAIM ──────────────────────────────────────────────

    def _detect_vwap_undercut_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return self._reason_vwap_undercut_reclaim(snap, ms) is None

    def _reason_vwap_undercut_reclaim(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        FORMING: 1–5 bars below VWAP, low ≤0.15% below VWAP, close in upper half.
        Ideal for trend-up days where price undercuts VWAP and snaps back.
        """
        if not (1 <= snap.bars_below_vwap <= 5):
            return "bars_below_vwap_not_in_1_to_5"
        if snap.bar.low < snap.vwap * Decimal("0.9985"):
            return "low_too_far_below_vwap"
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.45:
            return "close_in_lower_45pct_of_bar"
        # Reject if price is far below EMA9 — momentum is already broken
        if snap.ema_9 is not None and snap.bar.close < snap.ema_9 * Decimal("0.997"):
            return "close_too_far_below_ema9"
        return None

    # ── 2. SWEEP_RECLAIM ──────────────────────────────────────────────────────

    def _detect_sweep_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return self._reason_sweep_reclaim(snap, ms) is None

    def _reason_sweep_reclaim(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        FORMING: swept OR low (wick below) but closed back above it.
        Looser than FAKE_BREAKDOWN — VWAP reclaim not required yet.
        """
        if not snap.swept_orl:
            return "or_low_not_swept"
        if snap.orb_low is None or snap.bar.close <= snap.orb_low:
            return "close_not_above_orb_low"
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.50:
            return "close_in_lower_half"
        if snap.relative_volume is not None and snap.relative_volume < 1.1:
            return "relative_volume_lt_1_1"
        # Block strongly bearish EMA alignment (both EMAs sloping down, price below both)
        if (
            snap.ema_9 is not None and snap.ema_20 is not None
            and snap.bar.close < snap.ema_9 and snap.bar.close < snap.ema_20
            and snap.ema_9 < snap.ema_20
        ):
            return "strongly_bearish_ema_alignment"
        return None

    # ── 3. FAKE_BREAKDOWN (strict) ────────────────────────────────────────────

    def _detect_fake_breakdown(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return self._reason_fake_breakdown(snap, ms) is None

    def _reason_fake_breakdown(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        FORMING: price must have been below VWAP (bars_below_vwap >= 1) before
        sweeping the OR low — a bar that dips below OR low from above VWAP is a
        different pattern (aggressive breakdown attempt) and should not count.

        Then: swept OR low shallow (≤0.3%), closed back above OR low with conviction.
        CONFIRMED requires a full VWAP reclaim — this is the SSS gate.
        """
        # Price must have lost VWAP before the OR low sweep; otherwise this is
        # just an aggressive flush from above, not a fake breakdown.
        if snap.bars_below_vwap < 1:
            return "price_never_lost_vwap_before_sweep"
        if not snap.swept_orl:
            return "or_low_not_swept"
        if snap.orb_low is None or snap.bar.close <= snap.orb_low:
            return "close_not_above_orb_low"
        # Depth limit: sweep ≤0.3% below OR low — deeper is a real breakdown
        if snap.bar.low < snap.orb_low * Decimal("0.997"):
            return "sweep_too_deep_gt_0_3pct"
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.55:
            return "close_not_in_upper_45pct"
        if snap.relative_volume is not None and snap.relative_volume < 1.2:
            return "relative_volume_lt_1_2"
        return None

    # ── 4. DEEP_EXHAUSTION_RECLAIM ────────────────────────────────────────────

    def _detect_deep_exhaustion_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return self._reason_deep_exhaustion_reclaim(snap, ms) is None

    def _reason_deep_exhaustion_reclaim(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        FORMING: deep intrabar sweep below VWAP (low ≤−0.35%), new session low,
        high volume capitulation candle that closes off its lows.
        CONFIRMED only after 3+ bars with no new low and VWAP deviation shrinking.

        Depth is measured off the bar's low, not its close: vwap_deviation_pct
        is close-based, so a bar with a strong reclaim (high
        bar_close_position_pct) mechanically pulls its own close back toward
        VWAP, shrinking the very depth measurement this gate needs — the
        cleaner the reclaim, the less likely a close-based check is to see it
        as "deep enough." Using the low keeps the two checks independent, the
        same way Fake Breakdown measures sweep depth off bar.low while
        measuring reclaim quality off bar_close_position_pct.
        """
        if snap.bar.low > snap.vwap * Decimal("0.9965"):
            return "not_deep_enough_below_vwap"
        if not snap.is_new_lod:
            return "not_new_lod"
        if snap.relative_volume is not None and snap.relative_volume < 1.5:
            return "relative_volume_lt_1_5"
        # Close must be off the lows — not a pure continuation candle
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.35:
            return "close_too_low_in_bar"
        return None

    def _detect_hod_breakout(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        Prereq: intraday_high > orb_high, price above VWAP.
        FORMING: higher highs, within 0.2% of intraday high.
        MNQ opposed → blocked (skipped until MNQ lead is wired).
        """
        # ORB must be established
        if snap.orb_state == ORBState.NOT_SET or snap.orb_high is None:
            return False
        if snap.intraday_high is None:
            return False
        # Prereq: session high already above the OR high
        if snap.intraday_high <= snap.orb_high:
            return False
        # Prereq: price above VWAP
        if not snap.is_above_vwap:
            return False
        # FORMING: making higher highs
        if not snap.is_higher_high:
            return False
        # FORMING: close within 0.2% of intraday high
        dist_to_hod = float((snap.intraday_high - snap.bar.close) / snap.intraday_high)
        if dist_to_hod > 0.002:
            return False
        return True

    def _reason_hod_breakout(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        if snap.orb_state == ORBState.NOT_SET or snap.orb_high is None:
            return "orb_not_set"
        if snap.intraday_high is None:
            return "intraday_high_missing"
        if snap.intraday_high <= snap.orb_high:
            return "intraday_high_not_above_orb_high"
        if not snap.is_above_vwap:
            return "not_above_vwap"
        if not snap.is_higher_high:
            return "not_higher_high"
        dist_to_hod = float((snap.intraday_high - snap.bar.close) / snap.intraday_high)
        if dist_to_hod > 0.002:
            return "close_too_far_from_hod"
        return None

    def _detect_trend_pullback(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        FORMING: ≥5 consecutive bars above VWAP, pulling back (deviation shrinking),
        within 0.5% of VWAP.
        MNQ opposed → blocked (skipped until MNQ lead is wired).
        """
        if snap.bars_above_vwap < 5:
            return False
        if not snap.vwap_deviation_shrinking:
            return False
        # Within 0.5% of VWAP (and still above it — deviation positive)
        if snap.vwap_deviation_pct <= 0 or snap.vwap_deviation_pct > 0.5:
            return False
        return True

    def _reason_trend_pullback(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        if snap.bars_above_vwap < 5:
            return "bars_above_vwap_lt_5"
        if not snap.vwap_deviation_shrinking:
            return "vwap_deviation_not_shrinking"
        if snap.vwap_deviation_pct <= 0:
            return "not_above_vwap"
        if snap.vwap_deviation_pct > 0.5:
            return "pullback_too_far_from_vwap"
        return None

    # ── VWAP_FAILED_RECLAIM_SHORT ─────────────────────────────────────────────

    def _detect_vwap_failed_reclaim_short(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return self._reason_vwap_failed_reclaim_short(snap, ms) is None

    def _reason_vwap_failed_reclaim_short(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        FORMING: price has been below VWAP for 3+ bars, wicked up to test VWAP
        (high >= VWAP * 0.9995) but closed back below it with a weak close.
        This is "buyers tried to reclaim VWAP, sellers defended it."

        Unlike VWAP_REJECTION (which fires on the initial cross-down), this setup
        requires the price to already be below VWAP and fail a reclaim attempt.
        """
        if snap.bars_below_vwap < 3:
            return "bars_below_vwap_lt_3"
        # High must have touched or wicked into VWAP zone
        if snap.bar.high < snap.vwap * Decimal("0.9995"):
            return "bar_high_did_not_reach_vwap"
        # Close back below VWAP — failed reclaim
        if snap.is_above_vwap:
            return "close_above_vwap"
        # Weak close = rejection conviction (close in lower half of bar)
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct > 0.5:
            return "close_in_upper_half"
        # EMA9 must be ≤ EMA20 — bearish alignment (sellers have structural control)
        if snap.ema_9 is not None and snap.ema_20 is not None and snap.ema_9 > snap.ema_20:
            return "ema9_above_ema20_not_bearish"
        # VWAP must not be rising — a rising VWAP means cumulative buying, short risk high
        if snap.vwap_slope_direction == "up":
            return "vwap_slope_rising"
        # Guard: if EMA9 is actively rising AND price closed above it, V-reclaim is likely
        if snap.ema_9_slope_direction == "up" and snap.ema_9 is not None and snap.bar.close >= snap.ema_9:
            return "ema9_curling_up_close_above_ema9"
        return None

    # ── TREND_PULLBACK_SHORT ──────────────────────────────────────────────────

    def _detect_trend_pullback_short(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return self._reason_trend_pullback_short(snap, ms) is None

    def _reason_trend_pullback_short(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        FORMING: confirmed bearish regime (5+ bars below VWAP, EMA9 < EMA20),
        price is in a pullback toward EMA9 (not making new lows, high approaches EMA9)
        but closes below EMA9 — sellers defending the dynamic resistance.

        Entry is triggered when a subsequent bar breaks below the forming bar's low,
        confirming the lower high and resuming the downtrend.
        """
        if snap.bars_below_vwap < 5:
            return "bars_below_vwap_lt_5"
        if snap.ema_9 is None or snap.ema_20 is None:
            return "ema_not_available"
        # Bearish EMA alignment required
        if snap.ema_9 >= snap.ema_20:
            return "ema9_not_below_ema20"
        # EMA9 must not be rising — flat or down only
        if snap.ema_9_slope_direction == "up":
            return "ema9_slope_rising"
        # EMA20 must not be rising — trend must have real momentum, not just short-term dip
        if snap.ema_20_slope_direction == "up":
            return "ema20_slope_rising"
        # A lower low must have been made recently — confirms this is a real downtrend,
        # not VWAP-area chop. Without this, any 5-bar below-VWAP drift + EMA9 touch qualifies.
        if not snap.recent_lower_low:
            return "no_recent_lower_low"
        # Not making new lows right now — price is in a pullback, not continuation
        if snap.is_new_lod:
            return "making_new_lod_not_a_pullback"
        # High must have approached EMA9 (within 0.3%) — the bounce touched EMA9
        if snap.bar.high < snap.ema_9 * Decimal("0.997"):
            return "bar_high_too_far_from_ema9"
        # Close must remain below EMA9 — EMA9 acted as resistance
        if snap.bar.close >= snap.ema_9:
            return "close_above_ema9_reclaim"
        return None

    def _advance_vwap_reclaim(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            # Invalidation: lost VWAP again
            if snap.vwap_cross_down:
                return setup.transition(SetupState.INVALIDATED, "VWAP lost after reclaim", bar_time=snap.timestamp), ""
            # Confirm: hold above VWAP on next bar + rvol ≥ 1.0 + no lower low
            if snap.is_above_vwap:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.0
                no_lower_low = not snap.is_lower_low
                if rvol_ok and no_lower_low:
                    # Entry above the reclaim bar's high; stop = reclaim bar's low
                    entry = setup.bar_snapshot.bar.high
                    stop = setup.bar_snapshot.bar.low
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = (
                        snap.intraday_high
                        if snap.intraday_high is not None and snap.intraday_high > entry
                        else entry + (entry - stop) * mult
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={
                            "entry_trigger": entry,
                            "stop_reference": stop,
                            "target_reference": target,
                        }
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if snap.vwap_cross_down:
                return setup.transition(SetupState.INVALIDATED, "VWAP lost while confirmed", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_vwap_rejection(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            # Invalidation: reclaimed VWAP
            if snap.vwap_cross_up:
                return setup.transition(SetupState.INVALIDATED, "VWAP reclaimed after rejection", bar_time=snap.timestamp), ""
            # Confirm: hold below VWAP on next bar + rvol ≥ 1.0 + no higher high
            if not snap.is_above_vwap:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.0
                no_higher_high = not snap.is_higher_high
                if rvol_ok and no_higher_high:
                    # Entry below the rejection bar's low; stop = rejection bar's high
                    entry = setup.bar_snapshot.bar.low
                    stop = setup.bar_snapshot.bar.high
                    mult = self._target_mult("sell", ms)
                    target = (
                        snap.intraday_low
                        if snap.intraday_low is not None and snap.intraday_low < entry
                        else entry - (stop - entry) * mult
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={
                            "entry_trigger": entry,
                            "stop_reference": stop,
                            "target_reference": target,
                        }
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if snap.vwap_cross_up:
                return setup.transition(SetupState.INVALIDATED, "VWAP reclaimed while confirmed", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.low <= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.high >= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_vwap_failed_reclaim_short(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED: hold below VWAP on next bar, no higher high.
        Entry = forming bar's low (break confirms rejection).
        Stop  = forming bar's high (VWAP test high).
        Timeout at 6 bars; invalidated if VWAP is reclaimed.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            if snap.vwap_cross_up:
                return setup.transition(SetupState.INVALIDATED, "vwap_reclaimed_after_rejection", bar_time=snap.timestamp), ""
            if bars > 6:
                return setup.transition(SetupState.INVALIDATED, "forming_timeout_gt_6_bars", bar_time=snap.timestamp), ""

            if not snap.is_above_vwap:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 0.8
                # Require actual downside continuation: bar must NOT exceed forming bar's high
                # AND must have already traded below forming bar's low (sellers following through,
                # not just drifting sideways below VWAP).
                no_higher_high = snap.bar.high <= setup.bar_snapshot.bar.high
                broke_forming_low = snap.bar.low < setup.bar_snapshot.bar.low
                if rvol_ok and no_higher_high and broke_forming_low:
                    entry = setup.bar_snapshot.bar.low
                    stop = setup.bar_snapshot.bar.high
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("sell", ms)
                    target = (
                        snap.intraday_low
                        if snap.intraday_low is not None and snap.intraday_low < entry
                        else entry - (stop - entry) * mult
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if snap.vwap_cross_up:
                return setup.transition(SetupState.INVALIDATED, "vwap_reclaimed_while_confirmed", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.low <= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.high >= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_trend_pullback_short(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED: on a subsequent bar, lower high forms (this bar's high
        is below the forming bar's high) and price is still below VWAP and EMA9.
        Entry = forming bar's low (break of pullback bar's low).
        Stop  = forming bar's high (max of the bounce).
        Timeout at 8 bars; invalidated on VWAP reclaim or close above EMA20.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            if snap.vwap_cross_up:
                return setup.transition(SetupState.INVALIDATED, "vwap_cross_up", bar_time=snap.timestamp), ""
            if snap.ema_20 is not None and snap.bar.close >= snap.ema_20:
                return setup.transition(SetupState.INVALIDATED, "close_above_ema20", bar_time=snap.timestamp), ""
            if bars > 8:
                return setup.transition(SetupState.INVALIDATED, "forming_timeout_gt_8_bars", bar_time=snap.timestamp), ""

            # Confirm: lower high + price broke below pullback bar's low (downtrend resumed)
            # + still below EMA9, EMA20, and VWAP.  All four must hold — no partial signals.
            lower_high = snap.bar.high < setup.bar_snapshot.bar.high
            broke_forming_low = snap.bar.low < setup.bar_snapshot.bar.low
            below_ema9 = snap.ema_9 is None or snap.bar.close < snap.ema_9
            below_ema20 = snap.ema_20 is None or snap.bar.close < snap.ema_20
            if lower_high and broke_forming_low and not snap.is_above_vwap and below_ema9 and below_ema20:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 0.8
                if rvol_ok:
                    entry = setup.bar_snapshot.bar.low
                    stop = setup.bar_snapshot.bar.high
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("sell", ms)
                    target = (
                        snap.intraday_low
                        if snap.intraday_low is not None and snap.intraday_low < entry
                        else entry - (stop - entry) * mult
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if snap.vwap_cross_up:
                return setup.transition(SetupState.INVALIDATED, "vwap_cross_up_while_confirmed", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.low <= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.high >= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_orb_breakdown(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            # Invalidation: OR low reclaimed (first cross back up, or close above level)
            if snap.orb_cross_up:
                return setup.transition(SetupState.INVALIDATED, "orb_low_reclaimed_cross_up", bar_time=snap.timestamp), ""
            if snap.orb_low is not None and snap.bar.close > snap.orb_low:
                return setup.transition(SetupState.INVALIDATED, "orb_low_reclaimed_close_above", bar_time=snap.timestamp), ""
            if snap.is_above_vwap:
                return setup.transition(SetupState.INVALIDATED, "reclaimed_vwap_while_forming", bar_time=snap.timestamp), ""
            # Confirm: hold below ORB low on next bar + rvol ≥ 1.0
            if snap.orb_low is not None and snap.bar.close < snap.orb_low:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.0
                if rvol_ok:
                    # Entry: forming bar's close — where price was when the breakdown
                    # was first detected (an actionable fill price, not the stale ORB level).
                    entry = setup.bar_snapshot.bar.close
                    # Stop: just above ORB low — the structural level; if price reclaims
                    # it the breakdown has failed.
                    stop = snap.orb_low * Decimal("1.001")
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    orb_range = (
                        (snap.orb_high - snap.orb_low)
                        if snap.orb_high and snap.orb_low else None
                    )
                    mult = self._target_mult("sell", ms)
                    target = (
                        entry - orb_range * mult
                        if orb_range else entry - (stop - entry) * mult
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={
                            "entry_trigger": entry,
                            "stop_reference": stop,
                            "target_reference": target,
                        }
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            # Invalidation: price reclaims ORB low (close above or first orb_cross_up)
            if snap.orb_cross_up:
                return setup.transition(SetupState.INVALIDATED, "orb_low_reclaimed_cross_up", bar_time=snap.timestamp), ""
            if snap.orb_low is not None and snap.bar.close > snap.orb_low:
                return setup.transition(SetupState.INVALIDATED, "orb_low_reclaimed_close_above", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.low <= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.high >= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _detect_vwap_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        FORMING: price crossed above VWAP from at least 2 bars below, closing
        with conviction (>0.05% above VWAP, upper half of bar).
        """
        if not snap.vwap_cross_up:
            return False
        if snap.vwap_cross_up_after_bars < 2:
            return False
        if snap.vwap_deviation_pct < 0.05:
            return False
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.5:
            return False
        return True

    def _reason_vwap_reclaim(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        if not snap.vwap_cross_up:
            return "no_vwap_cross_up"
        if snap.vwap_cross_up_after_bars < 2:
            return "bars_below_vwap_before_cross_lt_2"
        if snap.vwap_deviation_pct < 0.05:
            return "close_not_above_vwap_with_conviction"
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.5:
            return "close_in_lower_half_of_bar"
        return None

    @staticmethod
    def _vwap_rejection_alignment_reason(snap: BarSnapshot) -> str | None:
        if snap.ema_9 is not None and snap.bar.close >= snap.ema_9:
            return "close_not_below_ema9"
        if snap.ema_20 is not None and snap.bar.close >= snap.ema_20:
            return "close_not_below_ema20"
        if snap.ema_9_slope is not None and snap.ema_9_slope >= 0:
            return "ema9_slope_not_down"
        if snap.ema_20_slope is not None and snap.ema_20_slope >= 0:
            return "ema20_slope_not_down"
        if snap.is_bull_stack_5m is True:
            return "five_min_stack_still_bullish"
        if snap.ema9_5m_slope_direction == "up" or snap.ema21_5m_slope_direction == "up":
            return "five_min_slope_not_bearish"
        return None

    def _detect_vwap_rejection(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        Detects the INITIAL clean cross below VWAP — effectively a VWAP breakdown short.
        FORMING: price crossed below VWAP from at least 2 bars above, closing
        with conviction (>0.05% below VWAP, lower half of bar).

        NOTE: fires on the first cross-down regardless of EMA alignment or slope.
        No slope or alignment filters — risk of catching fake breakdowns.
        For the higher-quality failed-reclaim pattern see VWAP_FAILED_RECLAIM_SHORT.

        ── 1m prototype ── detection runs entirely on the 1m bar stream.
        """
        if not snap.vwap_cross_down:
            return False
        if snap.vwap_cross_down_after_bars < 2:
            return False
        if snap.vwap_deviation_pct > -0.05:
            return False
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct > 0.5:
            return False
        if self._vwap_rejection_alignment_reason(snap) is not None:
            return False
        return True

    def _reason_vwap_rejection(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        if not snap.vwap_cross_down:
            return "no_vwap_cross_down"
        if snap.vwap_cross_down_after_bars < 2:
            return "bars_above_vwap_before_cross_lt_2"
        if snap.vwap_deviation_pct > -0.05:
            return "close_not_below_vwap_with_conviction"
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct > 0.5:
            return "close_in_upper_half_of_bar"
        alignment_reason = self._vwap_rejection_alignment_reason(snap)
        if alignment_reason is not None:
            return alignment_reason
        return None

    def _detect_orb_breakout(self, snap: BarSnapshot, ms: MarketState) -> bool:
        if snap.orb_high is None:
            return False
        if snap.orb_state != ORBState.BREAKOUT_UP:
            return False
        if not snap.is_above_vwap:
            return False
        if snap.bar.close <= snap.orb_high:
            return False
        if snap.bar.open > snap.orb_high and snap.bar.low > snap.orb_high:
            return False
        return True

    def _reason_orb_breakout(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        if snap.orb_high is None:
            return "no_orb_high_set"
        if snap.orb_state != ORBState.BREAKOUT_UP:
            return "not_orb_breakout_up"
        if not snap.is_above_vwap:
            return "below_vwap"
        if snap.bar.close <= snap.orb_high:
            return "close_not_above_orb_high"
        if snap.bar.open > snap.orb_high and snap.bar.low > snap.orb_high:
            return "already_extended_above_orb_high"
        return None

    def _detect_orb_breakdown(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        FORMING: detects only the INITIAL cross below OR low (orb_cross_down flag),
        price below VWAP, and close below OR low.

        orb_cross_down is True only on the exact bar that first closes below orb_low
        (populated by the Feature Engine; see BarSnapshot.orb_cross_down).  Using
        is_new_lod as the primary trigger would fire on every subsequent new low,
        not just the initial break.
        """
        if snap.orb_low is None:
            return False
        if not snap.orb_cross_down:
            return False
        if snap.is_above_vwap:
            return False
        if snap.bar.close >= snap.orb_low:
            return False
        return True

    def _reason_orb_breakdown(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        if snap.orb_low is None:
            return "no_orb_low_set"
        if not snap.orb_cross_down:
            return "not_initial_orb_cross_down"
        if snap.is_above_vwap:
            return "above_vwap"
        if snap.bar.close >= snap.orb_low:
            return "close_not_below_orb_low"
        return None

    # ── ONL_SWEEP_RECLAIM_LONG ────────────────────────────────────────────────

    def _detect_onl_sweep_reclaim_long(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        Stateful state machine — all mutations happen here; _reason is a pure read.

        Phases:
          1. Bar goes below ONL → start tracking sweep_low
          2. Subsequent bars extend lower but within breakdown threshold → update sweep_low
          3. Bar closes above ONL → fire if sweep depth ≤ threshold and strong close
          4. Breakdown (extension > 0.5% from ONL, or > 8 bars below) → clear state
        Handles both single-bar sweeps (phases 1+3 on same bar) and multi-bar sweeps.
        """
        symbol = snap.symbol
        bar_count = self._bar_counts.get(symbol, 0)

        ctx = self._context_engine.get_context(symbol) if self._context_engine else None
        onl = ctx.onl if ctx else None

        if onl is None:
            return False

        state = self._onl_sweep_state.get(symbol)

        # Clear timed-out state (> 8 bars below ONL without reclaiming)
        if state is not None and bar_count - state["bar_count"] > 8:
            self._onl_sweep_state.pop(symbol, None)
            state = None

        if snap.bar.low < onl:
            if state is None:
                # Phase 1: first bar to go below ONL — start tracking
                self._onl_sweep_state[symbol] = {"sweep_low": snap.bar.low, "bar_count": bar_count}
                state = self._onl_sweep_state[symbol]
            elif snap.bar.low < state["sweep_low"]:
                # Check total extension from ONL; > 0.5% = confirmed breakdown, clear
                if float(onl - snap.bar.low) / float(onl) > 0.005:
                    self._onl_sweep_state.pop(symbol, None)
                    return False
                # Still within tolerance — update sweep_low
                self._onl_sweep_state[symbol] = {**state, "sweep_low": snap.bar.low}
                state = self._onl_sweep_state[symbol]

        # Detection check (pure read — see _reason)
        return self._reason_onl_sweep_reclaim_long(snap, ms) is None

    def _reason_onl_sweep_reclaim_long(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        Pure read — no state mutations.  Called both from _detect and _detector_reason.

        Sweep depth classification (ATR-normalized):
          0 – 2.0× ATR  : clean liquidity sweep  → fires
          2.0 – 3.0× ATR: deep sweep             → fires, strong close required
          > 3.0× ATR    : likely breakdown        → blocked

        Secondary check: sweep depth vs RTH candle distribution.
          > rth_p90 × 2 : abnormally large for today → blocked

        Hard cap: 30 pts regardless of ATR / distribution.
        5M21 / VWAP reclaim are confirmation score signals — not hard gates.
        """
        if self._context_engine is None:
            return "no_context_engine"
        ctx = self._context_engine.get_context(snap.symbol)
        if ctx is None or ctx.onl is None:
            return "no_onl"
        onl = ctx.onl

        state = self._onl_sweep_state.get(snap.symbol)
        if state is None:
            return "no_active_sweep_below_onl"

        if snap.bar.close <= onl:
            return "close_not_above_onl"

        sweep_pts = onl - state["sweep_low"]

        # Dynamic hard cap: max(30 pts, 2.5× ATR_14), absolute ceiling 60 pts.
        # On a high-volatility day (FOMC/CPI/open panic), a 40-pt sweep that is
        # only 2× ATR should not be blocked by a rigid 30-pt cap.
        if snap.atr_14 and snap.atr_14 > Decimal("0"):
            dynamic_cap = max(Decimal("30"), snap.atr_14 * Decimal("2.5"))
            hard_cap = min(dynamic_cap, Decimal("60"))
        else:
            hard_cap = Decimal("30")
        if sweep_pts > hard_cap:
            return f"sweep_too_deep_gt_{float(hard_cap):.0f}pts_cap"

        # ATR-normalized classification (cap above already blocks > 2.5× ATR)
        if snap.atr_14 and snap.atr_14 > Decimal("0"):
            sweep_atrs = float(sweep_pts) / float(snap.atr_14)
            # 2–2.5× ATR: deep but valid — require strong close (≥ 60th pct)
            if sweep_atrs > 2.0:
                if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.6:
                    return "deep_sweep_requires_strong_close_ge_60pct"
        else:
            # ATR not yet available — fall back to RTH p75 then percentage
            if snap.rth_p75_1m_range is not None:
                if sweep_pts > snap.rth_p75_1m_range * Decimal("2"):
                    return "sweep_too_deep_vs_rth_p75_fallback"
            elif float(sweep_pts) / float(onl) > 0.0015:
                return "sweep_too_deep_pct_fallback"

        # RTH distribution check — only once we have ≥ 15 bars of RTH history.
        # Early RTH (first ~15 min) has too few samples; ATR is more reliable then.
        rth_mature = snap.bars_since_open is not None and snap.bars_since_open >= 15
        if rth_mature and snap.rth_p90_1m_range is not None:
            if sweep_pts > snap.rth_p90_1m_range * Decimal("2"):
                return "sweep_too_deep_vs_rth_p90_distribution"

        # Base close-quality gate (≥ 50th pct for clean sweeps; ≥ 60th enforced above for deep)
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.5:
            return "close_in_lower_half"

        return None

    def _advance_onl_sweep_reclaim_long(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED: next bar holds above ONL, no new lower low, close ≥ 40th percentile.
        Entry = forming bar's high.  Stop = forming bar's low (sweep low).
        Timeout: 6 bars.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            ctx = self._context_engine.get_context(setup.symbol) if self._context_engine else None
            onl = ctx.onl if ctx else None

            if onl is not None and snap.bar.close < onl:
                return setup.transition(SetupState.INVALIDATED, "close_below_onl_reclaim_failed", bar_time=snap.timestamp), ""
            if bars > 6:
                return setup.transition(SetupState.INVALIDATED, "forming_timeout_gt_6_bars", bar_time=snap.timestamp), ""

            if onl is not None and snap.bar.close > onl:
                no_lower_low = not snap.is_lower_low
                close_pos_ok = snap.bar_close_position_pct is None or snap.bar_close_position_pct >= 0.4
                if no_lower_low and close_pos_ok:
                    entry = setup.bar_snapshot.bar.high
                    stop = setup.bar_snapshot.bar.low
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = entry + (entry - stop) * mult
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"

        return setup, ""

    # ── DOUBLE_BOTTOM_RECLAIM_LONG ────────────────────────────────────────────

    def _detect_double_bottom_reclaim_long(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        Stateful detector.  On each bar:
          1. Check if this bar is the second bottom (pure read of stored state).
          2. If not, update the first-bottom candidate state.
        This order prevents overwriting the first-bottom state with the second bottom's low.
        """
        symbol = snap.symbol
        bar_count = self._bar_counts.get(symbol, 0)

        # Check second bottom BEFORE potentially updating first-bottom state
        is_second = self._reason_double_bottom_reclaim_long(snap, ms) is None

        if not is_second:
            # Update first-bottom candidate.
            # First bottom does NOT require a strong close — panic sellers and exhaustion
            # candles can both be valid first legs.  Only the SECOND bottom needs to close
            # in the upper half (showing buyers won the zone).
            if snap.is_new_lod or snap.is_new_rth_lod:
                first = self._dbl_first_bottom.get(symbol)
                close_pos = snap.bar_close_position_pct
                weak_close = close_pos is not None and close_pos < 0.3
                if weak_close and first is not None and snap.bar.low < first[0] * Decimal("0.997"):
                    # Aggressive extension on a very weak close → breakdown, clear candidate
                    self._dbl_first_bottom.pop(symbol, None)
                else:
                    # Store / update first-bottom candidate regardless of close position
                    if first is None or snap.bar.low < first[0] * Decimal("0.9997"):
                        self._dbl_first_bottom[symbol] = (snap.bar.low, bar_count)

        return is_second

    def _reason_double_bottom_reclaim_long(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        """
        Pure read of double-bottom state — no mutations.
        Conditions (v0.2):
          - First bottom tracked (any close quality — first leg can be a panic sell)
          - Second bottom: low within min(40 pts, 2.5× ATR_14); fallback 0.15%
          - ≥ 3 bars and ≤ 60 bars since first bottom
          - Second low does not materially extend (low ≥ first_bottom − max_extension)
          - Strong close on second bottom (bar_close_position_pct ≥ 0.5)
          - Close above first bottom's low (buyers won the zone)
        5M21 / VWAP reclaim are confirmation score signals — not hard gates.
        """
        symbol = snap.symbol
        bar_count = self._bar_counts.get(symbol, 0)
        first = self._dbl_first_bottom.get(symbol)

        if first is None:
            return "no_first_bottom_tracked"

        first_low, first_bar = first
        bars_since_first = bar_count - first_bar

        if bars_since_first < 3:
            return "too_few_bars_since_first_bottom"
        if bars_since_first > 60:
            return "first_bottom_too_stale"

        # ATR-based proximity: low must be within min(40 pts, 2.5× ATR_14) of first_low
        tolerance = (
            min(Decimal("40"), snap.atr_14 * Decimal("2.5"))
            if snap.atr_14 else first_low * Decimal("0.0015")
        )
        if abs(snap.bar.low - first_low) > tolerance:
            return "low_not_near_first_bottom_zone"

        # Second low must not materially extend below first bottom
        max_extension = (
            min(Decimal("20"), snap.atr_14)
            if snap.atr_14 else first_low * Decimal("0.001")
        )
        if snap.bar.low < first_low - max_extension:
            return "second_low_extends_materially_below_first"

        # Second bottom must close strongly (upper half) — this is where buyers won
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct < 0.5:
            return "second_bottom_close_in_lower_half"

        if snap.bar.close <= first_low:
            return "second_bottom_close_not_above_first_low"

        return None

    def _advance_double_bottom_reclaim_long(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED: next bar holds above second bottom's low, no new lower low,
        and close ≥ second bottom bar's close.
        Entry = second bottom bar's high.  Stop = second bottom bar's low.
        Timeout: 8 bars.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            # Invalidation: lower low below the second bottom
            if snap.bar.low < setup.bar_snapshot.bar.low:
                return setup.transition(SetupState.INVALIDATED, "new_lower_low_below_second_bottom", bar_time=snap.timestamp), ""
            if bars > 8:
                return setup.transition(SetupState.INVALIDATED, "forming_timeout_gt_8_bars", bar_time=snap.timestamp), ""

            # Confirm: hold above second bottom's close, no lower low
            if snap.bar.close >= setup.bar_snapshot.bar.close and not snap.is_lower_low:
                entry = setup.bar_snapshot.bar.high
                stop = setup.bar_snapshot.bar.low
                rw_reason = self._risk_width_rejection(entry, stop, snap)
                if rw_reason:
                    return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                mult = self._target_mult("buy", ms)
                target = (
                    snap.intraday_high
                    if snap.intraday_high is not None and snap.intraday_high > entry
                    else entry + (entry - stop) * mult
                )
                confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                    update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                )
                logger.info(
                    "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                    setup.symbol, setup.setup_type,
                    float(entry), float(stop), float(target),
                )
                return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"

        return setup, ""

    def _detector_reason(
        self,
        setup_type: SetupType,
        snapshot: BarSnapshot,
        market_state: MarketState,
    ) -> str | None:
        if setup_type == SetupType.VWAP_UNDERCUT_RECLAIM:
            return self._reason_vwap_undercut_reclaim(snapshot, market_state)
        if setup_type == SetupType.SWEEP_RECLAIM:
            return self._reason_sweep_reclaim(snapshot, market_state)
        if setup_type == SetupType.FAKE_BREAKDOWN:
            return self._reason_fake_breakdown(snapshot, market_state)
        if setup_type == SetupType.DEEP_EXHAUSTION_RECLAIM:
            return self._reason_deep_exhaustion_reclaim(snapshot, market_state)
        if setup_type == SetupType.HOD_BREAKOUT:
            return self._reason_hod_breakout(snapshot, market_state)
        if setup_type == SetupType.TREND_PULLBACK:
            return self._reason_trend_pullback(snapshot, market_state)
        if setup_type == SetupType.VWAP_RECLAIM:
            return self._reason_vwap_reclaim(snapshot, market_state)
        if setup_type == SetupType.VWAP_REJECTION:
            return self._reason_vwap_rejection(snapshot, market_state)
        if setup_type == SetupType.VWAP_FAILED_RECLAIM_SHORT:
            return self._reason_vwap_failed_reclaim_short(snapshot, market_state)
        if setup_type == SetupType.TREND_PULLBACK_SHORT:
            return self._reason_trend_pullback_short(snapshot, market_state)
        if setup_type == SetupType.ORB_BREAKDOWN:
            return self._reason_orb_breakdown(snapshot, market_state)
        if setup_type == SetupType.ORB_BREAKOUT:
            return self._reason_orb_breakout(snapshot, market_state)
        if setup_type == SetupType.ONL_SWEEP_RECLAIM_LONG:
            return self._reason_onl_sweep_reclaim_long(snapshot, market_state)
        if setup_type == SetupType.DOUBLE_BOTTOM_RECLAIM_LONG:
            return self._reason_double_bottom_reclaim_long(snapshot, market_state)
        return "no_reason_available"

    # ── State machine ─────────────────────────────────────────────────────────

    async def _open_setup(
        self,
        symbol: str,
        setup_type: SetupType,
        snapshot: BarSnapshot,
        market_state: MarketState,
        trigger: BarEvent,
    ) -> None:
        bar_time = trigger.timestamp
        # Deterministic setup_id: same symbol + setup_type + bar_time always
        # produces the same UUID, so restarts never duplicate stored setups.
        setup_id = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{symbol}:{setup_type}:{bar_time.isoformat()}",
        )
        # structural_grade is a detection-time hint for ScoringEngine.
        # grade is intentionally left None here — ScoringEngine produces the final grade.
        structural_grade = SetupGrade.SSS if setup_type in _SSS_TYPES else None
        _attrs = attrs_for(setup_type)
        setup = Setup(
            setup_id=setup_id,
            symbol=symbol,
            setup_type=setup_type,
            state=SetupState.FORMING,
            detected_at=bar_time,
            updated_at=bar_time,
            market_state=market_state,
            bar_snapshot=snapshot,
            structural_grade=structural_grade,
            family=_attrs.family if _attrs else None,
            anchor=_attrs.anchor if _attrs else None,
            interaction=_attrs.interaction if _attrs else None,
            display_name=_attrs.display_name if _attrs else None,
        )
        self._active.setdefault(symbol, {})[setup.setup_id] = setup
        self._setups_detected += 1
        self._record_setup(symbol, setup)
        logger.info("Setup detected: %s %s @ %s", symbol, setup_type, snapshot.timestamp)
        await self._emit(setup, None, trigger)

    async def _update_active_setups(
        self,
        symbol: str,
        snapshot: BarSnapshot,
        market_state: MarketState,
        trigger: BarEvent,
    ) -> None:
        to_remove: list[UUID] = []
        for setup_id, setup in self._active.get(symbol, {}).items():
            updated, _ = self._advance_state(setup, snapshot, market_state)
            if updated.state != setup.state:
                self._active[symbol][setup_id] = updated
                self._record_setup(symbol, updated)
                await self._emit(updated, setup.state, trigger)
                if updated.state == SetupState.TRIGGERED:
                    self._setups_triggered += 1
            if updated.state in {
                SetupState.TRIGGERED,
                SetupState.FAILED,
                SetupState.INVALIDATED,
                SetupState.EXPIRED,
            }:
                to_remove.append(setup_id)
        for sid in to_remove:
            closed_setup = self._active[symbol].pop(sid, None)
            self._forming_bars.pop(sid, None)
            # Record cooldown: same setup type cannot reopen for _COOLDOWN_BARS bars.
            if closed_setup is not None:
                self._last_close_bar.setdefault(symbol, {})[closed_setup.setup_type] = (
                    self._bar_counts.get(symbol, 0)
                )

    @staticmethod
    def _target_mult(side: str, ms: "MarketState") -> Decimal:
        """R multiplier for take-profit.

        Uses live_bias (recalculated every bar) as the primary signal so that
        a TREND_DOWN day that has since reclaimed VWAP and flipped bullish still
        gives longs the 4R target instead of the 2R "against trend" multiplier.
        Falls back to locked day_type only when live_bias is neutral/unknown.
        """
        from alpha.models.enums import DayType, LiveBias
        lb = ms.live_bias
        bullish = lb in {LiveBias.BULLISH, LiveBias.TRANSITIONING_BULLISH}
        bearish = lb in {LiveBias.BEARISH, LiveBias.TRANSITIONING_BEARISH}

        if side == "buy":
            if bullish:
                return Decimal("4")
            if bearish:
                return Decimal("2")
        if side == "sell":
            if bearish:
                return Decimal("4")
            if bullish:
                return Decimal("2")

        # Neutral/unknown live_bias — fall back to locked day_type
        day_type = ms.day_type
        with_trend = (day_type == DayType.TREND_UP and side == "buy") or \
                     (day_type == DayType.TREND_DOWN and side == "sell")
        against_trend = (day_type == DayType.TREND_UP and side == "sell") or \
                        (day_type == DayType.TREND_DOWN and side == "buy")
        if with_trend:
            return Decimal("4")
        if against_trend:
            return Decimal("2")
        if day_type == DayType.RANGE:
            return Decimal("1.5")
        return Decimal("3")  # BALANCED / UNKNOWN

    # ── Risk-width guard ──────────────────────────────────────────────────────

    # Maximum risk as a multiple of ATR-14.  A confirmed entry whose
    # risk (|entry - stop|) exceeds this is rejected at confirm time —
    # typically caused by a huge reclaim candle producing a terrible R:R.
    _MAX_RISK_ATR_MULT: ClassVar[Decimal] = Decimal("1.5")
    # Fallback threshold (% of entry price) used when ATR is not yet available.
    _MAX_RISK_PCT: ClassVar[Decimal] = Decimal("0.005")  # 0.5 %

    @staticmethod
    def _risk_width_rejection(
        entry: Decimal, stop: Decimal, snap: BarSnapshot
    ) -> str | None:
        """Return a rejection reason if the risk width is too wide; None if acceptable.

        Applied at confirm time to prevent large-candle setups from producing
        entries with poor R:R.  atr_14 is the primary benchmark; when it is
        unavailable a hard percentage-of-price fallback applies.
        """
        risk = abs(entry - stop)
        if snap.atr_14 is not None:
            if risk > snap.atr_14 * SetupEngine._MAX_RISK_ATR_MULT:
                return (
                    f"risk_too_wide_gt_{SetupEngine._MAX_RISK_ATR_MULT}x_atr "
                    f"risk={float(risk):.2f} atr={float(snap.atr_14):.2f}"
                )
        else:
            risk_pct = risk / entry
            if risk_pct > SetupEngine._MAX_RISK_PCT:
                return (
                    f"risk_too_wide_gt_0_5pct "
                    f"risk_pct={float(risk_pct * 100):.2f}%"
                )
        return None

    def _advance_state(
        self,
        setup: Setup,
        snapshot: BarSnapshot,
        market_state: MarketState,
    ) -> tuple[Setup, str]:
        if setup.setup_type == SetupType.VWAP_UNDERCUT_RECLAIM:
            return self._advance_vwap_undercut_reclaim(setup, snapshot, market_state)
        if setup.setup_type == SetupType.SWEEP_RECLAIM:
            return self._advance_sweep_reclaim(setup, snapshot, market_state)
        if setup.setup_type == SetupType.FAKE_BREAKDOWN:
            return self._advance_fake_breakdown(setup, snapshot, market_state)
        if setup.setup_type == SetupType.DEEP_EXHAUSTION_RECLAIM:
            return self._advance_deep_exhaustion_reclaim(setup, snapshot, market_state)
        if setup.setup_type == SetupType.HOD_BREAKOUT:
            return self._advance_hod_breakout(setup, snapshot, market_state)
        if setup.setup_type == SetupType.TREND_PULLBACK:
            return self._advance_trend_pullback(setup, snapshot, market_state)
        if setup.setup_type == SetupType.VWAP_RECLAIM:
            return self._advance_vwap_reclaim(setup, snapshot, market_state)
        if setup.setup_type == SetupType.VWAP_REJECTION:
            return self._advance_vwap_rejection(setup, snapshot, market_state)
        if setup.setup_type == SetupType.VWAP_FAILED_RECLAIM_SHORT:
            return self._advance_vwap_failed_reclaim_short(setup, snapshot, market_state)
        if setup.setup_type == SetupType.TREND_PULLBACK_SHORT:
            return self._advance_trend_pullback_short(setup, snapshot, market_state)
        if setup.setup_type == SetupType.ORB_BREAKOUT:
            return self._advance_orb_breakout(setup, snapshot, market_state)
        if setup.setup_type == SetupType.ORB_BREAKDOWN:
            return self._advance_orb_breakdown(setup, snapshot, market_state)
        if setup.setup_type == SetupType.ONL_SWEEP_RECLAIM_LONG:
            return self._advance_onl_sweep_reclaim_long(setup, snapshot, market_state)
        if setup.setup_type == SetupType.DOUBLE_BOTTOM_RECLAIM_LONG:
            return self._advance_double_bottom_reclaim_long(setup, snapshot, market_state)
        return setup, ""

    # ── Per-type advance methods ───────────────────────────────────────────────

    def _advance_vwap_undercut_reclaim(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED: VWAP cross up, rvol ≥ 1.0, close in upper half, no lower low.
        FORMING timeout: 8 bars or fell deeper below VWAP.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            # Invalidation: price fell further below VWAP after detection
            if snap.bar.low < snap.vwap * Decimal("0.9985"):
                return setup.transition(SetupState.INVALIDATED, "low fell deeper below VWAP", bar_time=snap.timestamp), ""
            # Invalidation: spent too long below VWAP
            if snap.bars_below_vwap > 6:
                return setup.transition(SetupState.INVALIDATED, "extended below VWAP >6 bars", bar_time=snap.timestamp), ""
            if bars > 8:
                return setup.transition(SetupState.INVALIDATED, "forming timeout >8 bars", bar_time=snap.timestamp), ""

            if snap.vwap_cross_up:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.0
                close_pos_ok = snap.bar_close_position_pct is None or snap.bar_close_position_pct >= 0.5
                no_lower_low = not snap.is_lower_low
                if rvol_ok and close_pos_ok and no_lower_low:
                    entry = snap.vwap
                    stop = setup.bar_snapshot.bar.low * Decimal("0.9995")
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = entry + (entry - stop) * mult
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                    )
                    logger.info("Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type, float(entry), float(stop), float(target))
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_sweep_reclaim(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED: hold above OR low for 1–5 bars, VWAP deviation shrinking
        or already above VWAP, no lower low below the sweep candle.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            # Invalidation: broke below the sweep bar's low
            if snap.bar.low < setup.bar_snapshot.bar.low:
                return setup.transition(SetupState.INVALIDATED, "broke below sweep low", bar_time=snap.timestamp), ""
            if bars > 5:
                return setup.transition(SetupState.INVALIDATED, "forming timeout >5 bars", bar_time=snap.timestamp), ""

            # Confirm: holding above OR low, structure recovering
            if snap.orb_low is not None and snap.bar.close > snap.orb_low:
                no_lower_low = not snap.is_lower_low
                recovering = snap.vwap_deviation_shrinking or snap.is_above_vwap
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 0.8
                if no_lower_low and recovering and rvol_ok:
                    entry = snap.bar.high
                    stop = setup.bar_snapshot.bar.low * Decimal("0.9995")
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = entry + (entry - stop) * mult
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                    )
                    logger.info("Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type, float(entry), float(stop), float(target))
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_fake_breakdown(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED (strict): must reclaim VWAP with rvol ≥ 1.2, close above
        OR mid, close ≥ EMA9, no lower low.  Timeout at 12 bars.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            # Invalidation: close breaks below sweep bar low (structural failure),
            # OR wick extends more than 0.05% below sweep bar low (not a tiny noise wick).
            # A single tick wick into the sweep candle's range does NOT invalidate.
            sweep_low = setup.bar_snapshot.bar.low
            if snap.bar.close < sweep_low:
                return setup.transition(SetupState.INVALIDATED, "close_broke_below_sweep_low", bar_time=snap.timestamp), ""
            if snap.bar.low < sweep_low * Decimal("0.9995"):
                return setup.transition(SetupState.INVALIDATED, "wick_broke_sweep_low_by_gt_0_05pct", bar_time=snap.timestamp), ""
            # Invalidation: spending too long below VWAP without reclaiming
            if snap.bars_below_vwap > 8:
                return setup.transition(SetupState.INVALIDATED, "below_vwap_gt_8_bars_no_reclaim", bar_time=snap.timestamp), ""
            if bars > 12:
                return setup.transition(SetupState.INVALIDATED, "forming_timeout_gt_12_bars", bar_time=snap.timestamp), ""

            # Confirm: full VWAP reclaim with all gates
            if snap.vwap_cross_up and snap.is_above_vwap:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.2
                close_pos_ok = snap.bar_close_position_pct is None or snap.bar_close_position_pct >= 0.5
                or_mid_ok = snap.or_mid is None or snap.bar.close > snap.or_mid
                ema_ok = snap.ema_9 is None or snap.bar.close >= snap.ema_9
                no_lower_low = not snap.is_lower_low
                if rvol_ok and close_pos_ok and or_mid_ok and ema_ok and no_lower_low:
                    # Use bar close as entry — realistic fill price on the reclaim candle.
                    # snap.vwap would be an idealized price that rarely fills in practice.
                    entry = snap.bar.close
                    stop = setup.bar_snapshot.bar.low * Decimal("0.9995")
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = entry + (entry - stop) * mult
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                    )
                    logger.info("Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type, float(entry), float(stop), float(target))
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            # Lose the setup if VWAP is abandoned after confirm
            if snap.vwap_cross_down:
                return setup.transition(SetupState.INVALIDATED, "VWAP lost after confirm", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_deep_exhaustion_reclaim(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        """
        FORMING → CONFIRMED: wait ≥3 bars; require no new low, EMA9 reclaim,
        and VWAP deviation shrinking.  Timeout at 15 bars.
        Do not confirm on the first bounce — too many dead-cat moves.
        """
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            # Invalidation: new low below the exhaustion candle
            if snap.is_new_lod and snap.bar.low < setup.bar_snapshot.bar.low:
                return setup.transition(SetupState.INVALIDATED, "new LOD below exhaustion bar", bar_time=snap.timestamp), ""
            # Invalidation: VWAP deviation deepened significantly — sellers still in control
            if snap.vwap_deviation_pct < -0.70:
                return setup.transition(SetupState.INVALIDATED, "VWAP deviation deepened beyond -0.70%", bar_time=snap.timestamp), ""
            if bars > 15:
                return setup.transition(SetupState.INVALIDATED, "forming timeout >15 bars", bar_time=snap.timestamp), ""

            # Confirm: require ≥3 bars to avoid dead-cat bounce
            if bars >= 3:
                no_new_low = not (snap.is_new_lod and snap.bar.low < setup.bar_snapshot.bar.low)
                ema_reclaim = snap.ema_9 is None or snap.bar.close >= snap.ema_9
                shrinking = snap.vwap_deviation_shrinking
                if no_new_low and ema_reclaim and shrinking:
                    entry = snap.bar.high
                    stop = setup.bar_snapshot.bar.low * Decimal("0.9995")
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = entry + (entry - stop) * mult
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={"entry_trigger": entry, "stop_reference": stop, "target_reference": target}
                    )
                    logger.info("Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type, float(entry), float(stop), float(target))
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            # Invalidate if a new low prints after confirmation
            if snap.is_new_lod and snap.bar.low < setup.bar_snapshot.bar.low:
                return setup.transition(SetupState.INVALIDATED, "new LOD after confirmation", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_hod_breakout(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            # Confirm: new HOD + rvol ≥ 1.2
            if snap.is_new_hod and snap.intraday_high is not None:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.2
                if rvol_ok:
                    entry = snap.intraday_high
                    # Use breakout bar's low as stop — tighter and more logical
                    # than vwap-based stop which can be 100+ pts wide on MNQ.
                    stop = snap.bar.low
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = entry + (entry - stop) * mult
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={
                            "entry_trigger": entry,
                            "stop_reference": stop,
                            "target_reference": target,
                        }
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_orb_breakout(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            if snap.orb_high is not None and snap.bar.close < snap.orb_high:
                return setup.transition(SetupState.INVALIDATED, "fell_back_below_orb_high", bar_time=snap.timestamp), ""
            if not snap.is_above_vwap:
                return setup.transition(SetupState.INVALIDATED, "lost_vwap_while_forming", bar_time=snap.timestamp), ""
            if snap.orb_high is not None and snap.bar.close > snap.orb_high:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.0
                if rvol_ok:
                    entry = setup.bar_snapshot.bar.close
                    stop = snap.orb_high * Decimal("0.999")
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    orb_range = (
                        (snap.orb_high - snap.orb_low)
                        if snap.orb_high and snap.orb_low else None
                    )
                    mult = self._target_mult("buy", ms)
                    target = (
                        entry + orb_range * mult
                        if orb_range else entry + (entry - stop) * mult
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={
                            "entry_trigger": entry,
                            "stop_reference": stop,
                            "target_reference": target,
                        }
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            if snap.orb_high is not None and snap.bar.close < snap.orb_high:
                return setup.transition(SetupState.INVALIDATED, "fell_back_below_orb_high", bar_time=snap.timestamp), ""
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    def _advance_trend_pullback(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            # Invalidation: VWAP cross down
            if snap.vwap_cross_down:
                return setup.transition(
                    SetupState.INVALIDATED, "VWAP cross down while forming", bar_time=snap.timestamp
                ), ""
            # Confirm: within 0.25% of VWAP, still above, rvol ≥ 0.8, no lower low
            if (
                snap.is_above_vwap
                and 0 < snap.vwap_deviation_pct <= 0.25
            ):
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 0.8
                no_lower_low = not snap.is_lower_low
                if rvol_ok and no_lower_low:
                    entry = snap.vwap
                    # Stop: confirmation bar's low (unique per bar, tighter and more
                    # relevant than a fixed VWAP percentage across all detections).
                    stop = snap.bar.low
                    rw_reason = self._risk_width_rejection(entry, stop, snap)
                    if rw_reason:
                        return setup.transition(SetupState.INVALIDATED, rw_reason, bar_time=snap.timestamp), ""
                    mult = self._target_mult("buy", ms)
                    target = (
                        snap.intraday_high
                        if snap.intraday_high is not None and snap.intraday_high > entry
                        else entry + (entry - stop) * mult
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED, bar_time=snap.timestamp).model_copy(
                        update={
                            "entry_trigger": entry,
                            "stop_reference": stop,
                            "target_reference": target,
                        }
                    )
                    logger.info(
                        "Setup confirmed: %s %s entry=%.2f stop=%.2f target=%.2f",
                        setup.symbol, setup.setup_type,
                        float(entry), float(stop), float(target),
                    )
                    return confirmed, "confirmed"

        elif setup.state == SetupState.CONFIRMED:
            # Invalidation still applies after confirm
            if snap.vwap_cross_down:
                return setup.transition(
                    SetupState.INVALIDATED, "VWAP cross down while confirmed", bar_time=snap.timestamp
                ), ""
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED, bar_time=snap.timestamp), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED, bar_time=snap.timestamp), "stop hit"

        return setup, ""

    # ── Emit ──────────────────────────────────────────────────────────────────

    async def _emit(
        self, setup: Setup, prev_state: SetupState | None, trigger: BarEvent
    ) -> None:
        event = SetupEvent(
            symbol=setup.symbol,
            timestamp=setup.updated_at,
            metadata=trigger.metadata,
            setup_id=setup.setup_id,
            setup_type=setup.setup_type,
            setup_state=setup.state,
            prev_state=prev_state,
            entry_trigger=setup.entry_trigger,
            stop_reference=setup.stop_reference,
            target_reference=setup.target_reference,
            grade=str(setup.grade) if setup.grade is not None else None,
            score=setup.score,
        )
        await self._event_bus.publish(event)

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _roll_session_if_needed(
        self,
        symbol: str,
        timestamp: datetime,
        trigger: BarEvent,
    ) -> None:
        calendar = self._calendar_for_symbol(symbol)
        session_key = calendar.session_key(timestamp)
        if self._session_keys.get(symbol) == session_key:
            return

        for setup_id, active_setup in list(self._active.get(symbol, {}).items()):
            expired = active_setup.transition(
                SetupState.EXPIRED,
                reason="session_rolled",
                bar_time=timestamp,
            )
            self._record_setup(symbol, expired)
            await self._emit(expired, active_setup.state, trigger)
            self._forming_bars.pop(setup_id, None)

        self._session_keys[symbol] = session_key
        self._active[symbol] = {}
        # Reset per-session cooldown and bar counter on roll.
        self._last_close_bar.pop(symbol, None)
        self._bar_counts[symbol] = 0
        self._bar_count_session[symbol] = session_key

        old_context = self._session_contexts.get(symbol)
        if old_context is not None:
            self._prev_session_contexts[symbol] = old_context

        session_date = calendar.session_date(timestamp)
        self._session_contexts[symbol] = SessionSetupContext(
            symbol=symbol,
            session_key=session_key,
            session_date=session_date.isoformat(),
            session_open=calendar.session_open(session_date),
            session_close=calendar.session_close(session_date),
            session_timezone=calendar.timezone,
        )

    def _record_setup(self, symbol: str, setup: Setup) -> None:
        context = self._session_contexts.get(symbol)
        if context is None:
            return

        entry = self._history_entry_from_setup(setup)
        for index, existing in enumerate(context.setups):
            if existing.setup_id == setup.setup_id:
                context.setups[index] = entry
                break
        else:
            context.setups.append(entry)

        context.last_setup = entry
        context.counts = self._build_counts(context.setups)
        context.counts_by_type = self._build_counts_by_type(context.setups)
        context.counts_by_level = self._build_counts_by_level(context.setups)

    @staticmethod
    def _history_entry_from_setup(setup: Setup) -> SetupHistoryEntry:
        resolved_at = setup.updated_at if setup.state in {
            SetupState.TRIGGERED,
            SetupState.FAILED,
            SetupState.INVALIDATED,
            SetupState.EXPIRED,
        } else None
        _attrs = attrs_for(setup.setup_type)
        return SetupHistoryEntry(
            setup_id=setup.setup_id,
            setup_type=setup.setup_type,
            state=setup.state,
            detected_at=setup.detected_at,
            updated_at=setup.updated_at,
            resolved_at=resolved_at,
            side=_attrs.side if _attrs else SetupEngine._side_for_setup_type(setup.setup_type),
            level_tag=_attrs.anchor.value if _attrs else SetupEngine._level_tag_for_setup_type(setup.setup_type),
            family=_attrs.family if _attrs else None,
            anchor=_attrs.anchor if _attrs else None,
            interaction=_attrs.interaction if _attrs else None,
            display_name=_attrs.display_name if _attrs else None,
            entry_trigger=setup.entry_trigger,
            stop_reference=setup.stop_reference,
            target_reference=setup.target_reference,
            grade=setup.grade,
            score=setup.score,
            structural_grade=setup.structural_grade,
            session_phase=setup.bar_snapshot.session_phase,
            invalidation_reason=setup.invalidation_reason,
            score_reasons=setup.score_reasons,
            score_penalties=setup.score_penalties,
        )

    @staticmethod
    def _build_counts(setups: list[SetupHistoryEntry]) -> dict[str, int]:
        return {
            "detected_total": len(setups),
            "forming_total": sum(entry.state == SetupState.FORMING for entry in setups),
            "confirmed_total": sum(entry.state == SetupState.CONFIRMED for entry in setups),
            "triggered_total": sum(entry.state == SetupState.TRIGGERED for entry in setups),
            "failed_total": sum(entry.state == SetupState.FAILED for entry in setups),
            "invalidated_total": sum(entry.state == SetupState.INVALIDATED for entry in setups),
            "expired_total": sum(entry.state == SetupState.EXPIRED for entry in setups),
        }

    @staticmethod
    def _build_counts_by_type(setups: list[SetupHistoryEntry]) -> dict[str, dict[str, int]]:
        counts: dict[str, dict[str, int]] = {}
        for entry in setups:
            bucket = counts.setdefault(
                str(entry.setup_type),
                {
                    "detected": 0,
                    "forming": 0,
                    "confirmed": 0,
                    "triggered": 0,
                    "failed": 0,
                    "invalidated": 0,
                    "expired": 0,
                },
            )
            bucket["detected"] += 1
            bucket[str(entry.state)] += 1
        return counts

    @staticmethod
    def _build_counts_by_level(setups: list[SetupHistoryEntry]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for entry in setups:
            counts[entry.level_tag] = counts.get(entry.level_tag, 0) + 1
        return counts

    @staticmethod
    def _side_for_setup_type(setup_type: SetupType) -> OrderSide:
        bullish = {
            SetupType.VWAP_RECLAIM,
            SetupType.VWAP_UNDERCUT_RECLAIM,
            SetupType.ORB_BREAKOUT,
            SetupType.SWEEP_RECLAIM,
            SetupType.FAKE_BREAKDOWN,
            SetupType.DEEP_EXHAUSTION_RECLAIM,
            SetupType.HOD_BREAKOUT,
            SetupType.TREND_PULLBACK,
            SetupType.RELATIVE_STRENGTH_BREAKOUT,
        }
        return OrderSide.BUY if setup_type in bullish else OrderSide.SELL

    @staticmethod
    def _level_tag_for_setup_type(setup_type: SetupType) -> str:
        if setup_type == SetupType.HOD_BREAKOUT:
            return "hod"
        if setup_type in {
            SetupType.VWAP_RECLAIM, SetupType.VWAP_REJECTION,
            SetupType.VWAP_UNDERCUT_RECLAIM, SetupType.TREND_PULLBACK,
            SetupType.VWAP_FAILED_RECLAIM_SHORT, SetupType.TREND_PULLBACK_SHORT,
        }:
            return "vwap"
        if setup_type in {SetupType.ORB_BREAKOUT, SetupType.ORB_BREAKDOWN}:
            return "orb"
        if setup_type in {SetupType.SWEEP_RECLAIM, SetupType.FAKE_BREAKDOWN}:
            return "sweep"
        if setup_type == SetupType.DEEP_EXHAUSTION_RECLAIM:
            return "exhaustion"
        if setup_type == SetupType.RELATIVE_STRENGTH_BREAKOUT:
            return "relative_strength"
        return "other"

    def _calendar_for_symbol(self, symbol: str) -> SessionCalendar:
        if symbol not in self._symbol_calendars:
            self._symbol_calendars[symbol] = calendar_for_symbol(self._registry.get(symbol))
        return self._symbol_calendars[symbol]

    def _log_scan_detail(self, message: str, *args: object) -> None:
        if self._settings.runtime.setup_debug:
            logger.info("[setup-debug] " + message, *args)
        else:
            logger.debug(message, *args)

    def _session_allows_detection(self, symbol: str, snapshot: BarSnapshot) -> bool:
        asset_class = self._registry.get(symbol).asset_class
        if asset_class == AssetClass.FUTURE:
            return snapshot.session_phase in _RTH_PHASES
        return snapshot.session_phase in _RTH_PHASES

    def _get_snapshot(self, symbol: str) -> BarSnapshot | None:
        if self._feature_engine is None:
            return None
        from alpha.engines.feature.engine import FeatureEngine
        if isinstance(self._feature_engine, FeatureEngine):
            return self._feature_engine.get_snapshot(symbol)
        return None

    def _get_market_state(self, symbol: str) -> MarketState | None:
        if self._market_state_engine is None:
            return None
        from alpha.engines.market_state.engine import MarketStateEngine
        if isinstance(self._market_state_engine, MarketStateEngine):
            return self._market_state_engine.get_state(symbol)
        return None
