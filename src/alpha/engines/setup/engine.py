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
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import (
    AssetClass,
    EventType,
    HealthStatus,
    ORBState,
    OrderSide,
    SessionPhase,
    SetupGrade,
    SetupState,
    SetupType,
)
from alpha.models.events import AnyEvent, BarEvent, SetupEvent
from alpha.models.market_state import MarketState
from alpha.models.setup import SessionSetupContext, Setup, SetupHistoryEntry
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

# SSS = highest-conviction grade; assigned at detection time.
# FAKE_BREAKDOWN is the only new structural-sweep setup at SSS — it requires
# both an OR-low sweep and a full VWAP reclaim before confirmation.
_SSS_TYPES = frozenset({
    SetupType.FAKE_BREAKDOWN,
    SetupType.HOD_BREAKOUT,
    SetupType.TREND_PULLBACK,
    SetupType.VWAP_RECLAIM,
    SetupType.VWAP_REJECTION,
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
        self._setups_detected: int = 0
        self._setups_triggered: int = 0

    @property
    def name(self) -> str:
        return "SetupEngine"

    def set_feature_engine(self, engine: object) -> None:
        self._feature_engine = engine

    def set_market_state_engine(self, engine: object) -> None:
        self._market_state_engine = engine

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

    def prev_session_setup_context(self, symbol: str) -> SessionSetupContext | None:
        return self._prev_session_contexts.get(symbol)

    # ── Handler ───────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return

        symbol = event.symbol
        session_key = self._session_keys.get(symbol, "")
        self._roll_session_if_needed(symbol, event.timestamp)

        # Increment per-symbol bar counter; reset on new session.
        if self._bar_count_session.get(symbol) != session_key:
            self._bar_count_session[symbol] = session_key
            self._bar_counts[symbol] = 0
            self._last_close_bar.pop(symbol, None)
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
            # ORB family
            (SetupType.ORB_BREAKOUT, self._detect_orb_breakout),
            (SetupType.ORB_BREAKDOWN, self._detect_orb_breakdown),
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
        FORMING: swept OR low shallow (≤0.3%), closed back above with conviction.
        CONFIRMED requires a full VWAP reclaim — this is the SSS gate.
        """
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
        FORMING: deep VWAP deviation (≤−0.35%), new session low, high volume
        capitulation candle that closes off its lows.
        CONFIRMED only after 3+ bars with no new low and VWAP deviation shrinking.
        """
        if snap.vwap_deviation_pct > -0.35:
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

    def _advance_orb_breakdown(
        self, setup: Setup, snap: BarSnapshot, ms: MarketState
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            # Invalidation: reclaimed ORB low or VWAP
            if snap.orb_low is not None and snap.bar.close > snap.orb_low:
                return setup.transition(SetupState.INVALIDATED, "ORB low reclaimed", bar_time=snap.timestamp), ""
            if snap.is_above_vwap:
                return setup.transition(SetupState.INVALIDATED, "reclaimed VWAP", bar_time=snap.timestamp), ""
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
            # Invalidation: price reclaims ORB low
            if snap.orb_low is not None and snap.bar.close > snap.orb_low:
                return setup.transition(SetupState.INVALIDATED, "ORB low reclaimed while confirmed", bar_time=snap.timestamp), ""
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

    def _detect_vwap_rejection(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        FORMING: price crossed below VWAP from at least 2 bars above, closing
        with conviction (>0.05% below VWAP, lower half of bar).
        """
        if not snap.vwap_cross_down:
            return False
        if snap.vwap_cross_down_after_bars < 2:
            return False
        if snap.vwap_deviation_pct > -0.05:
            return False
        if snap.bar_close_position_pct is not None and snap.bar_close_position_pct > 0.5:
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
        return None

    def _detect_orb_breakdown(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        FORMING: price breaks below ORB low, below VWAP, setting a new LOD.
        """
        if snap.orb_state != ORBState.BREAKOUT_DOWN or snap.orb_low is None:
            return False
        if snap.is_above_vwap:
            return False
        if not snap.is_new_lod:
            return False
        return True

    def _reason_orb_breakdown(self, snap: BarSnapshot, ms: MarketState) -> str | None:
        if snap.orb_state != ORBState.BREAKOUT_DOWN or snap.orb_low is None:
            return "orb_not_in_breakdown"
        if snap.is_above_vwap:
            return "above_vwap"
        if not snap.is_new_lod:
            return "not_new_lod"
        return None

    # ── Stub detectors (not yet implemented) ─────────────────────────────────

    def _detect_orb_breakout(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return False

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
        if setup_type == SetupType.ORB_BREAKDOWN:
            return self._reason_orb_breakdown(snapshot, market_state)
        if setup_type == SetupType.ORB_BREAKOUT:
            return "not_implemented"
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
        grade = SetupGrade.SSS if setup_type in _SSS_TYPES else None
        setup = Setup(
            symbol=symbol,
            setup_type=setup_type,
            state=SetupState.FORMING,
            detected_at=bar_time,
            updated_at=bar_time,
            market_state=market_state,
            bar_snapshot=snapshot,
            grade=grade,
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
        if setup.setup_type == SetupType.ORB_BREAKDOWN:
            return self._advance_orb_breakdown(setup, snapshot, market_state)
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

            # Invalidation: broke below the sweep bar's low
            if snap.bar.low < setup.bar_snapshot.bar.low:
                return setup.transition(SetupState.INVALIDATED, "broke below sweep low", bar_time=snap.timestamp), ""
            # Invalidation: spending too long below VWAP without reclaiming
            if snap.bars_below_vwap > 8:
                return setup.transition(SetupState.INVALIDATED, "below VWAP >8 bars without reclaim", bar_time=snap.timestamp), ""
            if bars > 12:
                return setup.transition(SetupState.INVALIDATED, "forming timeout >12 bars", bar_time=snap.timestamp), ""

            # Confirm: full VWAP reclaim with all gates
            if snap.vwap_cross_up and snap.is_above_vwap:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.2
                close_pos_ok = snap.bar_close_position_pct is None or snap.bar_close_position_pct >= 0.5
                or_mid_ok = snap.or_mid is None or snap.bar.close > snap.or_mid
                ema_ok = snap.ema_9 is None or snap.bar.close >= snap.ema_9
                no_lower_low = not snap.is_lower_low
                if rvol_ok and close_pos_ok and or_mid_ok and ema_ok and no_lower_low:
                    entry = snap.vwap
                    stop = setup.bar_snapshot.bar.low * Decimal("0.9995")
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
        )
        await self._event_bus.publish(event)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _roll_session_if_needed(self, symbol: str, timestamp: datetime) -> None:
        calendar = self._calendar_for_symbol(symbol)
        session_key = calendar.session_key(timestamp)
        if self._session_keys.get(symbol) == session_key:
            return

        self._session_keys[symbol] = session_key
        stale_ids = list(self._active.get(symbol, {}).keys())
        self._active[symbol] = {}
        for setup_id in stale_ids:
            self._forming_bars.pop(setup_id, None)
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
        return SetupHistoryEntry(
            setup_id=setup.setup_id,
            setup_type=setup.setup_type,
            state=setup.state,
            detected_at=setup.detected_at,
            updated_at=setup.updated_at,
            resolved_at=resolved_at,
            side=SetupEngine._side_for_setup_type(setup.setup_type),
            level_tag=SetupEngine._level_tag_for_setup_type(setup.setup_type),
            entry_trigger=setup.entry_trigger,
            stop_reference=setup.stop_reference,
            target_reference=setup.target_reference,
            grade=setup.grade,
            score=setup.score,
            session_phase=setup.bar_snapshot.session_phase,
            invalidation_reason=setup.invalidation_reason,
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
            return snapshot.session_phase != SessionPhase.CLOSED
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
