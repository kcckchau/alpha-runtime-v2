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
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import (
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

_RTH_PHASES = frozenset({
    SessionPhase.OPENING_RANGE,
    SessionPhase.EARLY,
    SessionPhase.MID,
    SessionPhase.POWER_HOUR,
    SessionPhase.CLOSING,
})

_SSS_TYPES = frozenset({
    SetupType.FAKE_BREAKDOWN,
    SetupType.HOD_BREAKOUT,
    SetupType.TREND_PULLBACK,
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
        self._session_keys: dict[str, str] = {}
        self._symbol_calendars: dict[str, SessionCalendar] = {}
        # Bars-in-forming counter for invalidation timers
        self._forming_bars: dict[UUID, int] = {}
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

    # ── Handler ───────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return

        symbol = event.symbol
        self._roll_session_if_needed(symbol, event.timestamp)
        snapshot = self._get_snapshot(symbol)
        market_state = self._get_market_state(symbol)
        if snapshot is None or market_state is None:
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
            (SetupType.FAKE_BREAKDOWN, self._detect_fake_breakdown),
            (SetupType.HOD_BREAKOUT, self._detect_hod_breakout),
            (SetupType.TREND_PULLBACK, self._detect_trend_pullback),
            (SetupType.VWAP_RECLAIM, self._detect_vwap_reclaim),
            (SetupType.ORB_BREAKOUT, self._detect_orb_breakout),
            (SetupType.SWEEP_RECLAIM, self._detect_sweep_reclaim),
        ]
        for setup_type, detector in detectors:
            if self._has_active(symbol, setup_type):
                continue
            if detector(snapshot, market_state):
                await self._open_setup(symbol, setup_type, snapshot, market_state, trigger)

    def _has_active(self, symbol: str, setup_type: SetupType) -> bool:
        return any(
            s.setup_type == setup_type
            for s in self._active.get(symbol, {}).values()
        )

    # ── SSS detectors ─────────────────────────────────────────────────────────

    def _detect_fake_breakdown(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        FORMING: at least 1 bar below VWAP, low near VWAP (within 0.1%).
        MNQ opposed → blocked (not yet implemented; skipped until MNQ lead is wired).
        """
        if snap.session_phase not in _RTH_PHASES:
            return False
        if snap.bars_below_vwap < 1:
            return False
        # Low must be at or near VWAP (within 0.1% above)
        if snap.bar.low > snap.vwap * Decimal("1.001"):
            return False
        return True

    def _detect_hod_breakout(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        Prereq: intraday_high > orb_high, price above VWAP.
        FORMING: higher highs, within 0.2% of intraday high.
        MNQ opposed → blocked (skipped until MNQ lead is wired).
        """
        if snap.session_phase not in _RTH_PHASES:
            return False
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

    def _detect_trend_pullback(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """
        FORMING: ≥5 consecutive bars above VWAP, pulling back (deviation shrinking),
        within 0.5% of VWAP.
        MNQ opposed → blocked (skipped until MNQ lead is wired).
        """
        if snap.session_phase not in _RTH_PHASES:
            return False
        if snap.bars_above_vwap < 5:
            return False
        if not snap.vwap_deviation_shrinking:
            return False
        # Within 0.5% of VWAP (and still above it — deviation positive)
        if snap.vwap_deviation_pct <= 0 or snap.vwap_deviation_pct > 0.5:
            return False
        return True

    # ── Stub detectors (not yet implemented) ─────────────────────────────────

    def _detect_vwap_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return False

    def _detect_orb_breakout(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return False

    def _detect_sweep_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        return False

    # ── State machine ─────────────────────────────────────────────────────────

    async def _open_setup(
        self,
        symbol: str,
        setup_type: SetupType,
        snapshot: BarSnapshot,
        market_state: MarketState,
        trigger: BarEvent,
    ) -> None:
        now = datetime.now(timezone.utc)
        grade = SetupGrade.SSS if setup_type in _SSS_TYPES else None
        setup = Setup(
            symbol=symbol,
            setup_type=setup_type,
            state=SetupState.FORMING,
            detected_at=now,
            updated_at=now,
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
            self._active[symbol].pop(sid, None)
            self._forming_bars.pop(sid, None)

    def _advance_state(
        self,
        setup: Setup,
        snapshot: BarSnapshot,
        market_state: MarketState,
    ) -> tuple[Setup, str]:
        if setup.setup_type == SetupType.FAKE_BREAKDOWN:
            return self._advance_fake_breakdown(setup, snapshot)
        if setup.setup_type == SetupType.HOD_BREAKOUT:
            return self._advance_hod_breakout(setup, snapshot)
        if setup.setup_type == SetupType.TREND_PULLBACK:
            return self._advance_trend_pullback(setup, snapshot)
        return setup, ""

    # ── Per-type advance methods ───────────────────────────────────────────────

    def _advance_fake_breakdown(
        self, setup: Setup, snap: BarSnapshot
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            bars = self._forming_bars.get(setup.setup_id, 0) + 1
            self._forming_bars[setup.setup_id] = bars

            # Invalidation: below VWAP for >15 bars
            if bars > 15 and not snap.is_above_vwap:
                return setup.transition(SetupState.INVALIDATED, "below VWAP >15 bars"), ""

            # Confirm: VWAP cross up + rvol ≥ 1.2 + close ≥ 50% of bar + close > OR mid
            if snap.vwap_cross_up:
                rvol_ok = snap.relative_volume is None or snap.relative_volume >= 1.2
                close_pos_ok = (
                    snap.bar_close_position_pct is None
                    or snap.bar_close_position_pct >= 0.5
                )
                or_mid_ok = snap.or_mid is None or snap.bar.close > snap.or_mid
                if rvol_ok and close_pos_ok and or_mid_ok:
                    entry = snap.vwap
                    # Stop: sweep low (FORMING bar's low) × 0.9995
                    stop = setup.bar_snapshot.bar.low * Decimal("0.9995")
                    target = entry + (entry - stop) * Decimal("2.5")
                    confirmed = setup.transition(SetupState.CONFIRMED).model_copy(
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
                return setup.transition(SetupState.TRIGGERED), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED), "stop hit"

        return setup, ""

    def _advance_hod_breakout(
        self, setup: Setup, snap: BarSnapshot
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
                    stop = snap.vwap * Decimal("0.999")
                    target = entry + (entry - stop) * Decimal("2")
                    confirmed = setup.transition(SetupState.CONFIRMED).model_copy(
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
                return setup.transition(SetupState.TRIGGERED), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED), "stop hit"

        return setup, ""

    def _advance_trend_pullback(
        self, setup: Setup, snap: BarSnapshot
    ) -> tuple[Setup, str]:
        if setup.state == SetupState.FORMING:
            self._forming_bars[setup.setup_id] = (
                self._forming_bars.get(setup.setup_id, 0) + 1
            )
            # Invalidation: VWAP cross down
            if snap.vwap_cross_down:
                return setup.transition(
                    SetupState.INVALIDATED, "VWAP cross down while forming"
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
                    stop = snap.vwap * Decimal("0.997")
                    target = (
                        snap.intraday_high
                        if snap.intraday_high is not None
                        else entry + (entry - stop) * Decimal("3")
                    )
                    confirmed = setup.transition(SetupState.CONFIRMED).model_copy(
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
                    SetupState.INVALIDATED, "VWAP cross down while confirmed"
                ), ""
            if setup.entry_trigger and snap.bar.high >= setup.entry_trigger:
                return setup.transition(SetupState.TRIGGERED), "triggered"
            if setup.stop_reference and snap.bar.low <= setup.stop_reference:
                return setup.transition(SetupState.FAILED), "stop hit"

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
            SetupType.ORB_BREAKOUT,
            SetupType.SWEEP_RECLAIM,
            SetupType.FAKE_BREAKDOWN,
            SetupType.HOD_BREAKOUT,
            SetupType.TREND_PULLBACK,
            SetupType.RELATIVE_STRENGTH_BREAKOUT,
        }
        return OrderSide.BUY if setup_type in bullish else OrderSide.SELL

    @staticmethod
    def _level_tag_for_setup_type(setup_type: SetupType) -> str:
        if setup_type == SetupType.HOD_BREAKOUT:
            return "hod"
        if setup_type in {SetupType.VWAP_RECLAIM, SetupType.VWAP_REJECTION, SetupType.FAKE_BREAKDOWN, SetupType.TREND_PULLBACK}:
            return "vwap"
        if setup_type in {SetupType.ORB_BREAKOUT, SetupType.ORB_BREAKDOWN}:
            return "orb"
        if setup_type == SetupType.SWEEP_RECLAIM:
            return "sweep"
        if setup_type == SetupType.RELATIVE_STRENGTH_BREAKOUT:
            return "relative_strength"
        return "other"

    def _calendar_for_symbol(self, symbol: str) -> SessionCalendar:
        if symbol not in self._symbol_calendars:
            self._symbol_calendars[symbol] = calendar_for_symbol(self._registry.get(symbol))
        return self._symbol_calendars[symbol]

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
