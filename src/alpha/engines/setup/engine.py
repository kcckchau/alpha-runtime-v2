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

Each setup type (VWAP reclaim, ORB breakout, etc.) has its own detector
function that returns True when conditions are met. The engine drives
the state machine.

Input:  BarEvent (trigger), BarSnapshot + MarketState (from feature/market_state engines)
Output: SetupEvent
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import EventType, HealthStatus, SetupState, SetupType
from alpha.models.events import AnyEvent, BarEvent, SetupEvent
from alpha.models.market_state import MarketState
from alpha.models.setup import Setup
from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)


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
        for ticker in self._registry.tickers():
            self._active[ticker] = {}

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

    # ── Handler ───────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return

        symbol = event.symbol
        snapshot = self._get_snapshot(symbol)
        market_state = self._get_market_state(symbol)
        if snapshot is None or market_state is None:
            return

        # Scan for new setups
        await self._scan_for_setups(symbol, snapshot, market_state, event)

        # Update existing setups
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
            (SetupType.VWAP_RECLAIM, self._detect_vwap_reclaim),
            (SetupType.ORB_BREAKOUT, self._detect_orb_breakout),
            (SetupType.SWEEP_RECLAIM, self._detect_sweep_reclaim),
            (SetupType.FAKE_BREAKDOWN, self._detect_fake_breakdown),
        ]
        for setup_type, detector in detectors:
            if detector(snapshot, market_state):
                await self._open_setup(symbol, setup_type, snapshot, market_state, trigger)

    def _detect_vwap_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """Price crossed above VWAP on volume with clean close."""
        # TODO: implement full conditions
        return False

    def _detect_orb_breakout(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """Price broke above ORB high with volume confirmation."""
        from alpha.models.enums import ORBState
        if snap.orb_state != ORBState.BREAKOUT_UP:
            return False
        # TODO: add volume confirmation, clean close, no extended conditions
        return False

    def _detect_sweep_reclaim(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """Price swept below a level then immediately reclaimed it."""
        # TODO: implement
        return False

    def _detect_fake_breakdown(self, snap: BarSnapshot, ms: MarketState) -> bool:
        """Price broke a support level, then closed back above it."""
        # TODO: implement
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
        setup = Setup(
            symbol=symbol,
            setup_type=setup_type,
            state=SetupState.FORMING,
            detected_at=now,
            updated_at=now,
            market_state=market_state,
            bar_snapshot=snapshot,
        )
        self._active.setdefault(symbol, {})[setup.setup_id] = setup
        self._setups_detected += 1
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
            updated, reason = self._advance_state(setup, snapshot, market_state)
            if updated.state != setup.state:
                self._active[symbol][setup_id] = updated
                await self._emit(updated, setup.state, trigger)
                if updated.state == SetupState.TRIGGERED:
                    self._setups_triggered += 1
            if updated.state in {SetupState.FAILED, SetupState.INVALIDATED, SetupState.EXPIRED}:
                to_remove.append(setup_id)
        for sid in to_remove:
            self._active[symbol].pop(sid, None)

    def _advance_state(
        self,
        setup: Setup,
        snapshot: BarSnapshot,
        market_state: MarketState,
    ) -> tuple[Setup, str]:
        """Return (updated_setup, reason). No-op if state unchanged."""
        # TODO: implement per-setup-type advancement logic
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
