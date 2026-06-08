"""
Engine 5 — Market State Engine

Responsibilities:
  - Subscribe to BarEvent from the EventBus
  - Consume BarSnapshot from the FeatureEngine after each bar
  - Classify price action: trend, chop, VWAP state, ORB state, session structure
  - Detect lead-lag relationships between correlated symbols
  - Emit MarketStateEvent to the EventBus

Input:  BarEvent (trigger), BarSnapshot (from FeatureEngine.get_snapshot)
Output: MarketState → serialized into MarketStateEvent

Runs per-symbol on every bar. All classification is stateless over the
BarSnapshot + recent bar history — no open position state.
"""

from __future__ import annotations

import logging

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import DayType, EventType, HealthStatus, ORBState, TrendState, VWAPState
from alpha.models.events import AnyEvent, BarEvent, MarketStateEvent
from alpha.models.market_state import MarketState
from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)


class MarketStateEngine(BaseEngine):
    """
    Classifies market structure after each bar.

    Depends on FeatureEngine being initialized first so snapshots are
    available when BarEvents arrive.

    Usage::

        # After BootstrapEngine wires dependencies:
        # market_state_engine._feature = feature_engine
        engine.set_feature_engine(feature_engine)
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
        self._feature_engine: object | None = None   # FeatureEngine, avoid circular import
        self._latest_states: dict[str, MarketState] = {}
        self._classifications_total: int = 0
        # Day type is locked once ORB is established; reset each session
        self._day_types: dict[str, DayType] = {}
        self._day_type_session: dict[str, str] = {}

    @property
    def name(self) -> str:
        return "MarketStateEngine"

    def set_feature_engine(self, feature_engine: object) -> None:
        self._feature_engine = feature_engine

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        pass

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.BAR, self._handle_bar)

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        return EngineHealth(
            HealthStatus.HEALTHY,
            self.name,
            {"classifications_total": self._classifications_total},
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def get_state(self, symbol: str) -> MarketState | None:
        return self._latest_states.get(symbol)

    # ── Handler ───────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return

        snapshot = self._get_snapshot(event.symbol)
        if snapshot is None:
            return

        state = self._classify(event.symbol, snapshot)
        self._latest_states[event.symbol] = state
        self._classifications_total += 1
        await self._emit(state, event)

    # ── Classification ────────────────────────────────────────────────────────

    def _classify(self, symbol: str, snap: BarSnapshot) -> MarketState:
        trend = self._classify_trend(snap)
        vwap_state = self._classify_vwap(snap)
        structure_score = self._score_structure(snap, trend)
        day_type = self._classify_day_type(symbol, snap, trend)

        return MarketState(
            symbol=symbol,
            timestamp=snap.timestamp,
            trend=trend,
            trend_strength=min(1.0, abs(snap.vwap_deviation_pct) / 5.0),
            vwap_state=vwap_state,
            orb_state=snap.orb_state,
            session_phase=snap.session_phase,
            is_extended=snap.is_extended,
            structure_score=structure_score,
            confidence=self._confidence(snap),
            day_type=day_type,
        )

    def _classify_day_type(self, symbol: str, snap: BarSnapshot, trend: TrendState) -> DayType:
        # Reset on new session
        session_key = snap.timestamp.strftime("%Y-%m-%d")
        if self._day_type_session.get(symbol) != session_key:
            self._day_type_session[symbol] = session_key
            self._day_types.pop(symbol, None)

        # Return locked value if already determined
        if symbol in self._day_types:
            return self._day_types[symbol]

        # Need ORB established and enough bars to classify
        if snap.orb_state == ORBState.NOT_SET or snap.bars_since_open < 15:
            return DayType.UNKNOWN

        if snap.orb_state == ORBState.BREAKOUT_UP and trend == TrendState.TRENDING_UP:
            day_type = DayType.TREND_UP
        elif snap.orb_state == ORBState.BREAKOUT_DOWN and trend == TrendState.TRENDING_DOWN:
            day_type = DayType.TREND_DOWN
        elif snap.orb_state in {ORBState.INSIDE, ORBState.FAILED_BREAKOUT_UP, ORBState.FAILED_BREAKOUT_DOWN}:
            day_type = DayType.RANGE
        else:
            day_type = DayType.BALANCED

        self._day_types[symbol] = day_type
        logger.info("Day type locked: %s → %s (orb=%s trend=%s)", symbol, day_type, snap.orb_state, trend)
        return day_type

    @staticmethod
    def _classify_trend(snap: BarSnapshot) -> TrendState:
        if snap.ema_9 is None or snap.ema_20 is None:
            return TrendState.UNKNOWN
        if snap.ema_9 > snap.ema_20 and snap.bar.close > snap.ema_9:
            return TrendState.TRENDING_UP
        if snap.ema_9 < snap.ema_20 and snap.bar.close < snap.ema_9:
            return TrendState.TRENDING_DOWN
        return TrendState.CHOPPY

    @staticmethod
    def _classify_vwap(snap: BarSnapshot) -> VWAPState:
        if snap.bar.close > snap.vwap:
            return VWAPState.ABOVE
        return VWAPState.BELOW

    @staticmethod
    def _score_structure(snap: BarSnapshot, trend: TrendState) -> float:
        score = 0.5
        if trend in {TrendState.TRENDING_UP, TrendState.TRENDING_DOWN}:
            score += 0.3
        if snap.relative_volume and snap.relative_volume > 1.2:
            score += 0.2
        return min(1.0, score)

    @staticmethod
    def _confidence(snap: BarSnapshot) -> float:
        if snap.bars_since_open < 5:
            return 0.3
        if snap.bars_since_open < 15:
            return 0.6
        return 0.85

    # ── Emit ──────────────────────────────────────────────────────────────────

    async def _emit(self, state: MarketState, trigger: BarEvent) -> None:
        event = MarketStateEvent(
            symbol=state.symbol,
            timestamp=state.timestamp,
            metadata=trigger.metadata,
            state_data=state.model_dump(),
        )
        await self._event_bus.publish(event)

    def _get_snapshot(self, symbol: str) -> BarSnapshot | None:
        if self._feature_engine is None:
            return None
        from alpha.engines.feature.engine import FeatureEngine
        if isinstance(self._feature_engine, FeatureEngine):
            return self._feature_engine.get_snapshot(symbol)
        return None
