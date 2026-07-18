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

Day type classification:
  - Requires ≥30 RTH bars (≈10:00 ET) and ORB established
  - Additionally requires VWAP and structure to be aligned with ORB direction
  - Must see the same candidate type for 3 consecutive bars before locking
  - Once locked, day_type stays fixed for the session but day_type_status
    and confidence update every bar to reflect whether price action is still
    consistent with the morning narrative

Live bias:
  - Recalculated every bar, never locks
  - Drives trade_permission together with day_type_status
"""

from __future__ import annotations

import logging

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import (
    DayType,
    DayTypeStatus,
    EventType,
    HealthStatus,
    LiveBias,
    TrendState,
    VWAPState,
)
from alpha.models.events import AnyEvent, BarBundleEvent, BarEvent, MarketStateEvent
from alpha.models.market_state import MarketState
from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)

# Minimum RTH bars before we are willing to lock day type (~10:00 ET on 1m)
_DAY_TYPE_MIN_BARS = 30
# Number of consecutive bars that must agree before locking
_DAY_TYPE_CONFIRMATION_WIDTH = 3

# Confidence thresholds for DayTypeStatus transitions
_THRESHOLD_HEALTHY = 0.55
_THRESHOLD_STRESSED = 0.30


class MarketStateEngine(BaseEngine):
    """
    Classifies market structure after each bar.

    Depends on FeatureEngine being initialized first so snapshots are
    available when BarEvents arrive.

    Usage::

        # After BootstrapEngine wires dependencies:
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
        self._context_engine: object | None = None   # ContextEngine, avoid circular import
        self._latest_states: dict[str, MarketState] = {}
        self._classifications_total: int = 0

        # Day type lock state — reset each session
        self._day_types: dict[str, DayType] = {}
        self._day_type_session: dict[str, str] = {}
        # Sliding window of recent candidates (up to _DAY_TYPE_CONFIRMATION_WIDTH)
        self._day_type_candidates: dict[str, list[DayType]] = {}

        # Consecutive-trend-bar tracking
        self._trend_bars: dict[str, int] = {}
        self._trend_bars_session: dict[str, str] = {}
        self._last_trend: dict[str, TrendState] = {}
        self._pipeline_mode: bool = False  # set True by BarPipeline before engine.start()

    @property
    def name(self) -> str:
        return "MarketStateEngine"

    def set_feature_engine(self, feature_engine: object) -> None:
        self._feature_engine = feature_engine

    def set_context_engine(self, context_engine: object) -> None:
        self._context_engine = context_engine

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

    # ── Public pipeline API ───────────────────────────────────────────────────

    async def process_bar(self, snap: BarSnapshot, bundle: BarBundleEvent) -> MarketState:
        """
        Stage 2 of the sequential pipeline.

        Called by BarPipeline with the snapshot already computed by FeatureEngine.
        Classifies market structure and emits a MarketStateEvent.
        """
        state = self._classify(snap.symbol, snap)
        self._latest_states[snap.symbol] = state
        self._classifications_total += 1
        await self._emit(state, bundle.to_bar_event())
        return state

    # ── Handler ───────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return
        if self._pipeline_mode:
            return  # BarPipeline owns M1 processing
        snapshot = self._get_snapshot(event.symbol)
        if snapshot is None:
            return

        state = self._classify(event.symbol, snapshot)
        self._latest_states[event.symbol] = state
        self._classifications_total += 1
        await self._emit(state, event)

    # ── Classification ────────────────────────────────────────────────────────

    def _classify(self, symbol: str, snap: BarSnapshot) -> MarketState:
        session_key = snap.timestamp.strftime("%Y-%m-%d")

        trend = self._classify_trend(snap)
        trend_bars = self._track_trend_bars(symbol, session_key, trend)
        vwap_state = self._classify_vwap(snap)
        structure_score = self._score_structure(snap, trend)
        day_type = self._classify_day_type(symbol, session_key, snap, trend, vwap_state)
        live_bias = self._classify_live_bias(snap, trend, vwap_state)
        confidence = self._compute_day_type_confidence(day_type, snap, trend, vwap_state)
        day_type_status = self._compute_day_type_status(day_type, confidence)
        long_ok, short_ok, permission_reason = self._compute_trade_permission(
            day_type, day_type_status, live_bias
        )

        return MarketState(
            symbol=symbol,
            timestamp=snap.timestamp,
            trend=trend,
            trend_strength=min(1.0, abs(snap.vwap_deviation_pct) / 5.0),
            trend_bars=trend_bars,
            vwap_state=vwap_state,
            or_position=snap.or_position,
            session_phase=snap.session_phase,
            is_extended=snap.is_extended,
            structure_score=structure_score,
            confidence=confidence,
            day_type=day_type,
            day_type_status=day_type_status,
            live_bias=live_bias,
            trade_long_allowed=long_ok,
            trade_short_allowed=short_ok,
            trade_permission_reason=permission_reason,
        )

    # ── Day type classification (locks, but status/confidence evolve) ──────────

    def _classify_day_type(
        self,
        symbol: str,
        session_key: str,
        snap: BarSnapshot,
        trend: TrendState,
        vwap_state: VWAPState,
    ) -> DayType:
        # Reset all per-session state on a new calendar date
        if self._day_type_session.get(symbol) != session_key:
            self._day_type_session[symbol] = session_key
            self._day_types.pop(symbol, None)
            self._day_type_candidates.pop(symbol, None)

        # Return the locked value for the rest of the session
        if symbol in self._day_types:
            return self._day_types[symbol]

        # Gate: need OR established and enough RTH bars
        if not snap.or_established or snap.bars_since_open < _DAY_TYPE_MIN_BARS:
            return DayType.UNKNOWN

        # Determine candidate using OR direction, VWAP alignment, and trend
        above_vwap = vwap_state in {VWAPState.ABOVE, VWAPState.RECLAIMING}
        below_vwap = vwap_state in {VWAPState.BELOW, VWAPState.REJECTING}

        if snap.or_position == "ABOVE":
            if above_vwap and trend == TrendState.TRENDING_UP:
                candidate = DayType.TREND_UP
            else:
                # OR broke up but VWAP or trend not yet aligned — inconclusive
                candidate = DayType.BALANCED
        elif snap.or_position == "BELOW":
            if below_vwap and trend == TrendState.TRENDING_DOWN:
                candidate = DayType.TREND_DOWN
            else:
                candidate = DayType.BALANCED
        elif snap.or_established and snap.or_position == "INSIDE":
            candidate = DayType.RANGE
        else:
            candidate = DayType.BALANCED

        # Accumulate candidates; require _DAY_TYPE_CONFIRMATION_WIDTH consecutive agreement
        window = self._day_type_candidates.setdefault(symbol, [])
        window.append(candidate)
        if len(window) > _DAY_TYPE_CONFIRMATION_WIDTH:
            window.pop(0)

        if len(window) == _DAY_TYPE_CONFIRMATION_WIDTH and len(set(window)) == 1:
            day_type = window[0]
            self._day_types[symbol] = day_type
            self._day_type_candidates.pop(symbol, None)
            logger.info(
                "Day type locked: %s → %s (orb=%s trend=%s vwap=%s bars=%d)",
                symbol, day_type, snap.or_position, trend, vwap_state, snap.bars_since_open,
            )
            return day_type

        return DayType.UNKNOWN

    # ── Trend ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _classify_trend(snap: BarSnapshot) -> TrendState:
        if snap.ema_9 is None or snap.ema_21 is None:
            return TrendState.UNKNOWN
        if snap.ema_9 > snap.ema_21 and snap.bar.close > snap.ema_9:
            return TrendState.TRENDING_UP
        if snap.ema_9 < snap.ema_21 and snap.bar.close < snap.ema_9:
            return TrendState.TRENDING_DOWN
        return TrendState.CHOPPY

    def _track_trend_bars(self, symbol: str, session_key: str, trend: TrendState) -> int:
        if self._trend_bars_session.get(symbol) != session_key:
            self._trend_bars_session[symbol] = session_key
            self._trend_bars[symbol] = 0
            self._last_trend.pop(symbol, None)

        if self._last_trend.get(symbol) == trend:
            self._trend_bars[symbol] = self._trend_bars.get(symbol, 0) + 1
        else:
            self._trend_bars[symbol] = 1
            self._last_trend[symbol] = trend

        return self._trend_bars[symbol]

    # ── VWAP ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _classify_vwap(snap: BarSnapshot) -> VWAPState:
        # Use cross signals when available for finer granularity
        if snap.vwap_cross_up:
            return VWAPState.RECLAIMING
        if snap.vwap_cross_down:
            return VWAPState.REJECTING
        if snap.bar.close > snap.vwap:
            return VWAPState.ABOVE
        return VWAPState.BELOW

    # ── Live bias (per-bar, never locks) ──────────────────────────────────────

    @staticmethod
    def _classify_live_bias(snap: BarSnapshot, trend: TrendState, vwap_state: VWAPState) -> LiveBias:
        if trend == TrendState.UNKNOWN:
            return LiveBias.UNKNOWN

        above_vwap = vwap_state in {VWAPState.ABOVE, VWAPState.RECLAIMING}

        if trend == TrendState.TRENDING_UP and above_vwap:
            return LiveBias.BULLISH
        if trend == TrendState.TRENDING_DOWN and not above_vwap:
            return LiveBias.BEARISH
        if trend == TrendState.TRENDING_DOWN and above_vwap:
            # Trending down but still above VWAP — bearish pressure building
            return LiveBias.TRANSITIONING_BEARISH
        if trend == TrendState.TRENDING_UP and not above_vwap:
            # Trending up but below VWAP — bullish momentum below resistance
            return LiveBias.TRANSITIONING_BULLISH
        return LiveBias.NEUTRAL

    # ── Dynamic day-type confidence ────────────────────────────────────────────

    @staticmethod
    def _compute_day_type_confidence(
        locked_type: DayType,
        snap: BarSnapshot,
        trend: TrendState,
        vwap_state: VWAPState,
    ) -> float:
        above_vwap = vwap_state in {VWAPState.ABOVE, VWAPState.RECLAIMING}

        if locked_type == DayType.UNKNOWN:
            # Pre-lock: reflect progress toward eligibility
            if snap.bars_since_open < 5:
                return 0.20
            if snap.bars_since_open < 15:
                return 0.35
            if snap.bars_since_open < _DAY_TYPE_MIN_BARS:
                return 0.50
            return 0.60

        if locked_type == DayType.TREND_UP:
            score = 0.0
            if trend == TrendState.TRENDING_UP:
                score += 0.40
            elif trend == TrendState.CHOPPY:
                score += 0.10
            # TRENDING_DOWN contributes 0 — actively contradicts
            if above_vwap:
                score += 0.25
            if snap.is_higher_high:
                score += 0.20
            if not snap.is_new_lod:
                score += 0.15
            return min(1.0, score)

        if locked_type == DayType.TREND_DOWN:
            score = 0.0
            if trend == TrendState.TRENDING_DOWN:
                score += 0.40
            elif trend == TrendState.CHOPPY:
                score += 0.10
            if not above_vwap:
                score += 0.25
            if snap.is_new_lod:
                score += 0.20
            if not snap.is_higher_high:
                score += 0.15
            return min(1.0, score)

        if locked_type == DayType.RANGE:
            score = 0.0
            if trend == TrendState.CHOPPY:
                score += 0.40
            if snap.or_established and snap.or_position == "INSIDE":
                score += 0.35
            # Tight VWAP deviation suggests range-bound
            score += 0.25 * max(0.0, 1.0 - abs(snap.vwap_deviation_pct) / 3.0)
            return min(1.0, score)

        # BALANCED — mild base conviction, improves with chop
        score = 0.40
        if trend == TrendState.CHOPPY:
            score += 0.20
        return min(1.0, score)

    # ── Day type status ────────────────────────────────────────────────────────

    @staticmethod
    def _compute_day_type_status(locked_type: DayType, confidence: float) -> DayTypeStatus:
        if locked_type == DayType.UNKNOWN:
            return DayTypeStatus.FORMING
        if confidence >= _THRESHOLD_HEALTHY:
            return DayTypeStatus.LOCKED_HEALTHY
        if confidence >= _THRESHOLD_STRESSED:
            return DayTypeStatus.STRESSED
        return DayTypeStatus.INVALIDATED

    # ── Trade permission ───────────────────────────────────────────────────────

    @staticmethod
    def _compute_trade_permission(
        day_type: DayType,
        day_type_status: DayTypeStatus,
        live_bias: LiveBias,
    ) -> tuple[bool, bool, str]:
        """Returns (long_allowed, short_allowed, reason)."""
        if day_type == DayType.UNKNOWN or day_type_status == DayTypeStatus.FORMING:
            return True, True, "forming: no restriction"

        healthy = day_type_status == DayTypeStatus.LOCKED_HEALTHY
        invalidated = day_type_status == DayTypeStatus.INVALIDATED
        bearish = live_bias in {LiveBias.BEARISH, LiveBias.TRANSITIONING_BEARISH}
        bullish = live_bias in {LiveBias.BULLISH, LiveBias.TRANSITIONING_BULLISH}

        if day_type == DayType.TREND_UP:
            if healthy:
                return True, False, "trend_up healthy: short blocked"
            if invalidated and bearish:
                return False, True, "trend_up invalidated + bearish: long blocked"
            if invalidated:
                return True, True, "trend_up invalidated: both allowed"
            # STRESSED
            if bullish:
                return True, False, "trend_up stressed + bullish: short blocked"
            return True, True, "trend_up stressed + bearish: both allowed"

        if day_type == DayType.TREND_DOWN:
            if healthy:
                return False, True, "trend_down healthy: long blocked"
            if invalidated and bullish:
                return True, False, "trend_down invalidated + bullish: short blocked"
            if invalidated:
                return True, True, "trend_down invalidated: both allowed"
            # STRESSED
            if bearish:
                return False, True, "trend_down stressed + bearish: long blocked"
            return True, True, "trend_down stressed + bullish: both allowed"

        # RANGE or BALANCED
        if invalidated and bearish:
            return False, True, "range/balanced invalidated + bearish: long blocked"
        if invalidated and bullish:
            return True, False, "range/balanced invalidated + bullish: short blocked"
        return True, True, "no restriction"

    # ── Structure score ────────────────────────────────────────────────────────

    @staticmethod
    def _score_structure(snap: BarSnapshot, trend: TrendState) -> float:
        score = 0.5
        if trend in {TrendState.TRENDING_UP, TrendState.TRENDING_DOWN}:
            score += 0.3
        if snap.relative_volume and snap.relative_volume > 1.2:
            score += 0.2
        return min(1.0, score)

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
