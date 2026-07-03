"""
ThesisEngine — Shadow Narrative Mode (V1)

Tracks evolving trading theses for each symbol. Unlike SetupEngine (one-shot
detector), ThesisEngine maintains a running belief updated bar-by-bar using
three input streams:

  BarSnapshot       → from FeatureEngine (VWAP, EMA slopes, close position, ATR)
  LevelMemorySnapshot → from FeatureEngine (VWAP touch count, last outcome)
  TickFlowSnapshot    → computed internally from rolling TradeEvent window

Two thesis types for V1:
  FAKE_BREAKDOWN_RECLAIM_LONG    — price sweeps below VWAP, sellers fail, price reclaims
  VWAP_FAILED_RECLAIM_SHORT      — price below VWAP, failed reclaim attempt, VWAP = resistance

Lifecycle per symbol:
  WATCHING → BUILDING → READY → (ORDER_WORKING / TRIGGERED in V2)
  Any state → INVALIDATED → possible FLIP to opposite thesis

Shadow mode: emits ThesisEvent each bar but does NOT place orders.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Deque
from uuid import uuid4

from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.config.settings import AlphaSettings
from alpha.models.enums import BarTimeframe, EventType, HealthStatus, ThesisState, ThesisType
from alpha.models.events import AnyEvent, BarEvent, ThesisEvent, TradeEvent
from alpha.models.thesis import (
    ActiveThesis,
    EvidenceItem,
    LevelMemorySnapshot,
    ThesisCandidate,
    TickFlowSnapshot,
)
from alpha.models.snapshot import BarSnapshot
from alpha.models.events import EventMetadata

if TYPE_CHECKING:
    from alpha.engines.feature.engine import FeatureEngine

logger = logging.getLogger(__name__)

# Confidence thresholds
_WATCHING_TO_BUILDING = 0.30
_BUILDING_TO_READY = 0.65

# Risk gates
_MAX_RISK_ATR_MULT = Decimal("1.5")    # stop must be <= 1.5x ATR from entry
_MIN_REWARD_RATIO = Decimal("2.0")     # minimum R:R to commit

# Thesis expires after this many bars without reaching READY
_MAX_BARS_WATCHING = 8
_MAX_BARS_BUILDING = 20

# Rolling tick window duration
_TICK_WINDOW_SECONDS = 30
_TICK_SHORT_WINDOW_SECONDS = 10

# Large trade: size > this multiple of avg trade size
_LARGE_TRADE_MULT = 3.0


class _TradeRecord:
    """Lightweight trade record for the rolling window."""
    __slots__ = ("ts", "price", "size", "is_buy", "is_sell")

    def __init__(self, ts: datetime, price: Decimal, size: int, is_buy: bool, is_sell: bool) -> None:
        self.ts = ts
        self.price = price
        self.size = size
        self.is_buy = is_buy
        self.is_sell = is_sell


class ThesisEngine(BaseEngine):
    """
    Shadow-mode thesis engine. Tracks evolving trading narratives per symbol.
    Emits ThesisEvent after each M1 bar update.
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
        self._feature_engine: FeatureEngine | None = None

        # Active thesis per symbol
        self._active: dict[str, ActiveThesis] = {}

        # Rolling trade window per symbol — deque of _TradeRecord
        self._trade_windows: dict[str, Deque[_TradeRecord]] = {}

        # Session trade count for baseline TPS
        self._session_trades: dict[str, int] = {}
        self._session_start: dict[str, datetime] = {}

        self._bars_processed: int = 0
        self._thesis_created: int = 0

    @property
    def name(self) -> str:
        return "ThesisEngine"

    def set_feature_engine(self, engine: "FeatureEngine") -> None:
        self._feature_engine = engine

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        pass

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.BAR, self._handle_bar)
        self._event_bus.subscribe(EventType.TRADE, self._handle_trade)
        logger.info("ThesisEngine started — shadow narrative mode active")

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        return EngineHealth(
            HealthStatus.HEALTHY,
            self.name,
            {
                "active_theses": len(self._active),
                "bars_processed": self._bars_processed,
                "thesis_created": self._thesis_created,
            },
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def get_thesis(self, symbol: str) -> ActiveThesis | None:
        return self._active.get(symbol)

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return
        if event.timeframe != BarTimeframe.M1:
            return
        if event.is_partial:
            return

        self._bars_processed += 1
        symbol = event.symbol

        if self._feature_engine is None:
            return

        snap = self._feature_engine.get_snapshot(symbol)
        level = self._feature_engine.get_level_memory(symbol)
        if snap is None or level is None:
            return

        tick = self._compute_tick_flow(symbol, event.timestamp)
        self._update_session_baseline(symbol, event.timestamp)

        active = self._active.get(symbol)

        if active is None or active.dominant.state in {
            ThesisState.TRIGGERED,
            ThesisState.EXPIRED,
        }:
            # Try to detect a new dominant thesis
            candidate = self._detect_thesis(symbol, snap, level, event)
            if candidate is not None:
                flip = self._build_flip_candidate(candidate, snap, level)
                self._active[symbol] = ActiveThesis(dominant=candidate, flip=flip)
                self._thesis_created += 1
                logger.info(
                    "ThesisEngine: new thesis %s %s for %s",
                    candidate.thesis_type, candidate.state, symbol,
                )
        else:
            # Update existing thesis
            self._update_thesis(active, snap, level, tick, event)

            # Check if INVALIDATED → trigger immediate flip
            if active.dominant.state == ThesisState.INVALIDATED and active.flip is not None:
                logger.info(
                    "ThesisEngine: %s invalidated (%s) → flipping to %s",
                    active.dominant.thesis_type,
                    active.dominant.invalidation_reason,
                    active.flip.thesis_type,
                )
                # Promote flip to dominant, clear the old flip
                active.dominant = active.flip
                active.dominant.state = ThesisState.WATCHING
                active.flip = None
            elif active.dominant.state in {ThesisState.INVALIDATED, ThesisState.FLIPPED}:
                # No flip available — clear and allow fresh detection next bar
                del self._active[symbol]

        # Emit thesis event
        current = self._active.get(symbol)
        if current is not None:
            await self._emit(current.dominant, snap, level, event)

    async def _handle_trade(self, event: AnyEvent) -> None:
        if not isinstance(event, TradeEvent):
            return
        sym = event.symbol
        if sym not in self._trade_windows:
            self._trade_windows[sym] = deque()
        from alpha.models.enums import TakerSide
        record = _TradeRecord(
            ts=event.timestamp,
            price=event.price,
            size=event.size,
            is_buy=event.taker_side == TakerSide.BUY,
            is_sell=event.taker_side == TakerSide.SELL,
        )
        self._trade_windows[sym].append(record)
        # Track session count for baseline TPS
        self._session_trades[sym] = self._session_trades.get(sym, 0) + 1

    # ── Thesis detection ──────────────────────────────────────────────────────

    def _detect_thesis(
        self,
        symbol: str,
        snap: BarSnapshot,
        level: LevelMemorySnapshot,
        trigger: BarEvent,
    ) -> ThesisCandidate | None:
        """Return a new WATCHING thesis if a trigger condition is met, else None."""

        # FAKE_BREAKDOWN_RECLAIM_LONG triggers:
        # 1. Price just crossed below VWAP — watching whether sellers fail
        # 2. Price swept below VWAP but recovered — possible fake breakdown
        if snap.vwap_cross_down or snap.swept_below_vwap:
            return ThesisCandidate(
                thesis_id=uuid4(),
                thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
                symbol=symbol,
                state=ThesisState.WATCHING,
                confidence=0.10,
                key_level=snap.vwap,
                sweep_low=trigger.low,
                possible_flip=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
                created_at=trigger.timestamp,
                updated_at=trigger.timestamp,
            )

        # VWAP_FAILED_RECLAIM_SHORT triggers:
        # Price is below VWAP AND we see a failed reclaim attempt (rejected outcome)
        if (
            not snap.is_above_vwap
            and snap.bars_below_vwap >= 2
            and level.last_vwap_outcome in {"rejected", "broken"}
            and level.bars_since_last_vwap_touch <= 5
        ):
            return ThesisCandidate(
                thesis_id=uuid4(),
                thesis_type=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
                symbol=symbol,
                state=ThesisState.WATCHING,
                confidence=0.15,
                key_level=snap.vwap,
                rejection_high=trigger.high,
                possible_flip=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
                created_at=trigger.timestamp,
                updated_at=trigger.timestamp,
            )

        return None

    def _build_flip_candidate(
        self,
        dominant: ThesisCandidate,
        snap: BarSnapshot,
        level: LevelMemorySnapshot,
    ) -> ThesisCandidate | None:
        """Build a shadow flip thesis that tracks the opposite path."""
        if dominant.thesis_type == ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG:
            return ThesisCandidate(
                thesis_id=uuid4(),
                thesis_type=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
                symbol=dominant.symbol,
                state=ThesisState.WATCHING,
                confidence=0.05,
                key_level=snap.vwap,
                possible_flip=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            )
        if dominant.thesis_type == ThesisType.VWAP_FAILED_RECLAIM_SHORT:
            return ThesisCandidate(
                thesis_id=uuid4(),
                thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
                symbol=dominant.symbol,
                state=ThesisState.WATCHING,
                confidence=0.05,
                key_level=snap.vwap,
                possible_flip=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
            )
        return None

    # ── Thesis lifecycle update ───────────────────────────────────────────────

    def _update_thesis(
        self,
        active: ActiveThesis,
        snap: BarSnapshot,
        level: LevelMemorySnapshot,
        tick: TickFlowSnapshot,
        trigger: BarEvent,
    ) -> None:
        thesis = active.dominant
        thesis.bars_alive += 1
        thesis.updated_at = trigger.timestamp

        if thesis.thesis_type == ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG:
            self._update_fake_breakdown_long(thesis, snap, level, tick, trigger)
        elif thesis.thesis_type == ThesisType.VWAP_FAILED_RECLAIM_SHORT:
            self._update_vwap_failed_reclaim_short(thesis, snap, level, tick, trigger)

        # Advance state machine
        self._advance_state(thesis, snap)

    def _advance_state(self, thesis: ThesisCandidate, snap: BarSnapshot) -> None:
        if thesis.state == ThesisState.WATCHING:
            if thesis.confidence >= _WATCHING_TO_BUILDING:
                thesis.state = ThesisState.BUILDING
            elif thesis.bars_alive > _MAX_BARS_WATCHING:
                thesis.state = ThesisState.EXPIRED
        elif thesis.state == ThesisState.BUILDING:
            if thesis.confidence >= _BUILDING_TO_READY and self._necessary_conditions_met(thesis, snap):
                thesis.state = ThesisState.READY
                self._compute_entry_plan(thesis, snap)
            elif thesis.bars_alive > _MAX_BARS_BUILDING:
                thesis.state = ThesisState.EXPIRED

    def _necessary_conditions_met(self, thesis: ThesisCandidate, snap: BarSnapshot) -> bool:
        if thesis.thesis_type == ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG:
            if not snap.is_above_vwap:
                return False
            if snap.bars_above_vwap < 1:
                return False
            if thesis.sweep_low is None:
                return False
            if snap.atr_14 is not None and snap.atr_14 > 0:
                risk = snap.bar.close - thesis.sweep_low
                if risk > snap.atr_14 * _MAX_RISK_ATR_MULT:
                    return False
            return True

        if thesis.thesis_type == ThesisType.VWAP_FAILED_RECLAIM_SHORT:
            if snap.is_above_vwap:
                return False
            if snap.bars_below_vwap < 1:
                return False
            if thesis.rejection_high is None:
                return False
            if snap.atr_14 is not None and snap.atr_14 > 0:
                risk = thesis.rejection_high - snap.bar.close
                if risk > snap.atr_14 * _MAX_RISK_ATR_MULT:
                    return False
            return True

        return False

    def _compute_entry_plan(self, thesis: ThesisCandidate, snap: BarSnapshot) -> None:
        atr = snap.atr_14 or Decimal("10")
        if thesis.thesis_type == ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG:
            thesis.entry = snap.vwap + atr * Decimal("0.10")
            thesis.stop = thesis.sweep_low - atr * Decimal("0.10") if thesis.sweep_low else snap.vwap - atr
            if thesis.entry and thesis.stop:
                risk = thesis.entry - thesis.stop
                thesis.target = thesis.entry + risk * Decimal("2.5")
        elif thesis.thesis_type == ThesisType.VWAP_FAILED_RECLAIM_SHORT:
            thesis.entry = snap.vwap - atr * Decimal("0.10")
            thesis.stop = thesis.rejection_high + atr * Decimal("0.10") if thesis.rejection_high else snap.vwap + atr
            if thesis.entry and thesis.stop:
                risk = thesis.stop - thesis.entry
                thesis.target = thesis.entry - risk * Decimal("2.5")

    # ── FAKE_BREAKDOWN_RECLAIM_LONG confidence update ─────────────────────────

    def _update_fake_breakdown_long(
        self,
        thesis: ThesisCandidate,
        snap: BarSnapshot,
        level: LevelMemorySnapshot,
        tick: TickFlowSnapshot,
        trigger: BarEvent,
    ) -> None:
        evidence: list[EvidenceItem] = []
        delta = 0.0

        # Update sweep low if we saw a deeper dip
        if trigger.low < snap.vwap and (thesis.sweep_low is None or trigger.low < thesis.sweep_low):
            thesis.sweep_low = trigger.low

        # INVALIDATION checks — run first
        if snap.bars_below_vwap >= 3 and not snap.vwap_cross_up and not snap.swept_below_vwap:
            thesis.state = ThesisState.INVALIDATED
            thesis.invalidation_reason = f"price held below VWAP for {snap.bars_below_vwap} bars — sellers in control"
            return

        if snap.is_lower_low and not snap.is_above_vwap and thesis.bars_alive > 2:
            thesis.state = ThesisState.INVALIDATED
            thesis.invalidation_reason = "new session low formed below VWAP — no fake breakdown"
            return

        if (
            snap.bar_close_position_pct is not None
            and snap.bar_close_position_pct < 0.25
            and not snap.is_above_vwap
            and trigger.high - trigger.low > float(snap.atr_14 or 1) * 1.2
        ):
            thesis.state = ThesisState.INVALIDATED
            thesis.invalidation_reason = "wide red bar closed near its low below VWAP"
            return

        # Core signal: price recovered above VWAP
        if snap.vwap_cross_up:
            delta += 0.22
            evidence.append(EvidenceItem("price reclaimed VWAP", positive=True, weight=0.22))
        elif snap.swept_below_vwap:
            delta += 0.12
            evidence.append(EvidenceItem("price swept below VWAP but recovered above", positive=True, weight=0.12))
        elif snap.is_above_vwap:
            if snap.bars_above_vwap >= 2:
                delta += 0.06
                evidence.append(EvidenceItem(f"held above VWAP for {snap.bars_above_vwap} bars", positive=True, weight=0.06))

        # Reclaim candle quality
        cp = snap.bar_close_position_pct
        if cp is not None:
            if cp > 0.65:
                delta += 0.10
                evidence.append(EvidenceItem(f"strong close ({cp:.0%} of bar range)", positive=True, weight=0.10))
            elif cp < 0.35 and snap.is_above_vwap:
                delta -= 0.08
                evidence.append(EvidenceItem(f"weak close ({cp:.0%}) despite above VWAP", positive=False, weight=0.08))

        # EMA directional pressure
        if snap.ema_9_slope is not None:
            if snap.ema_9_slope > 0.005:
                delta += 0.08
                evidence.append(EvidenceItem("EMA9 turning up", positive=True, weight=0.08))
            elif snap.ema_9_slope < -0.015:
                delta -= 0.08
                evidence.append(EvidenceItem("EMA9 strongly declining", positive=False, weight=0.08))

        if snap.ema_20_slope is not None:
            if snap.ema_20_slope > 0.002:
                delta += 0.05
                evidence.append(EvidenceItem("EMA20 flattening/rising", positive=True, weight=0.05))
            elif snap.ema_20_slope < -0.010:
                delta -= 0.05
                evidence.append(EvidenceItem("EMA20 declining", positive=False, weight=0.05))

        # Tick flow: did selling dry up?
        if tick.sell_tps_10s is not None and tick.session_avg_tps > 0:
            sell_ratio = tick.sell_tps_10s / tick.session_avg_tps
            if sell_ratio < 0.4:
                delta += 0.10
                evidence.append(EvidenceItem(f"selling dried up ({sell_ratio:.0%} of avg TPS)", positive=True, weight=0.10))
            elif sell_ratio > 1.4:
                delta -= 0.08
                evidence.append(EvidenceItem(f"selling still elevated ({sell_ratio:.0%} of avg TPS)", positive=False, weight=0.08))

        if tick.volume_acceleration > 0:
            if tick.volume_acceleration < 0.65 and not snap.is_above_vwap:
                delta += 0.05
                evidence.append(EvidenceItem("breakdown volume shrinking", positive=True, weight=0.05))
            elif tick.volume_acceleration > 1.5 and snap.vwap_cross_up:
                delta += 0.05
                evidence.append(EvidenceItem("reclaim has accelerating volume", positive=True, weight=0.05))

        # VWAP level memory
        if level.last_vwap_outcome in {"swept", "reclaimed"}:
            delta += 0.05
            evidence.append(EvidenceItem(f"VWAP previously {level.last_vwap_outcome} — level respects buyers", positive=True, weight=0.05))
        elif level.last_vwap_outcome == "broken" and level.bars_since_last_vwap_touch < 5:
            delta -= 0.05
            evidence.append(EvidenceItem("VWAP was recently broken — may act as resistance", positive=False, weight=0.05))

        # Risk: is stop distance tight?
        if thesis.sweep_low is not None and snap.atr_14 is not None and snap.atr_14 > 0:
            risk = snap.bar.close - thesis.sweep_low
            risk_atr = float(risk / snap.atr_14)
            if risk_atr <= 0.7:
                delta += 0.05
                evidence.append(EvidenceItem(f"tight stop distance ({risk_atr:.1f}× ATR)", positive=True, weight=0.05))
            elif risk_atr > 1.5:
                delta -= 0.10
                evidence.append(EvidenceItem(f"stop too wide ({risk_atr:.1f}× ATR) — setup may be late", positive=False, weight=0.10))

        # Update confidence (clamped)
        thesis.confidence = max(0.0, min(1.0, thesis.confidence + delta))
        thesis.evidence = evidence

        # Commit/invalidation condition descriptions
        thesis.commit_conditions = self._commit_conditions_long(thesis, snap, level)
        thesis.invalidation_conditions = self._invalidation_conditions_long(snap)

    # ── VWAP_FAILED_RECLAIM_SHORT confidence update ───────────────────────────

    def _update_vwap_failed_reclaim_short(
        self,
        thesis: ThesisCandidate,
        snap: BarSnapshot,
        level: LevelMemorySnapshot,
        tick: TickFlowSnapshot,
        trigger: BarEvent,
    ) -> None:
        evidence: list[EvidenceItem] = []
        delta = 0.0

        # Update rejection high
        if trigger.high > snap.vwap and (thesis.rejection_high is None or trigger.high > thesis.rejection_high):
            thesis.rejection_high = trigger.high

        # INVALIDATION checks
        if snap.bars_above_vwap >= 2:
            thesis.state = ThesisState.INVALIDATED
            thesis.invalidation_reason = f"price held above VWAP for {snap.bars_above_vwap} bars — buyers in control"
            return

        if snap.is_higher_high and snap.is_above_vwap:
            thesis.state = ThesisState.INVALIDATED
            thesis.invalidation_reason = "higher high formed above VWAP — failed reclaim reversed"
            return

        if (
            snap.bar_close_position_pct is not None
            and snap.bar_close_position_pct > 0.75
            and snap.is_above_vwap
            and trigger.high - trigger.low > float(snap.atr_14 or 1) * 1.0
        ):
            thesis.state = ThesisState.INVALIDATED
            thesis.invalidation_reason = "strong green bar closed near its high above VWAP"
            return

        # Core signal: failed reclaim
        if snap.vwap_cross_down:
            delta += 0.20
            evidence.append(EvidenceItem("reclaim attempt failed — price crossed back below VWAP", positive=True, weight=0.20))
        elif not snap.is_above_vwap and snap.bars_below_vwap >= 2:
            delta += 0.06
            evidence.append(EvidenceItem(f"price holding below VWAP ({snap.bars_below_vwap} bars)", positive=True, weight=0.06))

        # Rejection candle quality
        cp = snap.bar_close_position_pct
        if cp is not None:
            if cp < 0.35:
                delta += 0.10
                evidence.append(EvidenceItem(f"weak close ({cp:.0%} of bar range) after VWAP test", positive=True, weight=0.10))
            elif cp > 0.65 and not snap.is_above_vwap:
                delta -= 0.08
                evidence.append(EvidenceItem(f"strong close ({cp:.0%}) — buyers present", positive=False, weight=0.08))

        # EMA pressure
        if snap.ema_9_slope is not None:
            if snap.ema_9_slope < -0.005:
                delta += 0.08
                evidence.append(EvidenceItem("EMA9 sloping down", positive=True, weight=0.08))
            elif snap.ema_9_slope > 0.015:
                delta -= 0.08
                evidence.append(EvidenceItem("EMA9 still rising — bearish thesis weakened", positive=False, weight=0.08))

        if snap.ema_20_slope is not None:
            if snap.ema_20_slope < -0.002:
                delta += 0.05
                evidence.append(EvidenceItem("EMA20 declining", positive=True, weight=0.05))

        # Tick flow: buying faded?
        if tick.buy_tps_10s is not None and tick.session_avg_tps > 0:
            buy_ratio = tick.buy_tps_10s / tick.session_avg_tps
            if buy_ratio < 0.4:
                delta += 0.10
                evidence.append(EvidenceItem(f"buying faded ({buy_ratio:.0%} of avg TPS)", positive=True, weight=0.10))
            elif buy_ratio > 1.4:
                delta -= 0.08
                evidence.append(EvidenceItem(f"buying still active ({buy_ratio:.0%} of avg TPS)", positive=False, weight=0.08))

        if tick.volume_acceleration > 0:
            if tick.volume_acceleration > 1.3 and not snap.is_above_vwap:
                delta += 0.05
                evidence.append(EvidenceItem("sell volume accelerating below VWAP", positive=True, weight=0.05))

        # Level memory
        if level.last_vwap_outcome == "rejected":
            delta += 0.06
            evidence.append(EvidenceItem("previous VWAP reclaim also failed — double rejection", positive=True, weight=0.06))
        elif level.last_vwap_outcome in {"reclaimed", "swept"}:
            delta -= 0.05
            evidence.append(EvidenceItem("VWAP has previously been reclaimed — support present", positive=False, weight=0.05))

        # Lower high structure
        if snap.is_lower_high and not snap.is_above_vwap:
            delta += 0.06
            evidence.append(EvidenceItem("lower high forming below VWAP", positive=True, weight=0.06))

        # Risk
        if thesis.rejection_high is not None and snap.atr_14 is not None and snap.atr_14 > 0:
            risk = thesis.rejection_high - snap.bar.close
            risk_atr = float(risk / snap.atr_14)
            if risk_atr <= 0.7:
                delta += 0.05
                evidence.append(EvidenceItem(f"tight stop distance ({risk_atr:.1f}× ATR)", positive=True, weight=0.05))
            elif risk_atr > 1.5:
                delta -= 0.10
                evidence.append(EvidenceItem(f"stop too wide ({risk_atr:.1f}× ATR)", positive=False, weight=0.10))

        thesis.confidence = max(0.0, min(1.0, thesis.confidence + delta))
        thesis.evidence = evidence
        thesis.commit_conditions = self._commit_conditions_short(thesis, snap, level)
        thesis.invalidation_conditions = self._invalidation_conditions_short(snap)

    # ── Narrative helpers ─────────────────────────────────────────────────────

    def _commit_conditions_long(
        self, thesis: ThesisCandidate, snap: BarSnapshot, level: LevelMemorySnapshot
    ) -> list[str]:
        conds = []
        if not snap.is_above_vwap:
            conds.append("price must close above VWAP")
        if snap.bars_above_vwap < 1:
            conds.append("need at least one confirmed bar above VWAP")
        if thesis.sweep_low is None:
            conds.append("need to identify a clear sweep low for stop placement")
        if snap.atr_14 is not None and thesis.sweep_low is not None:
            risk = snap.bar.close - thesis.sweep_low
            risk_atr = float(risk / snap.atr_14)
            if risk_atr > 1.5:
                conds.append(f"risk must tighten (currently {risk_atr:.1f}× ATR, need < 1.5×)")
        if thesis.confidence < _BUILDING_TO_READY:
            conds.append(f"confidence must reach {_BUILDING_TO_READY:.0%} (currently {thesis.confidence:.0%})")
        return conds

    def _invalidation_conditions_long(self, snap: BarSnapshot) -> list[str]:
        return [
            "2+ bars close below VWAP — sellers in control",
            "wide red bar closes near its low below VWAP",
            "new session low below the sweep low — no fake breakdown",
            "stop distance expands beyond 1.5× ATR",
        ]

    def _commit_conditions_short(
        self, thesis: ThesisCandidate, snap: BarSnapshot, level: LevelMemorySnapshot
    ) -> list[str]:
        conds = []
        if snap.is_above_vwap:
            conds.append("price must close below VWAP")
        if thesis.rejection_high is None:
            conds.append("need to identify a clear rejection high for stop placement")
        if snap.atr_14 is not None and thesis.rejection_high is not None:
            risk = thesis.rejection_high - snap.bar.close
            risk_atr = float(risk / snap.atr_14)
            if risk_atr > 1.5:
                conds.append(f"risk must tighten (currently {risk_atr:.1f}× ATR, need < 1.5×)")
        if thesis.confidence < _BUILDING_TO_READY:
            conds.append(f"confidence must reach {_BUILDING_TO_READY:.0%} (currently {thesis.confidence:.0%})")
        return conds

    def _invalidation_conditions_short(self, snap: BarSnapshot) -> list[str]:
        return [
            "2+ bars hold above VWAP — buyers in control",
            "higher high forms above VWAP — trend reversing",
            "strong green bar closes in upper range above VWAP",
            "stop distance expands beyond 1.5× ATR",
        ]

    # ── Tick flow computation ─────────────────────────────────────────────────

    def _compute_tick_flow(self, symbol: str, as_of: datetime) -> TickFlowSnapshot:
        window = self._trade_windows.get(symbol)
        cutoff_30 = as_of - timedelta(seconds=_TICK_WINDOW_SECONDS)
        cutoff_10 = as_of - timedelta(seconds=_TICK_SHORT_WINDOW_SECONDS)

        if not window:
            return TickFlowSnapshot(symbol=symbol, timestamp=as_of)

        # Trim stale entries
        while window and window[0].ts < cutoff_30:
            window.popleft()

        records_30 = list(window)
        records_10 = [r for r in records_30 if r.ts >= cutoff_10]

        def _metrics(records: list[_TradeRecord], window_s: float) -> tuple[float, float, float | None, float | None, float | None, float | None]:
            if not records:
                return 0.0, 0.0, None, None, None, None
            buys = [r for r in records if r.is_buy]
            sells = [r for r in records if r.is_sell]
            buy_tps = len(buys) / window_s if buys else None
            sell_tps = len(sells) / window_s if sells else None
            buy_vps = sum(r.size for r in buys) / window_s if buys else None
            sell_vps = sum(r.size for r in sells) / window_s if sells else None
            return len(records) / window_s, sum(r.size for r in records) / window_s, buy_tps, sell_tps, buy_vps, sell_vps

        tps_30, vps_30, _, _, _, _ = _metrics(records_30, _TICK_WINDOW_SECONDS)
        tps_10, vps_10, buy_tps_10, sell_tps_10, buy_vps_10, sell_vps_10 = _metrics(records_10, _TICK_SHORT_WINDOW_SECONDS)

        avg_size = (sum(r.size for r in records_30) / len(records_30)) if records_30 else 0.0
        large_burst = sum(1 for r in records_10 if r.size > avg_size * _LARGE_TRADE_MULT)

        vol_accel = (vps_10 / vps_30) if vps_30 > 0 else 1.0

        session_avg = self._session_avg_tps(symbol)

        return TickFlowSnapshot(
            symbol=symbol,
            timestamp=as_of,
            tps_10s=tps_10,
            tps_30s=tps_30,
            vps_10s=vps_10,
            vps_30s=vps_30,
            buy_tps_10s=buy_tps_10,
            sell_tps_10s=sell_tps_10,
            buy_vps_10s=buy_vps_10,
            sell_vps_10s=sell_vps_10,
            avg_trade_size_30s=avg_size,
            large_burst_count_10s=large_burst,
            volume_acceleration=vol_accel,
            session_avg_tps=session_avg,
        )

    def _session_avg_tps(self, symbol: str) -> float:
        start = self._session_start.get(symbol)
        count = self._session_trades.get(symbol, 0)
        if start is None or count == 0:
            return 0.0
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        return count / elapsed if elapsed > 0 else 0.0

    def _update_session_baseline(self, symbol: str, ts: datetime) -> None:
        if symbol not in self._session_start:
            self._session_start[symbol] = ts

    # ── Event emission ────────────────────────────────────────────────────────

    async def _emit(
        self,
        thesis: ThesisCandidate,
        snap: BarSnapshot,
        level: LevelMemorySnapshot,
        trigger: BarEvent,
    ) -> None:
        risk_pts = None
        reward_pts = None
        risk_ratio = None
        if thesis.entry and thesis.stop and thesis.target:
            if thesis.thesis_type == ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG:
                risk_pts = thesis.entry - thesis.stop
                reward_pts = thesis.target - thesis.entry
            else:
                risk_pts = thesis.stop - thesis.entry
                reward_pts = thesis.entry - thesis.target
            if risk_pts and risk_pts > 0:
                risk_ratio = float(reward_pts / risk_pts) if reward_pts else None

        pos_evidence = [e.text for e in thesis.evidence if e.positive]
        neg_evidence = [e.text for e in thesis.evidence if not e.positive]

        event = ThesisEvent(
            symbol=thesis.symbol,
            timestamp=trigger.timestamp,
            metadata=EventMetadata(
                source=trigger.metadata.source,
                received_at=trigger.metadata.received_at,
                is_replay=trigger.metadata.is_replay,
            ),
            thesis_id=thesis.thesis_id,
            thesis_type=str(thesis.thesis_type),
            thesis_state=str(thesis.state),
            confidence=round(thesis.confidence, 3),
            bars_alive=thesis.bars_alive,
            entry=thesis.entry,
            stop=thesis.stop,
            target=thesis.target,
            risk_pts=risk_pts,
            reward_pts=reward_pts,
            risk_ratio=risk_ratio,
            key_level=thesis.key_level,
            sweep_low=thesis.sweep_low,
            rejection_high=thesis.rejection_high,
            evidence_positive=pos_evidence,
            evidence_negative=neg_evidence,
            commit_conditions=thesis.commit_conditions,
            invalidation_conditions=thesis.invalidation_conditions,
            possible_flip=str(thesis.possible_flip) if thesis.possible_flip else None,
            invalidation_reason=thesis.invalidation_reason,
            vwap_touch_count=level.vwap_touch_count,
            last_vwap_outcome=level.last_vwap_outcome,
            session_phase=str(snap.session_phase),
        )
        await self._event_bus.publish(event)
