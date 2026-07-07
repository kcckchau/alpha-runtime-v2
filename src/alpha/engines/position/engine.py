"""
PositionMonitor — shadow-mode position tracker.

Dual-frequency design:
  M1 (PIPELINE_OUTPUT): refresh pending setup watchlist from scored setups.
  S1 (BAR S1): drive entry confirmation and exit checks against live price + intrabar flow.

In shadow mode this engine never places real orders — it emits PositionSignalEvent
(WOULD_ENTER / WOULD_HOLD / WOULD_EXIT) so the UI and logs can track hypothetical trades.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from alpha.core.engine import BaseEngine
from alpha.models.enums import EventType, BarTimeframe, OrderSide, SetupGrade, SetupType
from alpha.models.events import AnyEvent, BarEvent, PipelineOutputEvent, QuoteEvent, ThesisEvent, PositionSignalEvent

logger = logging.getLogger(__name__)

_SELL_TYPES: frozenset[SetupType] = frozenset({
    SetupType.VWAP_REJECTION,
    SetupType.VWAP_FAILED_RECLAIM_SHORT,
    SetupType.TREND_PULLBACK_SHORT,
    SetupType.ORB_BREAKDOWN,
})

_GRADE_ORDER = [SetupGrade.SSS, SetupGrade.A_PLUS, SetupGrade.A, SetupGrade.B, SetupGrade.C]


def _grade_index(g: SetupGrade | None) -> int:
    try:
        return _GRADE_ORDER.index(g)
    except (ValueError, TypeError):
        return len(_GRADE_ORDER)


def _direction(setup: Any) -> OrderSide:
    return OrderSide.SELL if setup.setup_type in _SELL_TYPES else OrderSide.BUY


@dataclass
class ShadowPosition:
    position_id: UUID
    symbol: str
    setup_id: UUID
    setup_type: SetupType
    direction: OrderSide
    entry_price: Decimal
    stop: Decimal
    target: Decimal
    grade: SetupGrade
    entered_at: datetime
    thesis_type_at_entry: str | None = None
    bars_held: int = 0
    mfe: Decimal = field(default_factory=lambda: Decimal("0"))
    mae: Decimal = field(default_factory=lambda: Decimal("0"))
    last_price: Decimal | None = None
    pending_thesis_invalidation: bool = False


class PositionMonitor(BaseEngine):
    """
    Shadow-mode position tracker.

    Subscribes to:
      - PIPELINE_OUTPUT  (M1) — refreshes pending setup watchlist
      - BAR (S1)         — drives entry confirmation and exit checks at 1s cadence
      - QUOTE            (drop_if_full) — latest quote for bid_ask_imbalance
      - THESIS           — thesis invalidation detection

    Publishes:
      - POSITION_SIGNAL  (WOULD_ENTER / WOULD_HOLD / WOULD_EXIT)
    """

    def __init__(
        self,
        event_bus: Any,
        intrabar_engines: dict[str, Any],   # symbol → IntrabarFlowEngine
        min_grade: SetupGrade = SetupGrade.A,
        timeout_bars: int = 120,            # S1 bars; 120 = 2 minutes
    ) -> None:
        super().__init__()
        self._bus = event_bus
        self._intrabar_engines = intrabar_engines
        self._min_grade = min_grade
        self._timeout_bars = timeout_bars

        # symbol → {setup_id → Setup}
        self._pending_setups: dict[str, dict[UUID, Any]] = {}
        # symbol → ShadowPosition (at most one per symbol)
        self._shadow_positions: dict[str, ShadowPosition] = {}
        # symbol → latest QuoteEvent
        self._latest_quote: dict[str, QuoteEvent] = {}
        # symbol → current thesis type string
        self._latest_thesis_type: dict[str, str | None] = {}
        # setup_ids that recently exited — cleared on next pipeline output
        self._recently_exited: set[UUID] = set()

    @property
    def name(self) -> str:
        return "PositionMonitor"

    async def _on_initialize(self) -> None:
        pass

    async def _on_start(self) -> None:
        self._bus.subscribe(EventType.PIPELINE_OUTPUT, self._on_pipeline_output)
        self._bus.subscribe(EventType.BAR, self._on_s1_bar)
        self._bus.subscribe(EventType.QUOTE, self._on_quote, drop_if_full=True)
        self._bus.subscribe(EventType.THESIS, self._on_thesis)
        logger.info("PositionMonitor started (min_grade=%s, timeout_bars=%d)", self._min_grade, self._timeout_bars)

    async def _on_stop(self) -> None:
        pass

    # ── EventBus handlers ─────────────────────────────────────────────────────

    async def _on_pipeline_output(self, event: AnyEvent) -> None:
        if not isinstance(event, PipelineOutputEvent):
            return
        sym = event.symbol
        self._recently_exited.clear()

        qualifying: dict[UUID, Any] = {}
        for setup in event.scored_setups:
            if (
                setup.grade is not None
                and _grade_index(setup.grade) <= _grade_index(self._min_grade)
                and setup.entry_trigger is not None
                and setup.stop_reference is not None
                and setup.target_reference is not None
            ):
                qualifying[setup.setup_id] = setup

        self._pending_setups[sym] = qualifying

    async def _on_thesis(self, event: AnyEvent) -> None:
        if not isinstance(event, ThesisEvent):
            return
        sym = event.symbol
        new_type = str(event.thesis_type)
        self._latest_thesis_type[sym] = new_type

        pos = self._shadow_positions.get(sym)
        if pos is not None and pos.thesis_type_at_entry is not None:
            if new_type != pos.thesis_type_at_entry:
                pos.pending_thesis_invalidation = True

    async def _on_quote(self, event: AnyEvent) -> None:
        if not isinstance(event, QuoteEvent):
            return
        self._latest_quote[event.symbol] = event

    async def _on_s1_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent) or event.timeframe != BarTimeframe.S1:
            return
        sym = event.symbol
        price = event.close

        pos = self._shadow_positions.get(sym)
        if pos is not None:
            await self._check_exit(pos, price, event.timestamp)
        else:
            await self._check_entry(sym, price, event.timestamp)

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _check_entry(self, symbol: str, price: Decimal, ts: datetime) -> None:
        pending = self._pending_setups.get(symbol, {})
        if not pending:
            return

        intrabar_engine = self._intrabar_engines.get(symbol)
        intrabar_state = intrabar_engine.get_state() if intrabar_engine else None
        bai = self._compute_bai(symbol)

        # Sort by grade (best first), then by setup_id for stability
        candidates = sorted(pending.values(), key=lambda s: (_grade_index(s.grade), str(s.setup_id)))

        for setup in candidates:
            if setup.setup_id in self._recently_exited:
                continue
            if self._confirms_entry(setup, price, intrabar_state, bai):
                direction = _direction(setup)
                thesis_type = self._latest_thesis_type.get(symbol)
                pos = ShadowPosition(
                    position_id=uuid4(),
                    symbol=symbol,
                    setup_id=setup.setup_id,
                    setup_type=setup.setup_type,
                    direction=direction,
                    entry_price=price,
                    stop=setup.stop_reference,
                    target=setup.target_reference,
                    grade=setup.grade,
                    entered_at=ts,
                    thesis_type_at_entry=thesis_type,
                )
                self._shadow_positions[symbol] = pos
                await self._emit_enter(pos, price, intrabar_state, bai, ts)
                break

    def _confirms_entry(
        self,
        setup: Any,
        price: Decimal,
        intrabar: Any | None,
        bai: float | None,
    ) -> bool:
        direction = _direction(setup)
        entry = setup.entry_trigger
        if direction == OrderSide.BUY:
            price_ok = price >= entry
            delta_ok = intrabar is None or intrabar.delta > 0
            bai_ok = bai is None or bai > 0.5
        else:
            price_ok = price <= entry
            delta_ok = intrabar is None or intrabar.delta < 0
            bai_ok = bai is None or bai < 0.5
        return price_ok and delta_ok and bai_ok

    # ── Exit ──────────────────────────────────────────────────────────────────

    async def _check_exit(self, pos: ShadowPosition, price: Decimal, ts: datetime) -> None:
        pos.bars_held += 1
        pos.last_price = price

        # Update MFE / MAE in points
        if pos.direction == OrderSide.BUY:
            excursion = price - pos.entry_price
        else:
            excursion = pos.entry_price - price

        if excursion > pos.mfe:
            pos.mfe = excursion
        if excursion < -pos.mae:
            pos.mae = -excursion

        reason: str | None = None
        if self._is_stop_hit(pos, price):
            reason = "stop_hit"
        elif self._is_target_hit(pos, price):
            reason = "target_hit"
        elif pos.pending_thesis_invalidation:
            reason = "thesis_invalidated"
        elif pos.bars_held >= self._timeout_bars:
            reason = "timeout"

        if reason:
            pnl = (price - pos.entry_price) if pos.direction == OrderSide.BUY else (pos.entry_price - price)
            await self._emit_exit(pos, price, reason, pnl, ts)
            self._recently_exited.add(pos.setup_id)
            del self._shadow_positions[pos.symbol]
        else:
            await self._emit_hold(pos, price, ts)

    def _is_stop_hit(self, pos: ShadowPosition, price: Decimal) -> bool:
        if pos.direction == OrderSide.BUY:
            return price <= pos.stop
        return price >= pos.stop

    def _is_target_hit(self, pos: ShadowPosition, price: Decimal) -> bool:
        if pos.direction == OrderSide.BUY:
            return price >= pos.target
        return price <= pos.target

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _compute_bai(self, symbol: str) -> float | None:
        q = self._latest_quote.get(symbol)
        if q is None:
            return None
        total = q.bid_size + q.ask_size
        if total == 0:
            return None
        return q.bid_size / total

    # ── Emitters ──────────────────────────────────────────────────────────────

    async def _emit_enter(
        self,
        pos: ShadowPosition,
        price: Decimal,
        intrabar: Any | None,
        bai: float | None,
        ts: datetime,
    ) -> None:
        delta = intrabar.delta if intrabar is not None else None
        event = PositionSignalEvent(
            symbol=pos.symbol,
            timestamp=ts,
            signal_type="would_enter",
            setup_id=pos.setup_id,
            setup_type=pos.setup_type,
            direction=pos.direction,
            entry_price=pos.entry_price,
            stop=pos.stop,
            target=pos.target,
            current_price=price,
            grade=pos.grade,
            intrabar_delta=delta,
            bid_ask_imbalance=bai,
        )
        logger.info(
            "PositionMonitor WOULD_ENTER %s %s %s @ %.2f stop=%.2f target=%.2f grade=%s delta=%s bai=%s",
            pos.symbol, pos.direction, pos.setup_type, float(price),
            float(pos.stop), float(pos.target), pos.grade, delta,
            f"{bai:.3f}" if bai is not None else "None",
        )
        await self._bus.publish(event)

    async def _emit_hold(self, pos: ShadowPosition, price: Decimal, ts: datetime) -> None:
        event = PositionSignalEvent(
            symbol=pos.symbol,
            timestamp=ts,
            signal_type="would_hold",
            setup_id=pos.setup_id,
            setup_type=pos.setup_type,
            direction=pos.direction,
            entry_price=pos.entry_price,
            stop=pos.stop,
            target=pos.target,
            current_price=price,
            grade=pos.grade,
            bars_held_so_far=pos.bars_held,
            mfe=pos.mfe,
            mae=pos.mae,
        )
        await self._bus.publish(event)

    async def _emit_exit(
        self,
        pos: ShadowPosition,
        price: Decimal,
        reason: str,
        pnl: Decimal,
        ts: datetime,
    ) -> None:
        event = PositionSignalEvent(
            symbol=pos.symbol,
            timestamp=ts,
            signal_type="would_exit",
            setup_id=pos.setup_id,
            setup_type=pos.setup_type,
            direction=pos.direction,
            entry_price=pos.entry_price,
            stop=pos.stop,
            target=pos.target,
            current_price=price,
            grade=pos.grade,
            exit_reason=reason,
            pnl_pts=pnl,
            bars_held=pos.bars_held,
            mfe=pos.mfe,
            mae=pos.mae,
        )
        logger.info(
            "PositionMonitor WOULD_EXIT %s %s reason=%s pnl=%.2f pts bars=%d mfe=%.2f mae=%.2f",
            pos.symbol, pos.setup_type, reason, float(pnl), pos.bars_held,
            float(pos.mfe), float(pos.mae),
        )
        await self._bus.publish(event)
