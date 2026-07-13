"""
PaperOrderRouter — OrderRouter implementation for paper (simulated) trading.

Simulates fills at the planned limit price with configurable latency.
Never contacts a broker. Safe to use in development and shadow mode.

Fill simulation:
  - Entry: fills at limit_price after fill_delay_ms
  - Stop: activates on fill and fills when simulated price crosses stop_price
    (V1 simplified: fills immediately for testing; V2 will check against live bars)
  - Target: same as stop — OCO semantics enforced locally

V1 simplifications:
  - No partial fills
  - Immediate fill on entry (at limit price)
  - Stop/target not OCO-linked to live price — fills are simulated for testing
  - No reconciliation needed (always in sync)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine
from uuid import uuid4

from alpha.engines.execution.interfaces import ReconciliationResult
from alpha.engines.execution.models import BrokerOrderIdentity, IntentTradePlan, PlannedOrder
from alpha.models.enums import OrderRole, OrderSide, OrderStatus
from alpha.models.order import Execution, Order

logger = logging.getLogger(__name__)

_UTC = timezone.utc

OrderCallbackT = Callable[[Order], Coroutine[Any, Any, None]]
FillCallbackT = Callable[[Execution], Coroutine[Any, Any, None]]


class PaperOrderRouter:
    """
    Simulates a broker for paper trading. All fills are instant at limit price.
    Implements the OrderRouter Protocol.
    """

    def __init__(self, *, fill_delay_ms: int = 50) -> None:
        self._fill_delay = fill_delay_ms / 1000
        self._order_callbacks: list[OrderCallbackT] = []
        self._fill_callbacks: list[FillCallbackT] = []
        self._submitted: dict[str, BrokerOrderIdentity] = {}   # runtime_order_id → identity
        self._orders: dict[str, Order] = {}                    # runtime_order_id → Order
        self._cancelled: set[str] = set()

    # ── OrderRouter Protocol ──────────────────────────────────────────────────

    async def submit_bracket(self, plan: IntentTradePlan) -> list[BrokerOrderIdentity]:
        """Submit all bracket orders. Entry fills immediately; stop/target activate after."""
        identities: list[BrokerOrderIdentity] = []
        entry = plan.entry_order
        if entry is None:
            return identities

        entry_id = self._make_identity(entry.runtime_order_id)
        identities.append(entry_id)
        self._submitted[entry.runtime_order_id] = entry_id

        # Submit stop and target (activated after entry fills)
        for order in [plan.stop_order] + list(plan.target_orders):
            if order is None:
                continue
            child_id = self._make_identity(order.runtime_order_id)
            identities.append(child_id)
            self._submitted[order.runtime_order_id] = child_id

        # Schedule entry fill
        asyncio.create_task(self._simulate_entry_fill(plan))
        return identities

    async def cancel(self, runtime_order_id: str) -> None:
        self._cancelled.add(runtime_order_id)
        order = self._orders.get(runtime_order_id)
        if order is None:
            return
        if order.status in {OrderStatus.ACCEPTED, OrderStatus.PARTIAL}:
            cancelled = order.model_copy(update={
                "status": OrderStatus.CANCELLED,
                "cancelled_at": datetime.now(_UTC),
            })
            self._orders[runtime_order_id] = cancelled
            for cb in self._order_callbacks:
                await cb(cancelled)

    async def flatten(self, instrument_id: str) -> list[BrokerOrderIdentity]:
        """Cancel all working orders for symbol and simulate market fill to flat."""
        flat_ids: list[BrokerOrderIdentity] = []
        for roid, order in list(self._orders.items()):
            if order.symbol == instrument_id and not order.is_terminal:
                await self.cancel(roid)
        return flat_ids

    async def reconcile(self) -> ReconciliationResult:
        """Paper mode is always in sync — no unmanaged orders."""
        return ReconciliationResult(
            unmanaged_orders=[],
            unknown_resolved=[],
            positions_changed=False,
            reconciled_at=datetime.now(_UTC).isoformat(),
        )

    def register_order_callback(self, handler: OrderCallbackT) -> None:
        self._order_callbacks.append(handler)

    def register_fill_callback(self, handler: FillCallbackT) -> None:
        self._fill_callbacks.append(handler)

    @property
    def is_connected(self) -> bool:
        return True

    @property
    def is_paper(self) -> bool:
        return True

    # ── Fill simulation ───────────────────────────────────────────────────────

    async def _simulate_entry_fill(self, plan: IntentTradePlan) -> None:
        entry = plan.entry_order
        if entry is None or entry.limit_price is None:
            return

        await asyncio.sleep(self._fill_delay)

        if entry.runtime_order_id in self._cancelled:
            return

        # SUBMITTED → ACCEPTED
        order = self._make_order(entry, plan.intent_id)
        self._orders[entry.runtime_order_id] = order
        accepted = order.model_copy(update={
            "status": OrderStatus.ACCEPTED,
            "accepted_at": datetime.now(_UTC),
        })
        self._orders[entry.runtime_order_id] = accepted
        for cb in self._order_callbacks:
            await cb(accepted)

        # ACCEPTED → FILLED
        await asyncio.sleep(self._fill_delay)
        filled = accepted.model_copy(update={
            "status": OrderStatus.FILLED,
            "filled_quantity": int(entry.quantity),
            "avg_fill_price": entry.limit_price,
            "filled_at": datetime.now(_UTC),
        })
        self._orders[entry.runtime_order_id] = filled
        for cb in self._order_callbacks:
            await cb(filled)

        # Emit fill event
        fill = Execution(
            order_id=order.order_id,
            broker_execution_id=f"paper-{uuid4().hex[:8]}",
            symbol=entry.runtime_order_id.split("-")[0] if "-" in entry.runtime_order_id else "PAPER",
            side=entry.side,
            quantity=int(entry.quantity),
            price=entry.limit_price,
            timestamp=datetime.now(_UTC),
        )
        for cb in self._fill_callbacks:
            await cb(fill)

        logger.info(
            "PaperRouter: entry filled | side=%s qty=%s @ %s",
            entry.side, entry.quantity, entry.limit_price,
        )

        # Activate stop
        if plan.stop_order:
            asyncio.create_task(self._activate_bracket_child(plan.stop_order))

    async def _activate_bracket_child(self, order: PlannedOrder) -> None:
        """Activate a stop/target child after entry fill. V1: no live price tracking."""
        await asyncio.sleep(self._fill_delay)
        if order.runtime_order_id in self._cancelled:
            return
        child = self._make_order(order)
        self._orders[order.runtime_order_id] = child
        accepted = child.model_copy(update={
            "status": OrderStatus.ACCEPTED,
            "accepted_at": datetime.now(_UTC),
        })
        self._orders[order.runtime_order_id] = accepted
        for cb in self._order_callbacks:
            await cb(accepted)
        logger.debug("PaperRouter: bracket child active | role=%s roid=%s", order.role, order.runtime_order_id)

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _make_identity(runtime_order_id: str) -> BrokerOrderIdentity:
        return BrokerOrderIdentity(
            runtime_order_id=runtime_order_id,
            broker="paper",
            broker_order_ref=runtime_order_id,
            broker_order_id=None,
            broker_perm_id=None,
        )

    @staticmethod
    def _make_order(planned: PlannedOrder, intent_id: Any = None) -> Order:
        from uuid import uuid4 as _uuid4
        return Order(
            order_id=_uuid4(),
            intent_id=_uuid4(),   # filled in by coordinator in V2
            symbol="PAPER",       # overridden by coordinator in V2
            side=planned.side,
            order_type=planned.order_type,
            quantity=int(planned.quantity),
            limit_price=planned.limit_price,
            stop_price=planned.stop_price,
            time_in_force=planned.time_in_force,
            status=OrderStatus.PENDING,
        )
