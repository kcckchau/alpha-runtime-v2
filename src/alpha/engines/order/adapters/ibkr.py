"""
IBKR broker adapter.

Routes OrderIntent → ib_insync LimitOrder/StopOrder, monitors fills,
and calls registered handlers on every order and execution event.

Order ID mapping:
  - IBKR assigns an integer permId (permanent) and a session orderId
  - We store the IBKR permId as broker_order_id on our Order object
  - Cancellation uses the session Trade object kept in self._trades
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Coroutine, Any
from uuid import UUID, uuid4

from alpha.config.settings import IBKRSettings
from alpha.core.registry import SymbolRegistry
from alpha.engines.ibkr.connection import IBKRConnection
from alpha.engines.ibkr.contracts import make_contract
from alpha.engines.order.adapters.base import BrokerAdapter, ExecutionHandlerT, OrderUpdateHandlerT
from alpha.models.enums import DataSourceId, OrderSide, OrderStatus, OrderType
from alpha.models.order import Execution, Order, OrderIntent

logger = logging.getLogger(__name__)
_UTC = timezone.utc

_IBKR_STATUS_MAP: dict[str, OrderStatus] = {
    "PendingSubmit":  OrderStatus.PENDING,
    "PendingCancel":  OrderStatus.PENDING,
    "PreSubmitted":   OrderStatus.SUBMITTED,
    "Submitted":      OrderStatus.ACCEPTED,
    "ApiPending":     OrderStatus.PENDING,
    "ApiCancelled":   OrderStatus.CANCELLED,
    "Cancelled":      OrderStatus.CANCELLED,
    "Filled":         OrderStatus.FILLED,
    "Inactive":       OrderStatus.REJECTED,
}


class IBKRBrokerAdapter(BrokerAdapter):
    """
    Routes orders to IBKR via ib_insync and tracks their lifecycle.

    Supports:
      - LimitOrder (entry)
      - StopOrder (stop loss)
      - Paper trading (IBKR paper account) and live accounts
    """

    def __init__(
        self,
        conn: IBKRConnection,
        settings: IBKRSettings,
        registry: SymbolRegistry,
    ) -> None:
        self._conn = conn
        self._settings = settings
        self._registry = registry
        self._order_handlers: list[OrderUpdateHandlerT] = []
        self._exec_handlers: list[ExecutionHandlerT] = []
        # Maps our order_id → ib_insync Trade object
        self._trades: dict[UUID, object] = {}
        # Maps IBKR permId → our order_id
        self._perm_to_ours: dict[int, UUID] = {}

    @property
    def source_id(self) -> DataSourceId:
        return DataSourceId.INTERACTIVE_BROKERS

    @property
    def is_paper(self) -> bool:
        return self._settings.is_paper

    @property
    def is_connected(self) -> bool:
        return self._conn.is_connected

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        ib = await self._conn.get()
        ib.orderStatusEvent += self._on_order_status
        ib.execDetailsEvent += self._on_exec_details
        logger.info("IBKR broker adapter connected (paper=%s)", self.is_paper)

    async def disconnect(self) -> None:
        if self._conn.is_connected:
            ib = await self._conn.get()
            ib.orderStatusEvent -= self._on_order_status
            ib.execDetailsEvent -= self._on_exec_details

    # ── Order management ──────────────────────────────────────────────────────

    async def submit_order(self, intent: OrderIntent) -> Order:
        ib = await self._conn.get()
        sym = self._registry.get(intent.symbol)
        contract = make_contract(sym)
        ib_order = self._build_ib_order(intent)

        trade = ib.placeOrder(contract, ib_order)
        order_id = uuid4()

        self._trades[order_id] = trade
        # permId assigned asynchronously; we'll map it in the status callback
        asyncio.ensure_future(self._map_perm_id(order_id, trade))

        order = Order(
            order_id=order_id,
            intent_id=intent.intent_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            quantity=intent.quantity,
            limit_price=intent.limit_price,
            stop_price=intent.stop_price,
            time_in_force=intent.time_in_force,
            status=OrderStatus.PENDING,
            submitted_at=datetime.now(_UTC),
        )
        logger.info(
            "Submitted IBKR order: %s %s %d @ %s",
            intent.symbol, intent.side, intent.quantity, intent.limit_price,
        )
        return order

    async def cancel_order(self, broker_order_id: str) -> bool:
        ib = await self._conn.get()
        perm_id = int(broker_order_id)
        order_id = self._perm_to_ours.get(perm_id)
        if order_id is None:
            logger.warning("cancel_order: permId %s not found", broker_order_id)
            return False
        trade = self._trades.get(order_id)
        if trade is None:
            return False
        ib.cancelOrder(trade.order)  # type: ignore[union-attr]
        return True

    async def get_order(self, broker_order_id: str) -> Order | None:
        # TODO: look up from in-memory trade map
        return None

    async def get_open_orders(self) -> list[Order]:
        ib = await self._conn.get()
        trades = ib.openTrades()
        return []  # TODO: convert to our Order model

    # ── Account ───────────────────────────────────────────────────────────────

    async def get_account_equity(self) -> float:
        ib = await self._conn.get()
        vals = ib.accountValues()
        for v in vals:
            if v.tag == "NetLiquidation" and v.currency == "USD":
                return float(v.value)
        return 0.0

    async def get_positions(self) -> dict[str, int]:
        ib = await self._conn.get()
        return {
            p.contract.symbol: int(p.position)
            for p in ib.positions()
            if p.position != 0
        }

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def on_order_update(self, handler: OrderUpdateHandlerT) -> None:
        self._order_handlers.append(handler)

    def on_execution(self, handler: ExecutionHandlerT) -> None:
        self._exec_handlers.append(handler)

    # ── IBKR event handlers ───────────────────────────────────────────────────

    def _on_order_status(self, trade: object) -> None:
        asyncio.ensure_future(self._handle_order_status(trade))

    async def _handle_order_status(self, trade: object) -> None:
        ibkr_status = getattr(trade, "orderStatus", None)
        if ibkr_status is None:
            return
        status_str = getattr(ibkr_status, "status", "")
        our_status = _IBKR_STATUS_MAP.get(status_str, OrderStatus.PENDING)
        perm_id = getattr(trade.order, "permId", 0)  # type: ignore[union-attr]
        order_id = self._perm_to_ours.get(perm_id)
        if order_id is None:
            return

        filled = int(getattr(ibkr_status, "filled", 0))
        avg_price = float(getattr(ibkr_status, "avgFillPrice", 0.0))

        order = Order(
            order_id=order_id,
            broker_order_id=str(perm_id),
            intent_id=uuid4(),  # placeholder
            symbol=trade.contract.symbol,  # type: ignore[union-attr]
            side=OrderSide.BUY,  # TODO: pull from intent map
            order_type=OrderType.LIMIT,
            quantity=int(getattr(trade.order, "totalQuantity", 0)),  # type: ignore[union-attr]
            filled_quantity=filled,
            avg_fill_price=Decimal(str(round(avg_price, 4))) if avg_price else None,
            status=our_status,
            filled_at=datetime.now(_UTC) if our_status == OrderStatus.FILLED else None,
        )
        for h in self._order_handlers:
            asyncio.ensure_future(h(order))

    def _on_exec_details(self, trade: object, fill: object) -> None:
        asyncio.ensure_future(self._handle_fill(trade, fill))

    async def _handle_fill(self, trade: object, fill: object) -> None:
        exec_obj = getattr(fill, "execution", None)
        if exec_obj is None:
            return
        perm_id = getattr(trade.order, "permId", 0)  # type: ignore[union-attr]
        order_id = self._perm_to_ours.get(perm_id, uuid4())

        execution = Execution(
            order_id=order_id,
            broker_execution_id=getattr(exec_obj, "execId", None),
            symbol=trade.contract.symbol,  # type: ignore[union-attr]
            side=OrderSide.BUY,  # TODO: from exec_obj.side
            quantity=int(getattr(exec_obj, "shares", 0)),
            price=Decimal(str(round(getattr(exec_obj, "avgPrice", 0.0), 4))),
            timestamp=datetime.now(_UTC),
        )
        logger.info(
            "Fill: %s %d @ %s [execId=%s]",
            execution.symbol, execution.quantity, execution.price,
            execution.broker_execution_id,
        )
        for h in self._exec_handlers:
            asyncio.ensure_future(h(execution))

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _map_perm_id(self, order_id: UUID, trade: object) -> None:
        """Wait for IBKR to assign a permId, then register the mapping."""
        for _ in range(20):
            perm_id = getattr(getattr(trade, "order", None), "permId", 0)
            if perm_id:
                self._perm_to_ours[perm_id] = order_id
                return
            await asyncio.sleep(0.5)
        logger.warning("Could not obtain permId for order %s", order_id)

    @staticmethod
    def _build_ib_order(intent: OrderIntent) -> object:
        from ib_insync import LimitOrder, MarketOrder, StopOrder

        action = "BUY" if intent.side == OrderSide.BUY else "SELL"
        qty = intent.quantity

        if intent.order_type == OrderType.MARKET:
            return MarketOrder(action, qty)
        if intent.order_type == OrderType.LIMIT:
            return LimitOrder(action, qty, float(intent.limit_price or 0))
        if intent.order_type == OrderType.STOP:
            return StopOrder(action, qty, float(intent.stop_price or 0))
        raise ValueError(f"Unsupported order type for IBKR: {intent.order_type}")
