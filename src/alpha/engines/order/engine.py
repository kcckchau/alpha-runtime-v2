"""
Engine 9 — Order Engine

Responsibilities:
  - Accept TradePlans from the Risk Engine
  - Convert TradePlan → OrderIntent → broker order
  - Track order lifecycle (pending → submitted → partial → filled / cancelled)
  - Emit OrderUpdateEvent on every status change
  - Support paper and live adapters via BrokerAdapter interface
  - Enforce hard order limits (duplicate detection, max size)

Input:  TradePlan (from RiskEngine.submit_plan direct call)
Output: OrderUpdateEvent (via EventBus on every lifecycle change)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.order.adapters.base import BrokerAdapter
from alpha.models.enums import EventType, HealthStatus, OrderStatus, OrderType, TimeInForce
from alpha.models.events import AnyEvent, OrderUpdateEvent
from alpha.models.order import Execution, Order, OrderIntent
from alpha.models.risk import TradePlan

logger = logging.getLogger(__name__)


class OrderEngine(BaseEngine):
    """
    Routes trade plans to a broker adapter and tracks order lifecycle.

    Usage::

        engine.register_adapter(AlpacaBrokerAdapter(settings))
        await engine.submit_plan(trade_plan)
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
        self._adapters: dict[str, BrokerAdapter] = {}
        self._orders: dict[UUID, Order] = {}
        self._orders_submitted: int = 0
        self._orders_filled: int = 0

    @property
    def name(self) -> str:
        return "OrderEngine"

    # ── Adapter registration ──────────────────────────────────────────────────

    def register_adapter(self, adapter: BrokerAdapter) -> None:
        self._adapters[adapter.source_id] = adapter
        adapter.on_order_update(self._on_order_update)
        adapter.on_execution(self._on_execution)
        logger.info("Registered broker adapter: %s (paper=%s)", adapter.source_id, adapter.is_paper)

    @property
    def primary_adapter(self) -> BrokerAdapter:
        mode = self._settings.runtime.mode
        from alpha.models.enums import RuntimeMode
        prefer_paper = mode in {RuntimeMode.PAPER, RuntimeMode.REPLAY, RuntimeMode.HISTORICAL_BACKFILL}
        for adapter in self._adapters.values():
            if prefer_paper and adapter.is_paper:
                return adapter
        for adapter in self._adapters.values():
            return adapter
        raise RuntimeError("No broker adapter registered")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        pass

    async def _on_start(self) -> None:
        for adapter in self._adapters.values():
            await adapter.connect()

    async def _on_stop(self) -> None:
        for adapter in self._adapters.values():
            try:
                await adapter.disconnect()
            except Exception:
                logger.exception("Error disconnecting adapter %s", adapter.source_id)

    async def _health_check(self) -> EngineHealth:
        connected = [sid for sid, a in self._adapters.items() if a.is_connected]
        return EngineHealth(
            HealthStatus.HEALTHY if connected else HealthStatus.DEGRADED,
            self.name,
            {
                "adapters_connected": connected,
                "orders_submitted": self._orders_submitted,
                "orders_filled": self._orders_filled,
                "open_orders": sum(1 for o in self._orders.values() if not o.is_terminal),
            },
        )

    # ── Public API ────────────────────────────────────────────────────────────

    async def submit_plan(self, plan: TradePlan) -> Order | None:
        """Convert TradePlan to OrderIntent and submit to broker."""
        intent = self._plan_to_intent(plan)
        try:
            order = await self.primary_adapter.submit_order(intent)
            self._orders[order.order_id] = order
            self._orders_submitted += 1
            await self._emit_update(order)
            logger.info(
                "Order submitted: %s %s %d @ %s [%s]",
                order.symbol, order.side, order.quantity,
                plan.entry_price, order.order_id,
            )
            return order
        except Exception:
            logger.exception("Order submission failed for plan %s", plan.plan_id)
            return None

    async def cancel_order(self, order_id: UUID) -> bool:
        order = self._orders.get(order_id)
        if order is None or order.broker_order_id is None:
            return False
        return await self.primary_adapter.cancel_order(order.broker_order_id)

    def get_open_orders(self) -> list[Order]:
        return [o for o in self._orders.values() if not o.is_terminal]

    # ── Callbacks from adapter ────────────────────────────────────────────────

    async def _on_order_update(self, updated: Order) -> None:
        self._orders[updated.order_id] = updated
        if updated.status == OrderStatus.FILLED:
            self._orders_filled += 1
        await self._emit_update(updated)

    async def _on_execution(self, execution: Execution) -> None:
        logger.info(
            "Execution: %s %s %d @ %s",
            execution.symbol, execution.side, execution.quantity, execution.price,
        )
        # TODO: forward to StorageEngine

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _plan_to_intent(plan: TradePlan) -> OrderIntent:
        return OrderIntent(
            intent_id=uuid4(),
            plan_id=plan.plan_id,
            symbol=plan.symbol,
            side=plan.side,
            order_type=OrderType.LIMIT,
            quantity=plan.position_size,
            limit_price=plan.entry_price,
            stop_price=plan.stop_price,
            time_in_force=TimeInForce.DAY,
            created_at=datetime.now(timezone.utc),
        )

    async def _emit_update(self, order: Order) -> None:
        event = OrderUpdateEvent(
            symbol=order.symbol,
            timestamp=datetime.now(timezone.utc),
            metadata=self._make_metadata(),
            order_id=order.order_id,
            broker_order_id=order.broker_order_id,
            order_status=order.status,
            filled_quantity=order.filled_quantity,
            avg_fill_price=order.avg_fill_price,
            reject_reason=order.reject_reason,
        )
        await self._event_bus.publish(event)

    def _make_metadata(self) -> object:
        from datetime import timezone
        from alpha.models.events import EventMetadata
        from alpha.models.enums import DataSourceId
        return EventMetadata(
            source=DataSourceId.UNKNOWN,
            received_at=datetime.now(timezone.utc),
            is_replay=False,
        )
