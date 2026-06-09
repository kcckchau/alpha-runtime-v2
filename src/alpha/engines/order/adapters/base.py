"""
BrokerAdapter abstraction.

Each broker integration (Alpaca, Interactive Brokers, Tastytrade, etc.)
implements this interface. The OrderEngine never imports broker SDKs directly.

Paper trading is implemented as a BrokerAdapter that simulates fills
against live market prices without touching a real account.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Coroutine, Any

from alpha.models.enums import DataSourceId
from alpha.models.order import Execution, Order, OrderIntent


OrderUpdateHandlerT = Callable[[Order], Coroutine[Any, Any, None]]
ExecutionHandlerT = Callable[[Execution], Coroutine[Any, Any, None]]


class BrokerAdapter(ABC):
    """
    Interface for order routing and execution tracking.

    The adapter:
      - Translates OrderIntent into broker-native order format
      - Calls registered handlers when orders are updated or filled
      - Never applies business logic — just routes and translates
    """

    @property
    @abstractmethod
    def source_id(self) -> DataSourceId: ...

    @property
    @abstractmethod
    def is_paper(self) -> bool:
        """True for paper / simulation adapters."""

    @property
    @abstractmethod
    def is_connected(self) -> bool: ...

    # ── Connection ────────────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    # ── Order management ──────────────────────────────────────────────────────

    @abstractmethod
    async def submit_order(self, intent: OrderIntent) -> Order:
        """Submit an order and return the initial Order object."""

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> bool:
        """Request cancellation. Returns True if request was accepted."""

    @abstractmethod
    async def get_order(self, broker_order_id: str) -> Order | None:
        """Fetch current order state from broker."""

    @abstractmethod
    async def get_open_orders(self) -> list[Order]:
        """Return all open orders from broker."""

    # ── Account ───────────────────────────────────────────────────────────────

    @abstractmethod
    async def get_account_equity(self, account_id: str = "default") -> float:
        """Return current equity in USD for the given logical account."""

    @abstractmethod
    async def get_positions(self, account_id: str = "default") -> dict[str, int]:
        """Return {symbol: shares} for all open positions in the given logical account."""

    @abstractmethod
    async def get_daily_pnl(self, account_id: str = "default") -> tuple[float, float]:
        """Return (realized_pnl, unrealized_pnl) for today from the broker.

        Values are sourced directly from the broker's own P&L accounting, so
        they include positions opened manually or in prior engine sessions.
        """

    @abstractmethod
    async def get_account_summary(self, account_id: str = "default") -> dict[str, float]:
        """Return a snapshot of key account metrics.

        Guaranteed keys (all in USD):
          net_liquidation     – total equity (cash + open positions market value)
          cash_balance        – settled cash not tied up in open positions
          gross_position_value – absolute market value of all open positions
        """

    # ── Streaming callbacks ───────────────────────────────────────────────────

    @abstractmethod
    def on_order_update(self, handler: OrderUpdateHandlerT) -> None:
        """Register a handler called on any order status change."""

    @abstractmethod
    def on_execution(self, handler: ExecutionHandlerT) -> None:
        """Register a handler called on each fill."""
