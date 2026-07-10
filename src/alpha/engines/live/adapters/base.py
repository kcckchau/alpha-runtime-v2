"""
LiveFeedAdapter abstraction.

Each broker/data-vendor integration for live streaming implements this interface.
The LiveIngestionEngine only works with this interface — no broker SDK
leaks outside the adapter directory.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, Coroutine, Any

from alpha.models.enums import BarTimeframe, DataSourceId
from alpha.models.events import BarEvent, OrderBookEvent, QuoteEvent, TradeEvent

BarHandlerT = Callable[[BarEvent], Coroutine[Any, Any, None]]
TradeHandlerT = Callable[[TradeEvent], Coroutine[Any, Any, None]]
QuoteHandlerT = Callable[[QuoteEvent], Coroutine[Any, Any, None]]
BookHandlerT = Callable[[OrderBookEvent], Coroutine[Any, Any, None]]
# Synchronous tick handler — bypasses the event bus for high-frequency trade ticks.
# Called directly from the adapter callback with (symbol, price, size).
TickHandlerT = Callable[[str, float, int], None]


class LiveFeedAdapter(ABC):
    """
    Streaming adapter for a single data vendor.

    The adapter is responsible for:
      - Connection management (reconnect with backoff)
      - Calling the registered handlers with normalized events only
      - Never raising inside a handler — log and continue
    """

    @property
    @abstractmethod
    def source_id(self) -> DataSourceId: ...

    @property
    @abstractmethod
    def is_connected(self) -> bool: ...

    # ── Connection ────────────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None:
        """Establish the streaming connection."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Cleanly close the connection and cancel all subscriptions."""

    # ── Subscriptions ─────────────────────────────────────────────────────────

    @abstractmethod
    async def subscribe_bars(
        self,
        symbols: list[str],
        timeframe: BarTimeframe,
        handler: BarHandlerT,
        *,
        start: "datetime | None" = None,
    ) -> None:
        """Subscribe to real-time bars. Handler called on each new bar.

        start: replay from this point in history before going real-time.
               Only supported by adapters that implement gateway replay
               (e.g. Databento live). Ignored by others.
        """

    @abstractmethod
    async def subscribe_trades(
        self,
        symbols: list[str],
        handler: TradeHandlerT,
    ) -> None:
        """Subscribe to trade ticks."""

    @abstractmethod
    async def subscribe_quotes(
        self,
        symbols: list[str],
        handler: QuoteHandlerT,
    ) -> None:
        """Subscribe to NBBO quote updates."""

    async def subscribe_order_book(
        self,
        symbols: list[str],
        handler: BookHandlerT,
        depth: int = 10,
    ) -> None:
        """Subscribe to L2 order book. Optional — default raises."""
        raise NotImplementedError(f"{self.source_id} does not support order book")

    async def subscribe_tick_trades(
        self,
        symbols: list[str],
        handler: TickHandlerT,
    ) -> None:
        """Subscribe to individual trade ticks (tick-by-tick). Optional — default no-ops.

        Handler is synchronous and called directly from the adapter callback —
        bypasses the event bus to support 200+ ticks/sec without asyncio overhead.
        Signature: handler(symbol: str, price: float, size: int) -> None
        """

    # ── Symbol management ─────────────────────────────────────────────────────

    @abstractmethod
    async def add_symbols(self, symbols: list[str]) -> None:
        """Add symbols to an active subscription."""

    @abstractmethod
    async def remove_symbols(self, symbols: list[str]) -> None:
        """Remove symbols from an active subscription."""
