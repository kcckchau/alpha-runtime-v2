"""
WebSocket connection manager.

Subscribes to the EventBus and broadcasts normalized bar and quote events
directly to connected browser clients as they arrive — no polling, no file reads.

Broker-agnostic: only consumes normalized BarEvent / QuoteEvent from the EventBus.
The source (Databento, IBKR, etc.) is irrelevant here.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from decimal import Decimal

from fastapi import WebSocket

from alpha.models.enums import BarTimeframe, EventType
from alpha.models.events import AnyEvent, BarEvent, QuoteEvent

logger = logging.getLogger(__name__)


def _decimal_str(v: Decimal | None) -> str | None:
    return str(v) if v is not None else None


class ConnectionManager:
    """
    Manages active WebSocket connections and fans out live market data.

    Usage:
        manager = ConnectionManager()
        manager.subscribe_to_event_bus(event_bus)   # call once at startup
        # Then in WS endpoint:
        await manager.connect(websocket, symbol)
        try:
            await websocket.wait_for_disconnect()
        finally:
            manager.disconnect(websocket, symbol)
    """

    # Timeframes that update _latest_bars snapshot — sub-minute bars are
    # broadcast only and should not overwrite the chart's persistent state.
    _SNAPSHOT_TIMEFRAMES = {BarTimeframe.M1, BarTimeframe.M5, BarTimeframe.H1, BarTimeframe.D1}

    def __init__(self) -> None:
        # symbol → set of connected WebSockets
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)

    # ── Connection management ─────────────────────────────────────────────────

    async def connect(self, websocket: WebSocket, symbol: str) -> None:
        await websocket.accept()
        self._connections[symbol.upper()].add(websocket)
        logger.debug("ConnectionManager: client connected for %s", symbol)

    def disconnect(self, websocket: WebSocket, symbol: str) -> None:
        self._connections[symbol.upper()].discard(websocket)
        logger.debug("ConnectionManager: client disconnected for %s", symbol)

    # ── EventBus subscription ─────────────────────────────────────────────────

    def subscribe_to_event_bus(self, event_bus: object) -> None:
        """Subscribe to the EventBus for bar and quote events."""
        from alpha.core.event_bus import EventBus
        if not isinstance(event_bus, EventBus):
            return
        event_bus.subscribe(EventType.BAR, self._on_bar)
        event_bus.subscribe(EventType.QUOTE, self._on_quote, drop_if_full=True)
        logger.info("ConnectionManager: subscribed to EventBus (BAR + QUOTE)")

    # ── EventBus handlers ─────────────────────────────────────────────────────

    async def _on_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return
        payload = {
            "type": "bar",
            "symbol": event.symbol,
            "timeframe": event.timeframe,
            "timestamp": event.timestamp.isoformat(),
            "open": str(event.open),
            "high": str(event.high),
            "low": str(event.low),
            "close": str(event.close),
            "volume": event.volume,
            "vwap": _decimal_str(event.vwap),
            "partial": event.is_partial,
        }
        await self._broadcast(event.symbol, payload)

    async def _on_quote(self, event: AnyEvent) -> None:
        if not isinstance(event, QuoteEvent):
            return
        payload = {
            "type": "quote",
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "bid_price": str(event.bid_price),
            "ask_price": str(event.ask_price),
            "bid_size": event.bid_size,
            "ask_size": event.ask_size,
            "last_price": _decimal_str(event.last_price),
            "last_size": event.last_size,
        }
        await self._broadcast(event.symbol, payload)

    # ── Broadcast ─────────────────────────────────────────────────────────────

    async def _broadcast(self, symbol: str, payload: dict) -> None:  # type: ignore[type-arg]
        clients = self._connections.get(symbol.upper(), set())
        if not clients:
            return
        dead: set[WebSocket] = set()
        for ws in list(clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)
        for ws in dead:
            clients.discard(ws)
