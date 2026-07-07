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
from alpha.models.events import AnyEvent, BarEvent, PipelineOutputEvent, QuoteEvent, SetupEvent, ThesisEvent

logger = logging.getLogger(__name__)


def _symbol_broadcast_keys(symbol: str) -> set[str]:
    """Return WS connection keys that should receive events for this symbol."""
    upper = symbol.upper()
    keys = {upper}
    if "-" in upper:
        keys.add(upper.split("-", 1)[0])
    return keys


def _resolve_snapshot_symbol(snapshot: dict, symbol: str) -> str:
    """Map a client symbol (e.g. MNQ) to the snapshot key (e.g. MNQ-09)."""
    bars = snapshot.get("bars") or {}
    upper = symbol.upper()
    if upper in bars:
        return upper
    root = upper.split("-", 1)[0]
    for key in bars:
        if key.split("-", 1)[0] == root:
            return key
    return upper


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
        """Subscribe to the EventBus for bar, quote, setup, and thesis events."""
        from alpha.core.event_bus import EventBus
        if not isinstance(event_bus, EventBus):
            return
        event_bus.subscribe(EventType.BAR, self._on_bar)
        event_bus.subscribe(EventType.QUOTE, self._on_quote, drop_if_full=True)
        event_bus.subscribe(EventType.SETUP, self._on_setup)
        event_bus.subscribe(EventType.THESIS, self._on_thesis)
        event_bus.subscribe(EventType.PIPELINE_OUTPUT, self._on_pipeline_output)
        logger.info("ConnectionManager: subscribed to EventBus (BAR + QUOTE + SETUP + THESIS + PIPELINE_OUTPUT)")

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

    async def _on_setup(self, event: AnyEvent) -> None:
        if not isinstance(event, SetupEvent):
            return
        payload = {
            "type": "setup",
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "setup_id": str(event.setup_id),
            "setup_type": str(event.setup_type),
            "setup_state": str(event.setup_state),
            "prev_state": str(event.prev_state) if event.prev_state else None,
        }
        await self._broadcast(event.symbol, payload)

    async def _on_thesis(self, event: AnyEvent) -> None:
        if not isinstance(event, ThesisEvent):
            return
        payload = {
            "type": "thesis",
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "thesis_id": str(event.thesis_id),
            "thesis_type": str(event.thesis_type),
            "thesis_state": str(event.thesis_state),
            "confidence": event.confidence,
            "bars_alive": event.bars_alive,
            "entry": str(event.entry) if event.entry else None,
            "stop": str(event.stop) if event.stop else None,
            "target": str(event.target) if event.target else None,
            "risk_ratio": event.risk_ratio,
            "evidence_positive": event.evidence_positive,
            "evidence_negative": event.evidence_negative,
            "invalidation_reason": event.invalidation_reason,
        }
        await self._broadcast(event.symbol, payload)

    async def _on_pipeline_output(self, event: AnyEvent) -> None:
        if not isinstance(event, PipelineOutputEvent):
            return
        ms = event.market_state
        thesis = event.thesis
        # Lightweight summary — UI uses this to detect stale panels and trigger refresh.
        payload = {
            "type": "pipeline_complete",
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "pipeline_ts": event.pipeline_ts.isoformat(),
            "flow_available": event.flow_context is not None,
            "market_state_ts": ms.timestamp.isoformat() if ms and hasattr(ms, "timestamp") else None,
            "thesis_type": str(thesis.dominant.thesis_type) if thesis else None,
            "active_setup_count": len(event.setups),
            "scored_setup_count": len(event.scored_setups),
        }
        await self._broadcast(event.symbol, payload)

    # ── Broadcast ─────────────────────────────────────────────────────────────

    async def _broadcast(self, symbol: str, payload: dict) -> None:  # type: ignore[type-arg]
        clients: set[WebSocket] = set()
        for key in _symbol_broadcast_keys(symbol):
            clients.update(self._connections.get(key, set()))
        if not clients:
            return
        dead: set[WebSocket] = set()
        for ws in list(clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)
        for ws in dead:
            for key in _symbol_broadcast_keys(symbol):
                self._connections.get(key, set()).discard(ws)
