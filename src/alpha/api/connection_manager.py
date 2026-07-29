"""
WebSocket connection manager.

Subscribes to the EventBus and broadcasts normalized bar and quote events
directly to connected browser clients as they arrive — no polling, no file reads.

Broker-agnostic: only consumes normalized BarEvent / QuoteEvent from the EventBus.
The source (Databento, IBKR, etc.) is irrelevant here.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from decimal import Decimal

from fastapi import WebSocket

from alpha.models.enums import BarTimeframe, EventType
from alpha.models.events import AnyEvent, BarEvent, PipelineOutputEvent, PositionSignalEvent, QuoteEvent, SetupEvent, ThesisEvent

logger = logging.getLogger(__name__)

# A single slow/dead client must never stall delivery to the rest — bound each
# client's send and drop it if it doesn't complete in time.
_SEND_TIMEOUT_S = 2.0


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
        event_bus.subscribe(EventType.POSITION_SIGNAL, self._on_position_signal)
        logger.info("ConnectionManager: subscribed to EventBus (BAR + QUOTE + SETUP + THESIS + PIPELINE_OUTPUT + POSITION_SIGNAL)")

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
        fc = event.flow_context

        flow_payload = None
        if fc is not None:
            split = None
            if fc.split is not None:
                split = {
                    "sweep_volume": fc.split.sweep_volume,
                    "sweep_delta": fc.split.sweep_delta,
                    "recovery_volume": fc.split.recovery_volume,
                    "recovery_delta": fc.split.recovery_delta,
                    "recovery_ratio": round(fc.split.recovery_ratio, 2) if fc.split.recovery_ratio is not None else None,
                }
            flow_payload = {
                "available": True,
                "total_volume": fc.total_volume,
                "buy_volume": fc.buy_volume,
                "sell_volume": fc.sell_volume,
                "delta": fc.delta,
                "delta_pct": round(fc.delta_pct, 1) if fc.delta_pct is not None else None,
                "large_buy_count": fc.large_buy_count,
                "large_sell_count": fc.large_sell_count,
                "large_trade_threshold": fc.large_trade_threshold,
                "bid_ask_imbalance": round(fc.bid_ask_imbalance, 3) if fc.bid_ask_imbalance is not None else None,
                "twap_bid_size": round(fc.twap_bid_size, 1) if fc.twap_bid_size is not None else None,
                "twap_ask_size": round(fc.twap_ask_size, 1) if fc.twap_ask_size is not None else None,
                "trade_count": fc.trade_count,
                "trade_velocity": round(fc.trade_velocity, 2) if fc.trade_velocity is not None else None,
                "avg_trade_size": round(fc.avg_trade_size, 1) if fc.avg_trade_size is not None else None,
                "is_genuine_sweep_reversal": fc.is_genuine_sweep_reversal,
                "is_v_reversal": fc.is_v_reversal,
                "absorption": {
                    "detected": fc.absorption.detected,
                    "confidence": round(fc.absorption.confidence, 2),
                    "sell_volume_at_low": fc.absorption.sell_volume_at_low,
                },
                "split": split,
                "has_trade_data": fc.has_trade_data,
                "has_quote_data": fc.has_quote_data,
            }

        payload = {
            "type": "pipeline_complete",
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "pipeline_ts": event.pipeline_ts.isoformat(),
            "flow_available": fc is not None,
            "flow": flow_payload,
            "market_state_ts": ms.timestamp.isoformat() if ms and hasattr(ms, "timestamp") else None,
            "thesis_type": str(thesis.dominant.thesis_type) if thesis else None,
            "active_setup_count": len(event.setups),
            "scored_setup_count": len(event.scored_setups),
        }
        await self._broadcast(event.symbol, payload)

    async def _on_position_signal(self, event: AnyEvent) -> None:
        if not isinstance(event, PositionSignalEvent):
            return
        payload = {
            "type": "position_signal",
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "signal_type": event.signal_type,
            "setup_id": str(event.setup_id),
            "setup_type": str(event.setup_type),
            "direction": str(event.direction),
            "entry_price": str(event.entry_price),
            "stop": str(event.stop),
            "target": str(event.target),
            "current_price": str(event.current_price),
            "grade": str(event.grade),
            "intrabar_delta": event.intrabar_delta,
            "bid_ask_imbalance": event.bid_ask_imbalance,
            "exit_reason": event.exit_reason,
            "pnl_pts": str(event.pnl_pts) if event.pnl_pts is not None else None,
            "bars_held": event.bars_held,
            "bars_held_so_far": event.bars_held_so_far,
            "mfe": str(event.mfe) if event.mfe is not None else None,
            "mae": str(event.mae) if event.mae is not None else None,
        }
        await self._broadcast(event.symbol, payload)

    async def broadcast(self, symbol: str, payload: dict) -> None:  # type: ignore[type-arg]
        """Public broadcast — for use by engines that need to push directly to WS clients."""
        await self._broadcast(symbol, payload)

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
                await asyncio.wait_for(ws.send_json(payload), timeout=_SEND_TIMEOUT_S)
            except Exception:
                dead.add(ws)
        for ws in dead:
            for key in _symbol_broadcast_keys(symbol):
                self._connections.get(key, set()).discard(ws)
            try:
                await ws.close()
            except Exception:
                pass
