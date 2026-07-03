from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from alpha.api.routes.runtime import _snapshot_or_default

router = APIRouter(tags=["ws"])


def _resolve_snapshot_symbol(snapshot: dict[str, Any], symbol: str) -> str:
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


def _symbol_payload(snapshot: dict[str, Any], symbol: str) -> dict[str, Any]:
    resolved = _resolve_snapshot_symbol(snapshot, symbol)
    return {
        "type": "runtime_update",
        "symbol": resolved,
        "updated_at": snapshot.get("updated_at"),
        "runtime_state": snapshot.get("runtime_state"),
        "mode": snapshot.get("mode"),
        "runtime_available": snapshot.get("runtime_available", False),
        "quote": snapshot.get("quotes", {}).get(resolved),
        "bar": snapshot.get("bars", {}).get(resolved),
        "context": snapshot.get("contexts", {}).get(resolved),
        "market_state": snapshot.get("market_states", {}).get(resolved),
        "setup_context": snapshot.get("setup_contexts", {}).get(resolved),
        "prev_setup_context": snapshot.get("prev_setup_contexts", {}).get(resolved),
        "setups": [
            setup for setup in snapshot.get("setups", []) if setup.get("symbol") == resolved
        ],
    }


async def _runtime_ws_impl(websocket: WebSocket, symbol: str, interval_ms: int = 1000) -> None:
    await websocket.accept()
    normalized_symbol = symbol.upper()
    sleep_seconds = max(interval_ms, 250) / 1000
    last_payload = ""

    try:
        while True:
            payload = _symbol_payload(_snapshot_or_default(), normalized_symbol)
            serialized = json.dumps(payload, sort_keys=True, default=str)
            if serialized != last_payload:
                await websocket.send_json(payload)
                last_payload = serialized
            await asyncio.sleep(sleep_seconds)
    except WebSocketDisconnect:
        return


@router.websocket("/runtime/ws")
async def runtime_ws(websocket: WebSocket, symbol: str, interval_ms: int = 1000) -> None:
    await _runtime_ws_impl(websocket, symbol, interval_ms)


@router.websocket("/ws/stream")
async def runtime_ws_legacy(
    websocket: WebSocket,
    symbol: str,
    timeframe: str | None = None,
    interval_ms: int = 1000,
) -> None:
    _ = timeframe
    await _runtime_ws_impl(websocket, symbol, interval_ms)


@router.websocket("/runtime/ws/live")
async def runtime_ws_live(websocket: WebSocket, symbol: str) -> None:
    """
    Push-based WebSocket endpoint.

    Broadcasts BarEvent and QuoteEvent messages immediately as they arrive
    from the EventBus — no polling, no file reads. Only active when the
    API runs inside `alpha run` (requires injected EventBus).

    Message types:
      {"type": "bar",   "symbol": ..., "timeframe": ..., "timestamp": ...,
       "open": ..., "high": ..., "low": ..., "close": ..., "volume": ...,
       "vwap": ..., "partial": bool}
      {"type": "quote", "symbol": ..., "timestamp": ...,
       "bid_price": ..., "ask_price": ..., "last_price": ...,
       "bid_size": ..., "ask_size": ...}
    """
    manager = getattr(websocket.app.state, "connection_manager", None)
    if manager is None:
        await websocket.close(code=1013, reason="ConnectionManager not available")
        return

    normalized = symbol.upper()
    await manager.connect(websocket, normalized)
    try:
        # Hold the connection open; manager broadcasts on incoming events.
        # We also send periodic pings to detect dead connections.
        while True:
            await asyncio.sleep(30)
            try:
                await websocket.send_json({"type": "ping"})
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket, normalized)
