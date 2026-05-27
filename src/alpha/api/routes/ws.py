from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from alpha.api.routes.runtime import _snapshot_or_default

router = APIRouter(tags=["ws"])


def _symbol_payload(snapshot: dict[str, Any], symbol: str) -> dict[str, Any]:
    return {
        "type": "runtime_update",
        "symbol": symbol,
        "updated_at": snapshot.get("updated_at"),
        "runtime_state": snapshot.get("runtime_state"),
        "mode": snapshot.get("mode"),
        "runtime_available": snapshot.get("runtime_available", False),
        "quote": snapshot.get("quotes", {}).get(symbol),
        "bar": snapshot.get("bars", {}).get(symbol),
        "context": snapshot.get("contexts", {}).get(symbol),
        "setup_context": snapshot.get("setup_contexts", {}).get(symbol),
        "setups": [
            setup for setup in snapshot.get("setups", []) if setup.get("symbol") == symbol
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
    # `timeframe` is accepted for backwards compatibility with the older client
    # websocket contract. The current stream payload is symbol-scoped and emits
    # the latest snapshot data regardless of timeframe.
    _ = timeframe
    await _runtime_ws_impl(websocket, symbol, interval_ms)
