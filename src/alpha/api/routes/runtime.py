from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from alpha.config.loader import get_settings
from alpha.models.enums import EngineState, HealthStatus
from alpha.runtime_status import read_snapshot

router = APIRouter(prefix="/runtime", tags=["runtime"])


class EngineStatusItem(BaseModel):
    name: str
    state: EngineState
    health: HealthStatus
    details: dict[str, Any] = {}


class RuntimeStatusResponse(BaseModel):
    mode: str
    symbols: list[str]
    engines: list[EngineStatusItem]
    runtime_state: str = "unknown"
    updated_at: str | None = None
    runtime_available: bool = False


def _snapshot_or_default() -> dict[str, Any]:
    settings = get_settings()
    snapshot = read_snapshot(settings)
    if snapshot is not None:
        snapshot["runtime_available"] = True
        return snapshot
    return {
        "mode": str(settings.runtime.mode),
        "symbols": settings.runtime.symbols,
        "engines": [],
        "runtime_state": "unknown",
        "updated_at": None,
        "runtime_available": False,
        "quotes": {},
        "bars": {},
        "market_states": {},
        "setups": [],
        "orders": [],
    }


@router.get("/status", response_model=RuntimeStatusResponse)
async def runtime_status() -> RuntimeStatusResponse:
    """Return current runtime mode, symbols, and engine health snapshot."""
    snapshot = _snapshot_or_default()
    return RuntimeStatusResponse(
        mode=snapshot["mode"],
        symbols=snapshot["symbols"],
        engines=snapshot["engines"],
        runtime_state=snapshot["runtime_state"],
        updated_at=snapshot["updated_at"],
        runtime_available=snapshot["runtime_available"],
    )


@router.get("/symbols")
async def list_symbols() -> list[str]:
    snapshot = _snapshot_or_default()
    return snapshot["symbols"]


@router.get("/setups")
async def list_active_setups(symbol: str | None = None) -> list[dict]:  # type: ignore[type-arg]
    snapshot = _snapshot_or_default()
    setups = snapshot["setups"]
    if symbol is None:
        return setups
    return [setup for setup in setups if setup.get("symbol") == symbol]


@router.get("/quotes")
async def list_latest_quotes(symbol: str | None = None) -> dict[str, Any]:
    snapshot = _snapshot_or_default()
    quotes = snapshot["quotes"]
    if symbol is None:
        return quotes
    return {symbol: quotes.get(symbol)}


@router.get("/bars")
async def list_latest_bars(symbol: str | None = None) -> dict[str, Any]:
    snapshot = _snapshot_or_default()
    bars = snapshot["bars"]
    if symbol is None:
        return bars
    return {symbol: bars.get(symbol)}


@router.get("/orders")
async def list_open_orders(symbol: str | None = None) -> list[dict]:  # type: ignore[type-arg]
    snapshot = _snapshot_or_default()
    orders = snapshot["orders"]
    if symbol is None:
        return orders
    return [order for order in orders if order.get("symbol") == symbol]
