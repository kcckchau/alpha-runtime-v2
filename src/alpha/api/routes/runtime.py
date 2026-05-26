from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from alpha.config.loader import get_settings
from alpha.config.settings import StorageSettings
from alpha.engines.storage.parquet import ParquetStore
from alpha.models.enums import BarTimeframe, EngineState, HealthStatus
from alpha.runtime_status import read_snapshot
from alpha.timeframe_context import aggregate_monthly_history, rows_to_history_payload

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
        "contexts": {},
        "market_states": {},
        "setups": [],
        "orders": [],
    }


def _parquet_store() -> ParquetStore:
    settings = get_settings()
    return ParquetStore(StorageSettings(parquet_root=settings.storage.parquet_root))


def _sorted_history_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: row["timestamp"])


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


@router.get("/bars/history")
async def list_bar_history(
    symbol: str,
    timeframe: str,
    start: date,
    end: date,
) -> list[dict[str, Any]]:  # type: ignore[type-arg]
    store = _parquet_store()
    normalized_symbol = symbol.upper()
    normalized_timeframe = timeframe.lower()

    if normalized_timeframe == "1mo":
        table = store.read_range("bars/1d", normalized_symbol, start, end)
        rows = aggregate_monthly_history(table.to_pylist())
        return rows_to_history_payload(rows, "1mo")

    timeframe_map = {
        "1m": BarTimeframe.M1,
        "1h": BarTimeframe.H1,
        "1d": BarTimeframe.D1,
    }
    if normalized_timeframe not in timeframe_map:
        return []

    table = store.read_range(
        f"bars/{timeframe_map[normalized_timeframe]}",
        normalized_symbol,
        start,
        end,
    )
    return rows_to_history_payload(_sorted_history_rows(table.to_pylist()), normalized_timeframe)


@router.get("/contexts")
async def list_contexts(symbol: str | None = None) -> dict[str, Any]:
    snapshot = _snapshot_or_default()
    contexts = snapshot["contexts"]
    if symbol is None:
        return contexts
    return {symbol: contexts.get(symbol)}


@router.get("/orders")
async def list_open_orders(symbol: str | None = None) -> list[dict]:  # type: ignore[type-arg]
    snapshot = _snapshot_or_default()
    orders = snapshot["orders"]
    if symbol is None:
        return orders
    return [order for order in orders if order.get("symbol") == symbol]
