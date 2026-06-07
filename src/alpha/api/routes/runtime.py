from __future__ import annotations

from datetime import date, datetime, timezone
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


_SNAPSHOT_STALE_SECONDS = 60


def _snapshot_or_default() -> dict[str, Any]:
    settings = get_settings()
    snapshot = read_snapshot(settings)
    if snapshot is not None:
        updated_at = snapshot.get("updated_at")
        if updated_at:
            try:
                age = datetime.now(timezone.utc) - datetime.fromisoformat(updated_at)
                if age.total_seconds() <= _SNAPSHOT_STALE_SECONDS:
                    snapshot["runtime_available"] = True
                    return snapshot
            except ValueError:
                pass
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
        "setup_contexts": {},
        "prev_setup_contexts": {},
        "orders": [],
    }


def _parquet_store() -> ParquetStore:
    settings = get_settings()
    return ParquetStore(StorageSettings(parquet_root=settings.storage.parquet_root))


def _sorted_history_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: row["timestamp"])


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _aggregate_history_rows(
    rows: list[dict[str, Any]],
    timeframe: str,
) -> list[dict[str, Any]]:
    if timeframe not in {"15m", "1h"}:
        return rows

    bucket_minutes = 15 if timeframe == "15m" else 60
    buckets: dict[str, list[dict[str, Any]]] = {}

    for row in _sorted_history_rows(rows):
        ts = _parse_timestamp(row["timestamp"])
        if bucket_minutes == 60:
            bucket = ts.replace(minute=0, second=0, microsecond=0)
        else:
            minute = (ts.minute // bucket_minutes) * bucket_minutes
            bucket = ts.replace(minute=minute, second=0, microsecond=0)
        buckets.setdefault(bucket.isoformat(), []).append(row)

    aggregated: list[dict[str, Any]] = []
    for bucket_key in sorted(buckets.keys()):
        bucket_rows = buckets[bucket_key]
        first = bucket_rows[0]
        last = bucket_rows[-1]
        aggregated.append(
            {
                "symbol": first["symbol"],
                "timestamp": bucket_key,
                "open": first["open"],
                "high": str(max(float(item["high"]) for item in bucket_rows)),
                "low": str(min(float(item["low"]) for item in bucket_rows)),
                "close": last["close"],
                "volume": sum(int(item["volume"]) for item in bucket_rows),
                "vwap": last.get("vwap"),
                "trade_count": sum(
                    int(item.get("trade_count") or 0) for item in bucket_rows
                ) or None,
                "is_partial": any(bool(item.get("is_partial")) for item in bucket_rows),
                "source": first.get("source"),
                "is_replay": first.get("is_replay"),
            }
        )
    return aggregated


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


@router.get("/setup-contexts")
async def list_setup_contexts(symbol: str | None = None) -> dict[str, Any]:
    snapshot = _snapshot_or_default()
    contexts = snapshot["setup_contexts"]
    if symbol is None:
        return contexts
    return {symbol: contexts.get(symbol)}


@router.get("/prev-setup-contexts")
async def list_prev_setup_contexts(symbol: str | None = None) -> dict[str, Any]:
    snapshot = _snapshot_or_default()
    contexts = snapshot["prev_setup_contexts"]
    if symbol is None:
        return contexts
    return {symbol: contexts.get(symbol)}


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
        "5m": BarTimeframe.M5,
        "1h": BarTimeframe.H1,
        "1d": BarTimeframe.D1,
    }
    if normalized_timeframe == "15m":
        table = store.read_range(
            f"bars/{BarTimeframe.M5}",
            normalized_symbol,
            start,
            end,
        )
        rows = _aggregate_history_rows(table.to_pylist(), "15m")
        return rows_to_history_payload(rows, "15m")
    if normalized_timeframe not in timeframe_map:
        return []

    table = store.read_range(
        f"bars/{timeframe_map[normalized_timeframe]}",
        normalized_symbol,
        start,
        end,
    )
    rows = _sorted_history_rows(table.to_pylist())
    return rows_to_history_payload(rows, normalized_timeframe)


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
