"""
Engine 2 — Storage Engine

Responsibilities:
  - Persist all event types to Parquet and/or PostgreSQL
  - Provide typed read APIs for all stored data
  - No business logic — pure storage abstraction

Storage policy:
  - Raw market data (bars, trades, quotes) → Parquet (time-partitioned)
  - Derived data (market states, setups, orders) → PostgreSQL (queryable)
  - Execution reports → PostgreSQL (auditable, never deleted)

Write path:
  Storage engine subscribes to all event types via EventBus and
  persists them asynchronously. Heavy writes are batched.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.engines.storage.parquet import ParquetStore
from alpha.engines.storage.postgres import PostgresStore
from alpha.models.enums import BarTimeframe, EventType, HealthStatus
from alpha.models.events import (
    AnyEvent,
    BarEvent,
    MarketStateEvent,
    OrderBookEvent,
    OrderUpdateEvent,
    QuoteEvent,
    SetupEvent,
    SystemEvent,
    TradeEvent,
)

logger = logging.getLogger(__name__)


class StorageEngine(BaseEngine):
    """
    Dual-backend storage engine (Parquet + PostgreSQL).

    All engines write through this interface. No engine touches storage
    libraries (pyarrow, sqlalchemy) directly.

    Usage::

        await storage.save_bar(bar_event)
        bars = await storage.load_bars("AAPL", BarTimeframe.M1, start, end)
    """

    def __init__(self, settings: AlphaSettings, event_bus: EventBus) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._parquet = ParquetStore(settings.storage)
        self._postgres = PostgresStore()
        self._write_queue: asyncio.Queue[AnyEvent] = asyncio.Queue(maxsize=10_000)
        self._writer_task: asyncio.Task[None] | None = None
        self._writes_total: int = 0

    @property
    def name(self) -> str:
        return "StorageEngine"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        self._settings.storage.parquet_root.mkdir(parents=True, exist_ok=True)
        logger.info("Parquet root: %s", self._settings.storage.parquet_root)
        # TODO: establish SQLAlchemy async engine + run migrations check

    async def _on_start(self) -> None:
        self._writer_task = asyncio.ensure_future(self._writer_loop())
        self._subscribe_to_bus()

    async def _on_stop(self) -> None:
        if self._writer_task and not self._writer_task.done():
            await self._write_queue.join()
            self._writer_task.cancel()

    async def _health_check(self) -> EngineHealth:
        details = {
            "queue_depth": self._write_queue.qsize(),
            "writes_total": self._writes_total,
        }
        return EngineHealth(HealthStatus.HEALTHY, self.name, details)

    # ── Public write API ──────────────────────────────────────────────────────

    async def save_bar(self, event: BarEvent) -> None:
        await self._write_queue.put(event)

    # ── Public read API ───────────────────────────────────────────────────────

    async def load_bars(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: date,
        end: date,
        columns: list[str] | None = None,
    ) -> Any:
        """Return a PyArrow Table of bars for the given range."""
        import pyarrow as pa
        data_type = f"bars/{timeframe}"
        return self._parquet.read_range(data_type, symbol, start, end, columns)

    async def has_bars(self, symbol: str, timeframe: BarTimeframe, d: date) -> bool:
        return self._parquet.exists(f"bars/{timeframe}", symbol, d)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _subscribe_to_bus(self) -> None:
        for et in EventType:
            self._event_bus.subscribe(et, self._on_event)

    async def _on_event(self, event: AnyEvent) -> None:
        try:
            self._write_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("StorageEngine write queue full — dropping %s", event.event_type)

    async def _writer_loop(self) -> None:
        while True:
            event = await self._write_queue.get()
            try:
                await self._persist(event)
                self._writes_total += 1
            except Exception:
                logger.exception("StorageEngine: write error for %s", event.event_type)
            finally:
                self._write_queue.task_done()

    async def _persist(self, event: AnyEvent) -> None:
        import pyarrow as pa

        data_type = self._data_type_for(event)
        row = self._serialize_event(event)
        table = pa.Table.from_pylist([row])
        self._parquet.write(
            table,
            data_type,
            self._storage_symbol(event),
            event.timestamp.date(),
        )

    @staticmethod
    def _data_type_for(event: AnyEvent) -> str:
        if isinstance(event, BarEvent):
            return f"bars/{event.timeframe}"
        if isinstance(event, TradeEvent):
            return "trades"
        if isinstance(event, QuoteEvent):
            return "quotes"
        if isinstance(event, OrderBookEvent):
            return "order_books"
        if isinstance(event, MarketStateEvent):
            return "market_states"
        if isinstance(event, SetupEvent):
            return "setups"
        if isinstance(event, OrderUpdateEvent):
            return "orders"
        if isinstance(event, SystemEvent):
            return "system"
        raise TypeError(f"Unsupported event type: {type(event).__name__}")

    def _serialize_event(self, event: AnyEvent) -> dict[str, Any]:
        base = {
            "event_id": str(event.metadata.event_id),
            "event_type": str(event.event_type),
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "source": str(event.metadata.source),
            "received_at": event.metadata.received_at.isoformat(),
            "is_replay": event.metadata.is_replay,
            "sequence_num": event.metadata.sequence_num,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }

        if isinstance(event, BarEvent):
            return {
                **base,
                "timeframe": str(event.timeframe),
                "open": self._decimal_text(event.open),
                "high": self._decimal_text(event.high),
                "low": self._decimal_text(event.low),
                "close": self._decimal_text(event.close),
                "volume": event.volume,
                "vwap": self._decimal_text(event.vwap),
                "trade_count": event.trade_count,
                "is_partial": event.is_partial,
            }
        if isinstance(event, TradeEvent):
            return {
                **base,
                "price": self._decimal_text(event.price),
                "size": event.size,
                "conditions_json": self._json_text(event.conditions),
                "exchange": event.exchange,
                "taker_side": str(event.taker_side),
                "trade_id": event.trade_id,
            }
        if isinstance(event, QuoteEvent):
            return {
                **base,
                "bid_price": self._decimal_text(event.bid_price),
                "bid_size": event.bid_size,
                "ask_price": self._decimal_text(event.ask_price),
                "ask_size": event.ask_size,
                "bid_exchange": event.bid_exchange,
                "ask_exchange": event.ask_exchange,
            }
        if isinstance(event, OrderBookEvent):
            return {
                **base,
                "bids_json": self._json_text(event.bids),
                "asks_json": self._json_text(event.asks),
                "is_snapshot": event.is_snapshot,
            }
        if isinstance(event, MarketStateEvent):
            return {
                **base,
                "state_data_json": self._json_text(event.state_data),
            }
        if isinstance(event, SetupEvent):
            return {
                **base,
                "setup_id": str(event.setup_id),
                "setup_type": str(event.setup_type),
                "setup_state": str(event.setup_state),
                "prev_state": str(event.prev_state) if event.prev_state is not None else None,
            }
        if isinstance(event, OrderUpdateEvent):
            return {
                **base,
                "order_id": str(event.order_id),
                "broker_order_id": event.broker_order_id,
                "order_status": str(event.order_status),
                "filled_quantity": event.filled_quantity,
                "avg_fill_price": self._decimal_text(event.avg_fill_price),
                "reject_reason": event.reject_reason,
            }
        if isinstance(event, SystemEvent):
            return {
                **base,
                "event_name": event.event_name,
                "payload_json": self._json_text(event.payload),
            }
        raise TypeError(f"Unsupported event type: {type(event).__name__}")

    @staticmethod
    def _storage_symbol(event: AnyEvent) -> str:
        if isinstance(event, SystemEvent):
            return "__system__"
        return event.symbol

    @staticmethod
    def _decimal_text(value: Decimal | None) -> str | None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _json_text(value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=str)
