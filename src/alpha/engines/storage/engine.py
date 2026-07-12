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
from collections import defaultdict
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
    EventMetadata,
    MarketStateEvent,
    OrderBookEvent,
    OrderUpdateEvent,
    QuoteEvent,
    SetupEvent,
    SystemEvent,
    TradeEvent,
)

logger = logging.getLogger(__name__)

# Event types where the same logical record may be written more than once
# (restart, or a tick backfill re-covering a day already captured live) —
# mapped to the column that uniquely identifies one logical record, so
# ParquetStore.write() can upsert instead of blindly appending.
_DEDUP_KEYS: dict[str, str] = {
    "setups": "setup_id",
    "trades": "trade_id",
}


class StorageEngine(BaseEngine):
    """
    Dual-backend storage engine (Parquet + PostgreSQL).

    All engines write through this interface. No engine touches storage
    libraries (pyarrow, sqlalchemy) directly.

    Usage::

        await storage.save_bar(bar_event)
        bars = await storage.load_bars("AAPL", BarTimeframe.M1, start, end)
    """

    # How often buffered rows are flushed to Parquet. Each flush does a full
    # read-modify-write of the target day's file, so batching many events into
    # one flush (rather than one flush per event) is what keeps the writer
    # ahead of high-frequency trade/quote volume as the file grows over the day.
    #
    # trades/quotes get a much longer interval than everything else: the
    # rewrite cost scales with the *existing* file size, not the batch being
    # appended, so late in the day a 2s cadence falls hopelessly behind —
    # measured on a real MNQ-09 session, a single quotes flush at end-of-day
    # volume (~23.7M rows) took ~17s, 8.5x the 2s budget. bars/setups/etc.
    # stay on the short interval since their daily row counts are trivial.
    _FLUSH_INTERVAL_SECONDS = 2.0
    _HIGH_FREQ_FLUSH_INTERVAL_SECONDS = 120.0
    _HIGH_FREQ_TYPES = frozenset({"trades", "quotes"})
    # Safety cap so a burst (e.g. a volatile open) can't grow the in-memory
    # buffer unbounded while waiting out the long interval above.
    _MAX_BUFFERED_ROWS = 50_000

    def __init__(self, settings: AlphaSettings, event_bus: EventBus) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._parquet = ParquetStore(settings.storage)
        self._postgres = PostgresStore()
        self._write_queue: asyncio.Queue[AnyEvent] = asyncio.Queue(maxsize=10_000)
        self._writer_task: asyncio.Task[None] | None = None
        self._flush_task: asyncio.Task[None] | None = None
        self._writes_total: int = 0
        # Buffered rows awaiting flush, keyed by (data_type, symbol, date).
        # Populated synchronously (no I/O) by the writer loop; drained by _flush_loop.
        self._buffers: dict[tuple[str, str, date], list[dict[str, Any]]] = defaultdict(list)
        # monotonic time a given partition key was last actually written —
        # used to apply _HIGH_FREQ_FLUSH_INTERVAL_SECONDS per-key rather than
        # globally, since bars and trades for the same symbol/day shouldn't
        # share a cadence.
        self._last_flush_at: dict[tuple[str, str, date], float] = {}

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
        self._flush_task = asyncio.ensure_future(self._flush_loop())
        self._subscribe_to_bus()

    async def _on_stop(self) -> None:
        if self._writer_task and not self._writer_task.done():
            await self._write_queue.join()
            self._writer_task.cancel()
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
        # Final flush so nothing buffered in memory is lost on shutdown.
        await self._flush_all()

    async def _health_check(self) -> EngineHealth:
        details = {
            "queue_depth": self._write_queue.qsize(),
            "buffered_rows": sum(len(rows) for rows in self._buffers.values()),
            "writes_total": self._writes_total,
        }
        return EngineHealth(HealthStatus.HEALTHY, self.name, details)

    # ── Public write API ──────────────────────────────────────────────────────

    async def save_bar(self, event: BarEvent) -> None:
        await self._write_queue.put(event)

    async def save_trade(self, event: TradeEvent) -> None:
        await self._write_queue.put(event)

    async def save_quote(self, event: QuoteEvent) -> None:
        await self._write_queue.put(event)

    async def save_trades_bulk(self, symbol: str, d: date, events: list[TradeEvent]) -> None:
        """Write an entire day's trades in one shot — for backfill, where the
        whole day is already materialized in memory before any storage call
        happens. Bypasses the write queue / periodic-flush buffer entirely:
        that pipeline exists to batch *incrementally arriving* live events
        into fewer writes, but a backfill has no incremental arrival to batch
        — routing it through save_trade() one event at a time instead forces
        a flush to repeatedly read-and-rewrite an already-huge file as it
        grows, which is the same O(day size) cost per flush we fixed the
        flush interval for, just triggered on a compressed few-second dump
        instead of spread over a real trading session.
        """
        if not events:
            return
        table = self._events_to_table(events)
        await asyncio.get_running_loop().run_in_executor(
            None, self._parquet.write, table, "trades", symbol, d, "trade_id", True
        )

    async def save_quotes_bulk(self, symbol: str, d: date, events: list[QuoteEvent]) -> None:
        """Write an entire day's quotes in one shot. See save_trades_bulk."""
        if not events:
            return
        table = self._events_to_table(events)
        await asyncio.get_running_loop().run_in_executor(
            None, self._parquet.write, table, "quotes", symbol, d, None, False
        )

    def _events_to_table(self, events: list[AnyEvent]) -> Any:
        import pyarrow as pa
        return pa.Table.from_pylist([self._serialize_event(e) for e in events])

    async def flush(self) -> None:
        await self._write_queue.join()
        # Draining the queue only moves events into the in-memory buffer; force
        # an actual Parquet write so callers relying on `flush()` for durability
        # (e.g. catch-up) get a correct guarantee.
        await self._flush_all()

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

    async def has_trades(self, symbol: str, d: date) -> bool:
        return self._parquet.exists("trades", symbol, d)

    async def has_quotes(self, symbol: str, d: date) -> bool:
        return self._parquet.exists("quotes", symbol, d)

    async def clear_quotes(self, symbol: str, d: date) -> None:
        """Delete a day's quotes file. Quotes have no per-row dedup key (see
        _DEDUP_KEYS), so a forced refetch must clear the old file first —
        otherwise the new rows would just append on top of the old ones."""
        self._parquet.delete("quotes", symbol, d)

    async def list_bar_dates(self, symbol: str, timeframe: BarTimeframe) -> list[date]:
        return self._parquet.list_dates(f"bars/{timeframe}", symbol)

    async def load_bar_events(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: date,
        end: date,
    ) -> list[BarEvent]:
        table = self._parquet.read_range(f"bars/{timeframe}", symbol, start, end)
        return [self._row_to_bar_event(row, timeframe) for row in table.to_pylist()]

    # ── Internal ──────────────────────────────────────────────────────────────

    # Event types storage already treats as best-effort (dropped under backpressure
    # at the internal write-queue level — see `_on_event`). Also exempting them at
    # the EventBus subscription level (drop_if_full=True) means a slow storage
    # consumer never blocks the publisher for these — critical for TRADE, since
    # LiveIngestionEngine awaits `event_bus.publish()` before updating the live
    # partial bar, so backpressure here would directly delay the chart's live candle.
    _BEST_EFFORT_TYPES = frozenset({EventType.QUOTE, EventType.TRADE})

    def _subscribe_to_bus(self) -> None:
        for et in EventType:
            drop = et in self._BEST_EFFORT_TYPES
            self._event_bus.subscribe(et, self._on_event, drop_if_full=drop)

    async def _on_event(self, event: AnyEvent) -> None:
        if isinstance(event, BarEvent):
            # Replay bars are already persisted to Parquet via persist_direct in
            # _load_or_fetch_bars. Storing them again here would duplicate every
            # bar on each restart.
            if event.metadata.is_replay:
                return
            # Live bars are low-frequency and critical to the chart — never drop them.
            # A brief await here applies natural backpressure instead of data loss.
            await self._write_queue.put(event)
            return
        try:
            self._write_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("StorageEngine write queue full — dropping %s", event.event_type)

    async def _writer_loop(self) -> None:
        while True:
            event = await self._write_queue.get()
            try:
                # Live quotes are ephemeral high-frequency data (~hundreds/sec) and
                # are never loaded back from Parquet, so skip buffering/writing them
                # to avoid falling behind the live feed. Historical/replay quotes
                # (e.g. from an explicit tick backfill via save_quote) are finite
                # and requested specifically to be persisted, so those are kept.
                if not (isinstance(event, QuoteEvent) and not event.metadata.is_replay):
                    self._buffer_event(event)
                self._writes_total += 1
            except Exception:
                logger.exception("StorageEngine: write error for %s", event.event_type)
            finally:
                self._write_queue.task_done()

    def _buffer_event(self, event: AnyEvent) -> None:
        """Append the serialized row to an in-memory buffer (no I/O).

        Writing to Parquet requires reading, concatenating, and rewriting the
        *entire* existing file for that partition, so doing this once per event
        (as the file grows across the trading day) cannot keep up with
        high-frequency trade ticks. Instead we accumulate rows here and let
        `_flush_loop` batch them into far fewer, larger writes.
        """
        data_type = self._data_type_for(event)
        if data_type is None:
            return
        row = self._serialize_event(event)
        key = (data_type, self._storage_symbol(event), event.timestamp.date())
        self._buffers[key].append(row)

    def _flush_interval_for(self, data_type: str) -> float:
        return (
            self._HIGH_FREQ_FLUSH_INTERVAL_SECONDS
            if data_type in self._HIGH_FREQ_TYPES
            else self._FLUSH_INTERVAL_SECONDS
        )

    async def _flush_loop(self) -> None:
        # Ticks on the short interval regardless of data type — that's just a
        # cheap timestamp comparison per buffered key (see _flush_due). Only
        # keys whose own interval has elapsed, or whose buffer has grown past
        # _MAX_BUFFERED_ROWS, actually trigger a write this tick.
        while True:
            await asyncio.sleep(self._FLUSH_INTERVAL_SECONDS)
            await self._flush_due()

    async def _flush_due(self) -> None:
        if not self._buffers:
            return
        now = asyncio.get_running_loop().time()
        due_keys = [
            key
            for key, rows in self._buffers.items()
            if rows
            and (
                now - self._last_flush_at.get(key, 0.0) >= self._flush_interval_for(key[0])
                or len(rows) >= self._MAX_BUFFERED_ROWS
            )
        ]
        if not due_keys:
            return
        pending = {key: self._buffers.pop(key) for key in due_keys}
        await self._write_pending(pending, now)

    async def _flush_all(self) -> None:
        """Force-flush every buffered partition regardless of its interval.

        Used for durability guarantees (flush(), shutdown) where "wait out
        the rest of the high-frequency interval" isn't an acceptable answer.
        """
        if not self._buffers:
            return
        # Swap out the buffer up front (single-threaded event loop — no race
        # with `_buffer_event`, which never awaits between read and append)
        # so new events keep accumulating in a fresh buffer while this flush
        # writes the previous batch out.
        pending, self._buffers = self._buffers, defaultdict(list)
        await self._write_pending(pending, asyncio.get_running_loop().time())

    async def _write_pending(
        self, pending: dict[tuple[str, str, date], list[dict[str, Any]]], flushed_at: float
    ) -> None:
        loop = asyncio.get_running_loop()
        for (data_type, symbol, d), rows in pending.items():
            if not rows:
                continue
            self._last_flush_at[(data_type, symbol, d)] = flushed_at
            try:
                await loop.run_in_executor(None, self._write_rows_sync, rows, data_type, symbol, d)
            except Exception:
                logger.exception("StorageEngine: flush error for %s/%s/%s", data_type, symbol, d)

    def _write_rows_sync(
        self,
        rows: list[dict[str, Any]],
        data_type: str,
        symbol: str,
        d: date,
    ) -> None:
        import pyarrow as pa

        table = pa.Table.from_pylist(rows)
        # Setups use upsert semantics: same setup_id on restart overwrites the
        # previous row rather than duplicating it. Trades can be written twice
        # (once live, once via a later tick backfill covering the same day) —
        # trade_id is stable across both sources, so dedup on it the same way.
        # For trades specifically, prefer the live-captured row on conflict:
        # its received_at reflects real feed latency, which a replay/backfill
        # rewrite of the same trade would otherwise silently overwrite with a
        # meaningless value (whenever the backfill happened to run).
        dedup_key = _DEDUP_KEYS.get(data_type)
        self._parquet.write(
            table, data_type, symbol, d, dedup_key=dedup_key, prefer_live=(data_type == "trades")
        )

    @staticmethod
    def _data_type_for(event: AnyEvent) -> str | None:
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
        return None  # unrecognised event types are not persisted

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
    def _row_to_bar_event(row: dict[str, Any], timeframe: BarTimeframe) -> BarEvent:
        timestamp = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
        received_at = datetime.fromisoformat(str(row["received_at"]).replace("Z", "+00:00"))
        return BarEvent(
            symbol=str(row["symbol"]),
            timestamp=timestamp,
            metadata=EventMetadata(
                event_id=row["event_id"],
                source=row["source"],
                received_at=received_at,
                is_replay=bool(row.get("is_replay", False)),
                sequence_num=row.get("sequence_num"),
            ),
            timeframe=timeframe,
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            vwap=Decimal(str(row["vwap"])) if row.get("vwap") is not None else None,
            trade_count=int(row["trade_count"]) if row.get("trade_count") is not None else None,
            is_partial=bool(row.get("is_partial", False)),
        )

    @staticmethod
    def _decimal_text(value: Decimal | None) -> str | None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _json_text(value: Any) -> str:
        return json.dumps(value, sort_keys=True, default=str)
