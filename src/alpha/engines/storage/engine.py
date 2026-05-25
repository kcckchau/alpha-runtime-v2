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
import logging
from datetime import date, datetime
from typing import Any

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.engines.storage.parquet import ParquetStore
from alpha.engines.storage.postgres import PostgresStore
from alpha.models.enums import BarTimeframe, EventType, HealthStatus
from alpha.models.events import AnyEvent, BarEvent

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
        # TODO: convert event → Arrow Table row and write to Parquet
        # For now, this is a no-op placeholder
        pass
