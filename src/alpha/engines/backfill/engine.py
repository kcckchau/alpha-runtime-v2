"""
BackfillEngine — standalone engine for the ``alpha backfill`` CLI command.

Full historical backfill for configured symbols and timeframes.

Uses replay.start_date / replay.end_date when set (via --start / --end CLI flags).
Falls back to warmup-driven windows sized to the deepest indicator per timeframe.
Idempotent: only fetches gaps not already in local Parquet cache.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from alpha.calendar.nyse import NYSECalendar
from alpha.config.loader import get_settings
from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.instruments import resolve_symbol
from alpha.models.enums import BarTimeframe

if TYPE_CHECKING:
    from alpha.engines.historical.engine import HistoricalDataEngine
    from alpha.engines.storage.engine import StorageEngine

logger = logging.getLogger(__name__)


class BackfillEngine(BaseEngine):
    """Standalone engine that fetches missing bar gaps and writes them to Parquet cache."""

    def __init__(self, settings: AlphaSettings | None = None) -> None:
        super().__init__()
        self._settings = settings or get_settings()
        self._event_bus = EventBus()
        self._registry = SymbolRegistry()
        self._calendar = NYSECalendar()

        self._storage: StorageEngine | None = None
        self._historical: HistoricalDataEngine | None = None
        self._ibkr_conn: object | None = None

    @property
    def name(self) -> str:
        return "BackfillEngine"

    async def _on_initialize(self) -> None:
        logger.info(
            "Initializing BackfillEngine | symbols=%s",
            self._settings.runtime.symbols,
        )

        # Register symbols
        for ticker in self._settings.runtime.symbols:
            sym = resolve_symbol(ticker)
            self._registry.register(sym)
        logger.info("Registry loaded: %d symbols", len(self._registry))

        await self._event_bus.start()

        # Create engines
        from alpha.engines.storage.engine import StorageEngine
        from alpha.engines.historical.engine import HistoricalDataEngine

        self._storage = StorageEngine(self._settings, self._event_bus)
        self._historical = HistoricalDataEngine(
            self._settings, self._event_bus, self._registry, self._calendar
        )

        # Wire data source
        self._wire_source()

        await self._storage.initialize()
        await self._historical.initialize()

    async def _on_start(self) -> None:
        if self._historical is None or self._storage is None:
            logger.error("Cannot run backfill: historical or storage engine not initialized")
            return

        await self._storage.start()
        await self._historical.start()

        from alpha.engines.bootstrap.catchup import CatchupService

        catchup = CatchupService(
            settings=self._settings,
            storage=self._storage,
            historical=self._historical,
            event_bus=self._event_bus,
            registry=self._registry,
        )

        hist = self._settings.historical
        replay = self._settings.replay

        now = datetime.now(timezone.utc)
        end = (
            datetime(
                replay.end_date.year, replay.end_date.month, replay.end_date.day,
                23, 59, 59, tzinfo=timezone.utc,
            )
            if replay.end_date
            else now
        )

        # Warmup-driven start dates (calendar-day adjusted: ≈1.4× trading days for weekends)
        default_starts: dict[BarTimeframe, datetime] = {
            BarTimeframe.M1: end - timedelta(days=max(3, hist.minute1_warmup_bars // 390 + 1)),
            BarTimeframe.M5: end - timedelta(days=max(7, hist.minute5_warmup_bars // 78 + 2)),
            BarTimeframe.H1: end - timedelta(days=int(hist.hourly_warmup_bars * 0.22) + 30),
            BarTimeframe.D1: end - timedelta(days=int(hist.daily_warmup_bars * 1.5)),
        }

        # An explicit --start extends the window further back on all timeframes
        if replay.start_date:
            explicit = datetime(
                replay.start_date.year, replay.start_date.month, replay.start_date.day,
                tzinfo=timezone.utc,
            )
            starts: dict[BarTimeframe, datetime] = {
                tf: min(default, explicit) for tf, default in default_starts.items()
            }
        else:
            starts = default_starts

        logger.info(
            "Running full historical backfill | symbols=%s | end=%s"
            " | 1m_start=%s | 5m_start=%s | 1h_start=%s | 1d_start=%s",
            self._settings.runtime.symbols,
            end.date().isoformat(),
            starts[BarTimeframe.M1].date().isoformat(),
            starts[BarTimeframe.M5].date().isoformat(),
            starts[BarTimeframe.H1].date().isoformat(),
            starts[BarTimeframe.D1].date().isoformat(),
        )

        for symbol in self._settings.runtime.symbols:
            logger.info("Backfilling %s", symbol)
            for timeframe in (BarTimeframe.M1, BarTimeframe.M5, BarTimeframe.H1, BarTimeframe.D1):
                await catchup.fetch_range(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=starts[timeframe],
                    end=end,
                )

        await self._storage.flush()
        logger.info("Historical backfill complete")

    async def _on_stop(self) -> None:
        if self._storage is not None:
            await self._storage.stop()
        if self._historical is not None:
            await self._historical.stop()
        # Disconnect IBKR if used
        if self._ibkr_conn is not None:
            from alpha.engines.ibkr.connection import IBKRConnection
            if isinstance(self._ibkr_conn, IBKRConnection):
                await self._ibkr_conn.disconnect()
        await self._event_bus.stop()

    def _wire_source(self) -> None:
        """Register historical data source (Databento or IBKR) based on settings."""
        from alpha.models.enums import DataSourceId

        hist_src = self._settings.historical.primary_source

        if hist_src == DataSourceId.DATABENTO:
            from alpha.engines.historical.sources.databento import DatabentoHistoricalDataSource
            self._historical.register_source(
                DatabentoHistoricalDataSource(self._registry, self._settings.databento)
            )
            logger.info("Databento historical source registered (dataset=%s)", self._settings.databento.dataset)

        elif hist_src == DataSourceId.INTERACTIVE_BROKERS:
            from alpha.engines.ibkr.connection import IBKRConnection
            from alpha.engines.historical.sources.ibkr import IBKRHistoricalDataSource
            conn = IBKRConnection(self._settings.ibkr)
            self._ibkr_conn = conn
            self._historical.register_source(
                IBKRHistoricalDataSource(conn, self._registry, self._settings.ibkr)
            )
            logger.info("IBKR historical source registered (paper=%s)", self._settings.ibkr.is_paper)

        else:
            logger.warning("No recognized historical data source configured: %s", hist_src)
