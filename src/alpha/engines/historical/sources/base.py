"""
DataSource abstraction for historical data.

Each broker/data-vendor integration implements this interface.
The Historical Data Engine calls these methods and passes the results
to the normalizer — engine code never touches source-specific formats.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncIterator

from alpha.models.enums import BarTimeframe, DataSourceId
from alpha.models.events import BarEvent, QuoteEvent, TradeEvent


class HistoricalDataSource(ABC):
    """Contract for fetching and normalizing historical market data."""

    @property
    @abstractmethod
    def source_id(self) -> DataSourceId:
        """Unique identifier for this data source."""

    @property
    @abstractmethod
    def supports_trades(self) -> bool:
        """True if this source provides trade tick data."""

    @property
    @abstractmethod
    def supports_quotes(self) -> bool:
        """True if this source provides quote (bid/ask) data."""

    # ── Fetch methods ─────────────────────────────────────────────────────────

    @abstractmethod
    def fetch_bars(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        adjust: bool = True,
    ) -> AsyncIterator[BarEvent]:
        """
        Yield normalized BarEvents in chronological order.

        The source is responsible for:
          - Pagination / rate limiting
          - Adjustment for splits/dividends if adjust=True
          - Filling EventMetadata with correct source and received_at
        """

    def fetch_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> AsyncIterator[TradeEvent]:
        """Yield normalized TradeEvents. Default raises NotImplementedError."""
        raise NotImplementedError(f"{self.source_id} does not support trade ticks")
        # Make this a generator to satisfy the return type annotation
        return
        yield  # type: ignore[misc]

    def fetch_quotes(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> AsyncIterator[QuoteEvent]:
        """Yield normalized QuoteEvents. Default raises NotImplementedError."""
        raise NotImplementedError(f"{self.source_id} does not support quotes")
        return
        yield  # type: ignore[misc]

    # ── Health ────────────────────────────────────────────────────────────────

    async def ping(self) -> bool:
        """Return True if the source is reachable."""
        return True
