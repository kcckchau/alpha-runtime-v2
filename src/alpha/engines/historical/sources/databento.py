"""
Databento historical data source.

Fetches OHLCV bars, trades, and top-of-book quotes from the Databento
Historical API for CME Globex US futures (and any other Databento dataset).

Key differences from IBKR:
  - No pacing limits — Databento allows bulk range requests
  - Continuous contract symbols (ES.c.0) remove the need for manual roll tracking
  - All prices are fixed-point integers (raw / 1_000_000_000 = actual price)
  - Timestamps are nanoseconds since epoch
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import AsyncIterator

import databento as db

from alpha.config.settings import DatabentoSettings
from alpha.core.registry import SymbolRegistry
from alpha.engines.historical.sources.base import HistoricalDataSource
from alpha.models.enums import AssetClass, BarTimeframe, DataSourceId, TakerSide
from alpha.models.events import BarEvent, EventMetadata, QuoteEvent, TradeEvent

logger = logging.getLogger(__name__)

_PRICE_SCALE = Decimal("1000000000")  # Databento fixed-point scale factor
_UTC = timezone.utc

# Databento schema names for each bar timeframe.
# Timeframes not natively supported are fetched at 1m and must be aggregated
# by the caller (Historical Data Engine).
_TIMEFRAME_TO_SCHEMA: dict[BarTimeframe, str] = {
    BarTimeframe.S1: "ohlcv-1s",
    BarTimeframe.M1: "ohlcv-1m",
    BarTimeframe.H1: "ohlcv-1h",
    BarTimeframe.D1: "ohlcv-1d",
}
_FALLBACK_SCHEMA = "ohlcv-1m"  # used when timeframe has no native schema


def _to_datetime(ts_ns: int) -> datetime:
    return datetime.fromtimestamp(ts_ns / 1e9, tz=_UTC)


def _to_decimal(raw: int) -> Decimal:
    return Decimal(raw) / _PRICE_SCALE


class DatabentoHistoricalDataSource(HistoricalDataSource):
    """
    Fetches and normalizes historical data from Databento.

    Handles:
      - OHLCV bars (1s, 1m, 1h, 1d natively; others must be aggregated upstream)
      - Trade ticks
      - Top-of-book quotes (MBP-1)
    """

    def __init__(self, registry: SymbolRegistry, settings: DatabentoSettings) -> None:
        self._registry = registry
        self._settings = settings
        self._client = db.Historical(settings.api_key.get_secret_value())

    @property
    def source_id(self) -> DataSourceId:
        return DataSourceId.DATABENTO

    @property
    def supports_trades(self) -> bool:
        return True

    @property
    def supports_quotes(self) -> bool:
        return True

    async def ping(self) -> bool:
        try:
            # Lightweight metadata call to verify the API key is valid.
            self._client.metadata.get_dataset_range(dataset=self._settings.dataset)
            return True
        except Exception:
            return False

    def _databento_symbol(self, ticker: str) -> str:
        """
        Map an internal ticker to a Databento continuous contract symbol.

        For futures, uses root_symbol + continuous_suffix (e.g. "ES" → "ES.c.0").
        For non-futures, returns the ticker as-is.
        """
        try:
            sym = self._registry.get(ticker)
        except Exception:
            return ticker
        if sym.asset_class == AssetClass.FUTURE:
            root = sym.root_symbol or sym.ticker
            return f"{root}{self._settings.continuous_suffix}"
        return sym.ticker

    def availability_end(self, schema: str) -> datetime:
        """Return the latest datetime for which data is available for the given schema.

        get_dataset_range() has no schema parameter — it returns the overall
        dataset end which matches ohlcv-1m. ohlcv-1h lags ~30 min behind M1
        (floor to hour) and ohlcv-1d lags ~7-8h (floor to day). A single
        metadata call derives all three with conservative truncation.
        """
        try:
            info = self._client.metadata.get_dataset_range(dataset=self._settings.dataset)
            m1_end = datetime.fromisoformat(info["end"].replace("Z", "+00:00"))
        except Exception:
            return datetime.now(tz=_UTC)

        if schema == "ohlcv-1h":
            return m1_end.replace(minute=0, second=0, microsecond=0)
        if schema == "ohlcv-1d":
            return m1_end.replace(hour=0, minute=0, second=0, microsecond=0)
        return m1_end  # ohlcv-1m and others: use actual dataset edge

    def _safe_end(self, end: datetime, schema: str | None = None) -> datetime:
        """Clamp end to the schema's availability edge."""
        avail = self.availability_end(schema or "ohlcv-1m")
        return min(end, avail)

    async def fetch_bars(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        adjust: bool = True,
    ) -> AsyncIterator[BarEvent]:
        schema = _TIMEFRAME_TO_SCHEMA.get(timeframe, _FALLBACK_SCHEMA)
        db_symbol = self._databento_symbol(symbol)
        end = self._safe_end(end, schema=schema)

        if start >= end:
            logger.debug(
                "Databento fetch_bars: %s [%s] skipped — gap start %s is at or after "
                "available end %s (Databento ingestion lag)",
                symbol, timeframe, start.isoformat(), end.isoformat(),
            )
            return

        logger.debug(
            "Databento fetch_bars: %s [%s] schema=%s %s → %s",
            symbol, timeframe, schema, start.date(), end.date(),
        )

        try:
            store = self._client.timeseries.get_range(
                dataset=self._settings.dataset,
                schema=schema,
                symbols=[db_symbol],
                start=start.isoformat(),
                end=end.isoformat(),
                stype_in=self._settings.stype_in,
            )
        except Exception as exc:
            msg = str(exc)
            if (
                "data_schema_not_fully_available" in msg
                or "data_end_after_available_end" in msg
                or "data_start_after_available_end" in msg
            ):
                # Per-schema ingestion lags behind wall-clock — ohlcv-1h and ohlcv-1d
                # lag significantly more than ohlcv-1m. metadata.get_dataset_range()
                # ignores the schema arg and returns the overall dataset end, so the
                # clamp doesn't help for slower schemas. Expected near realtime.
                logger.debug(
                    "Databento fetch_bars: %s [%s] schema=%s not yet available at end=%s — skipping",
                    symbol, timeframe, schema, end.isoformat(),
                )
            else:
                logger.exception("Databento timeseries.get_range failed for %s", symbol)
            return

        # Collect records in a thread to avoid blocking the event loop.
        # `store` iterates synchronously over an HTTP response — doing it on
        # the main thread would starve the live MBP-1 feed and trigger slow-client errors.
        loop = asyncio.get_running_loop()
        records: list = await loop.run_in_executor(None, list, store)

        now = datetime.now(tz=_UTC)
        for record in records:
            ts = _to_datetime(record.ts_event)
            if ts < start or ts > end:
                continue
            event = BarEvent(
                symbol=symbol,
                timestamp=ts,
                timeframe=timeframe,
                open=_to_decimal(record.open),
                high=_to_decimal(record.high),
                low=_to_decimal(record.low),
                close=_to_decimal(record.close),
                volume=int(record.volume),
                vwap=_to_decimal(record.vwap) if getattr(record, "vwap", None) else None,
                trade_count=getattr(record, "trade_count", None),
                metadata=EventMetadata(
                    source=DataSourceId.DATABENTO,
                    received_at=now,
                    is_replay=True,
                ),
            )
            yield event

    async def fetch_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> AsyncIterator[TradeEvent]:
        db_symbol = self._databento_symbol(symbol)
        end = self._safe_end(end, schema="trades")

        if start >= end:
            return

        logger.debug(
            "Databento fetch_trades: %s %s → %s", symbol, start.date(), end.date()
        )

        try:
            store = self._client.timeseries.get_range(
                dataset=self._settings.dataset,
                schema="trades",
                symbols=[db_symbol],
                start=start.isoformat(),
                end=end.isoformat(),
                stype_in=self._settings.stype_in,
            )
        except Exception as exc:
            msg = str(exc)
            if "data_schema_not_fully_available" in msg or "data_end_after_available_end" in msg:
                logger.debug("Databento fetch_trades: %s schema not yet available at end=%s", symbol, end.isoformat())
            else:
                logger.exception("Databento trades fetch failed for %s", symbol)
            return

        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(None, list, store)

        now = datetime.now(tz=_UTC)
        for record in records:
            ts = _to_datetime(record.ts_event)
            if ts < start or ts > end:
                continue
            raw_side = getattr(record, "side", None)
            taker_side = _map_taker_side(raw_side)
            event = TradeEvent(
                symbol=symbol,
                timestamp=ts,
                price=_to_decimal(record.price),
                size=int(record.size),
                trade_id=str(getattr(record, "sequence", "")),
                taker_side=taker_side,
                metadata=EventMetadata(
                    source=DataSourceId.DATABENTO,
                    received_at=now,
                    is_replay=True,
                ),
            )
            yield event

    async def fetch_quotes(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> AsyncIterator[QuoteEvent]:
        db_symbol = self._databento_symbol(symbol)
        end = self._safe_end(end, schema="mbp-1")

        if start >= end:
            return

        logger.debug(
            "Databento fetch_quotes: %s %s → %s", symbol, start.date(), end.date()
        )

        try:
            store = self._client.timeseries.get_range(
                dataset=self._settings.dataset,
                schema="mbp-1",
                symbols=[db_symbol],
                start=start.isoformat(),
                end=end.isoformat(),
                stype_in=self._settings.stype_in,
            )
        except Exception as exc:
            msg = str(exc)
            if "data_schema_not_fully_available" in msg or "data_end_after_available_end" in msg:
                logger.debug("Databento fetch_quotes: %s schema not yet available at end=%s", symbol, end.isoformat())
            else:
                logger.exception("Databento quotes fetch failed for %s", symbol)
            return

        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(None, list, store)

        now = datetime.now(tz=_UTC)
        for record in records:
            ts = _to_datetime(record.ts_event)
            if ts < start or ts > end:
                continue
            # MBP-1 top-of-book: levels[0] is best bid/ask
            bid_px = _to_decimal(record.levels[0].bid_px)
            ask_px = _to_decimal(record.levels[0].ask_px)
            bid_sz = int(record.levels[0].bid_sz)
            ask_sz = int(record.levels[0].ask_sz)
            event = QuoteEvent(
                symbol=symbol,
                timestamp=ts,
                bid_price=bid_px,
                bid_size=bid_sz,
                ask_price=ask_px,
                ask_size=ask_sz,
                metadata=EventMetadata(
                    source=DataSourceId.DATABENTO,
                    received_at=now,
                    is_replay=True,
                ),
            )
            yield event


def _map_taker_side(raw: str | None) -> TakerSide:
    # Databento side: 'A' = aggressor hit the ask (buyer) → BUY
    #                 'B' = aggressor hit the bid (seller) → SELL
    if raw == "A":
        return TakerSide.BUY
    if raw == "B":
        return TakerSide.SELL
    return TakerSide.UNKNOWN
