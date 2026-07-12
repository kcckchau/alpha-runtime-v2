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
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import AsyncIterator, Any

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

    def _archive_path(self, schema: str, db_symbol: str, start: datetime, end: datetime) -> Path | None:
        root = self._settings.raw_archive_root
        if root is None:
            return None
        fname = f"{start.strftime('%Y%m%dT%H%M%S')}_{end.strftime('%Y%m%dT%H%M%S')}.dbn.zst"
        return root / schema / db_symbol / fname

    def _load_from_archive(
        self, schema: str, db_symbol: str, start: datetime, end: datetime
    ) -> db.DBNStore | None:
        """Decode a local raw-DBN archive instead of paying for another
        Databento call, if one exists for this exact (schema, symbol, start,
        end) key — e.g. `scripts/download_raw.py` already fetched it.
        """
        path = self._archive_path(schema, db_symbol, start, end)
        if path is None or not path.exists():
            return None
        try:
            return db.DBNStore.from_file(path)
        except Exception:
            logger.exception("Failed to read raw DBN archive, re-fetching: %s", path)
            return None

    def _archive_raw(
        self, store: Any, schema: str, db_symbol: str, start: datetime, end: datetime
    ) -> None:
        """Persist the raw DBN response before it's decoded and discarded.

        Every parsing decision this file makes (fixed-point price scaling,
        taker_side mapping, etc.) is only as trustworthy as our own code —
        we've already found one of those backwards once. Keeping the raw
        bytes means a parsing bug can be re-checked or reprocessed offline
        against ground truth without paying for another Databento call.
        Synchronous (called via run_in_executor) — this is local file I/O
        on data already fully downloaded, not a network call.
        """
        path = self._archive_path(schema, db_symbol, start, end)
        if path is None or path.exists():
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            store.to_file(path)
        except Exception:
            logger.exception("Failed to archive raw DBN: %s", path)

    def _log_cost_estimate(self, schema: str, db_symbol: str, start: datetime, end: datetime) -> None:
        """Log Databento's own estimated $ cost before a real fetch.

        Cheap (one metadata call) and catches an accidentally huge pull —
        e.g. a full day of a dense schema — before it happens instead of
        after the bill arrives. Synchronous (called via run_in_executor);
        best-effort, never blocks the actual fetch if it fails.
        """
        try:
            cost = self._client.metadata.get_cost(
                dataset=self._settings.dataset,
                schema=schema,
                symbols=[db_symbol],
                start=start.isoformat(),
                end=end.isoformat(),
                stype_in=self._settings.stype_in,
            )
            logger.info(
                "Databento estimated cost: %s %s %s → %s: $%.4f",
                schema, db_symbol, start.date(), end.date(), cost,
            )
        except Exception:
            logger.debug("Databento get_cost failed (non-fatal)", exc_info=True)

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

        loop = asyncio.get_running_loop()
        t0 = time.perf_counter()
        store = await loop.run_in_executor(None, self._load_from_archive, schema, db_symbol, start, end)
        from_cache = store is not None
        if store is not None:
            logger.debug(
                "Databento fetch_bars: %s [%s] schema=%s — using local raw archive, no API call",
                symbol, timeframe, schema,
            )
        else:
            await loop.run_in_executor(None, self._log_cost_estimate, schema, db_symbol, start, end)
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
            await loop.run_in_executor(None, self._archive_raw, store, schema, db_symbol, start, end)

        # Collect records in a thread to avoid blocking the event loop.
        # `store` iterates synchronously over an HTTP response — doing it on
        # the main thread would starve the live MBP-1 feed and trigger slow-client errors.
        records: list = await loop.run_in_executor(None, list, store)
        elapsed = time.perf_counter() - t0
        rate = len(records) / elapsed if elapsed > 0 else 0.0
        logger.info(
            "Databento fetch_bars: %s [%s] schema=%s — %d records in %.1fs (%.0f/s)%s",
            symbol, timeframe, schema, len(records), elapsed, rate, " [cache]" if from_cache else "",
        )

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

        loop = asyncio.get_running_loop()
        t0 = time.perf_counter()
        store = await loop.run_in_executor(None, self._load_from_archive, "trades", db_symbol, start, end)
        from_cache = store is not None
        if store is not None:
            logger.debug("Databento fetch_trades: %s — using local raw archive, no API call", symbol)
        else:
            await loop.run_in_executor(None, self._log_cost_estimate, "trades", db_symbol, start, end)
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
            await loop.run_in_executor(None, self._archive_raw, store, "trades", db_symbol, start, end)

        records = await loop.run_in_executor(None, list, store)
        elapsed = time.perf_counter() - t0
        rate = len(records) / elapsed if elapsed > 0 else 0.0
        logger.info(
            "Databento fetch_trades: %s — %d records in %.1fs (%.0f/s)%s",
            symbol, len(records), elapsed, rate, " [cache]" if from_cache else "",
        )

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

        loop = asyncio.get_running_loop()
        t0 = time.perf_counter()
        store = await loop.run_in_executor(None, self._load_from_archive, "mbp-1", db_symbol, start, end)
        from_cache = store is not None
        if store is not None:
            logger.debug("Databento fetch_quotes: %s — using local raw archive, no API call", symbol)
        else:
            await loop.run_in_executor(None, self._log_cost_estimate, "mbp-1", db_symbol, start, end)
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
            await loop.run_in_executor(None, self._archive_raw, store, "mbp-1", db_symbol, start, end)

        records = await loop.run_in_executor(None, list, store)
        elapsed = time.perf_counter() - t0
        rate = len(records) / elapsed if elapsed > 0 else 0.0
        logger.info(
            "Databento fetch_quotes: %s — %d records in %.1fs (%.0f/s)%s",
            symbol, len(records), elapsed, rate, " [cache]" if from_cache else "",
        )

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
    # Empirically verified (2026-07-12) by joining trades against quotes and
    # checking which side of the book each print actually landed on — a
    # comment-trusting "fix" made earlier in this file's history had this
    # backwards. Don't re-flip this without re-running that check: 18,012/
    # 18,348 'A'-derived BUY-tagged trades in a June 5 sample printed at the
    # bid, and 17,463/17,826 'B'-derived SELL-tagged trades printed at the
    # ask — i.e. the on-disk truth is the opposite of Databento's own schema
    # docs as previously understood here.
    if raw == "A":
        return TakerSide.SELL
    if raw == "B":
        return TakerSide.BUY
    return TakerSide.UNKNOWN
