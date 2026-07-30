"""
Databento live feed adapter.

Streams real-time futures data via the Databento Live WebSocket gateway.

Architecture notes:
  - Databento's live client is synchronous/blocking — it runs in a dedicated
    background thread and bridges to the main asyncio loop via
    asyncio.run_coroutine_threadsafe().
  - Bars: subscribe to ohlcv-1m schema. Each completed bar arrives as OHLCVMsg.
  - Quotes: subscribe to mbp-1 schema (top-of-book). Each update is MBP1Msg.
  - Tick trades: subscribe to trades schema. Each tick is TradeMsg.
  - Multiple subscriptions share a single Live session.

Symbol resolution:
  - Records carry instrument_id (int), NOT a symbol string.
  - The gateway sends SymbolMappingMsg on connect, mapping instrument_id →
    the continuous symbol we subscribed with (e.g. "MNQ.c.0").
  - We maintain _instrument_id_to_ticker to resolve records at dispatch time.

Bar schema detection:
  - OHLCVMsg is the same class for all OHLCV schemas (1s, 1m, 1h, 1d).
  - We use record.rtype (int) to determine the timeframe at dispatch time.
  - rtype values: 32=ohlcv-1s, 33=ohlcv-1m, 34=ohlcv-1h, 35=ohlcv-1d.

Reconnect behavior:
  - If the background thread exits (connection dropped), _connected is set False.
  - The LiveIngestionEngine health check will detect UNHEALTHY and a restart
    (or a higher-level supervisor) should call connect() + subscribe_* again.
  - TODO: add automatic exponential-backoff reconnect inside the thread.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import threading
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

import databento as db
import databento_dbn as dbn

from collections.abc import Callable

from alpha.config.settings import DatabentoSettings
from alpha.core.registry import SymbolRegistry
from alpha.engines.live.ingress_observer import IngressObserver
from alpha.engines.live.adapters.base import (
    BarHandlerT,
    BookHandlerT,
    LiveFeedAdapter,
    QuoteHandlerT,
    TickHandlerT,
    TradeHandlerT,
)
from alpha.models.enums import AssetClass, BarTimeframe, DataSourceId, TakerSide
from alpha.models.events import BarEvent, EventMetadata, OrderBookEvent, QuoteEvent, TradeEvent

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_PRICE_SCALE = Decimal("1000000000")
_UTC = timezone.utc

# Databento ohlcv schemas for supported timeframes.
_TIMEFRAME_TO_SCHEMA: dict[BarTimeframe, str] = {
    BarTimeframe.S1: "ohlcv-1s",
    BarTimeframe.M1: "ohlcv-1m",
    BarTimeframe.H1: "ohlcv-1h",
    BarTimeframe.D1: "ohlcv-1d",
}

# record.rtype int values → (timeframe, schema string)
# Avoids importing databento_dbn just for the enum constants.
_RTYPE_TO_TF: dict[int, tuple[BarTimeframe, str]] = {
    32: (BarTimeframe.S1, "ohlcv-1s"),
    33: (BarTimeframe.M1, "ohlcv-1m"),
    34: (BarTimeframe.H1, "ohlcv-1h"),
    35: (BarTimeframe.D1, "ohlcv-1d"),
}

# Databento rtype ints for quick dispatch branching
_RTYPE_OHLCV = frozenset(_RTYPE_TO_TF.keys())
_RTYPE_MBP1 = 1
_RTYPE_MBP10 = 10
_RTYPE_TRADE = 0    # MBP_0 / Trades schema records
_RTYPE_SYMBOL_MAPPING = 22
_RTYPE_SYSTEM = 23
_RTYPE_ERROR = 21

# Matches the live gateway's rejection message for a schema the account's plan
# doesn't cover for real-time delivery, e.g. "Not authorized for mbp-10 schema"
# — confirmed 2026-07-12 that this plan has full mbo/mbp-10 access historically
# but not live, so this isn't hypothetical.
_UNAUTHORIZED_SCHEMA_RE = re.compile(r"Not authorized for (\S+) schema")


def _to_datetime(ts_ns: int) -> datetime:
    return datetime.fromtimestamp(ts_ns / 1e9, tz=_UTC)


def _to_decimal(raw: int) -> Decimal:
    return Decimal(raw) / _PRICE_SCALE


def _map_taker_side(side: object) -> TakerSide:
    # Empirically verified (2026-07-12, see the identical fix and its
    # methodology note in historical/sources/databento.py's _map_taker_side)
    # by joining trades against quotes and checking which side of the book
    # each print actually landed on: 'A'-tagged prints land at the bid and
    # 'B'-tagged prints land at the ask — the reverse of Databento's schema
    # docs as previously (and wrongly) understood here. This is a SEPARATE
    # copy of that function for the live ingestion path — confirmed
    # 2026-07-21 via a live-vs-backfill split cross-check on the same day
    # (MNQ-09 2026-07-10) that this copy still had the old, inverted
    # mapping while the historical one had already been fixed.
    s = str(side) if side is not None else ""
    if "A" in s:
        return TakerSide.SELL
    if "B" in s:
        return TakerSide.BUY
    return TakerSide.UNKNOWN


class DatabentoLiveFeedAdapter(LiveFeedAdapter):
    """
    Streams real-time bars, quotes, and trades from the Databento live gateway.

    A single Live session is created per adapter instance. Subscriptions for
    different schemas (ohlcv-1m, mbp-1, trades) share this session.
    """

    def __init__(
        self,
        settings: DatabentoSettings,
        registry: SymbolRegistry,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._client: db.Live | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._connected = False

        # Active handlers — keyed by (ticker, schema) for bars, ticker for others.
        self._bar_handlers: dict[tuple[str, str], BarHandlerT] = {}
        self._quote_handlers: dict[str, QuoteHandlerT] = {}
        self._trade_handlers: dict[str, TradeHandlerT] = {}
        self._tick_handlers: dict[str, TickHandlerT] = {}
        self._book_handlers: dict[str, BookHandlerT] = {}

        # instrument_id (int) → internal ticker — populated by SymbolMappingMsg.
        self._instrument_id_to_ticker: dict[int, str] = {}
        # Databento continuous symbol (e.g. "MNQ.c.0") → internal ticker.
        self._db_to_ticker: dict[str, str] = {}
        # schema → set of db_syms — used to replay subscriptions on reconnect.
        self._sub_specs: dict[str, set[str]] = defaultdict(set)

        self._observer = IngressObserver()
        self._skipped_records_handler: Callable[[], None] | None = None

    def set_skipped_records_handler(self, fn: Callable[[], None]) -> None:
        """Register a callback invoked when Databento reports SKIPPED_RECORDS."""
        self._skipped_records_handler = fn

    @property
    def source_id(self) -> DataSourceId:
        return DataSourceId.DATABENTO

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Symbol helpers ────────────────────────────────────────────────────────

    def _databento_symbol(self, ticker: str) -> str:
        try:
            sym = self._registry.get(ticker)
        except Exception:
            return ticker
        if sym.asset_class == AssetClass.FUTURE:
            root = sym.root_symbol or sym.ticker
            return f"{root}{self._settings.continuous_suffix}"
        return sym.ticker

    def _register_db_symbols(self, tickers: list[str]) -> list[str]:
        db_syms = []
        for ticker in tickers:
            db_sym = self._databento_symbol(ticker)
            self._db_to_ticker[db_sym] = ticker
            db_syms.append(db_sym)
        return db_syms

    def _resolve_ticker(self, record: object) -> str | None:
        """Resolve instrument_id on a record to our internal ticker."""
        iid = getattr(record, "instrument_id", None)
        if iid is not None:
            return self._instrument_id_to_ticker.get(iid)
        return None

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._client = db.Live(key=self._settings.api_key.get_secret_value())
        self._connected = True
        logger.info("Databento live adapter: session created")

    async def disconnect(self) -> None:
        self._connected = False
        if self._client is not None:
            try:
                self._client.stop()
            except Exception:
                pass
            self._client = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        logger.info("Databento live adapter disconnected")

    # ── Subscriptions ─────────────────────────────────────────────────────────

    async def subscribe_bars(
        self,
        symbols: list[str],
        timeframe: BarTimeframe,
        handler: BarHandlerT,
        *,
        start: datetime | None = None,
    ) -> None:
        if self._client is None:
            raise RuntimeError("Call connect() before subscribing")
        schema = _TIMEFRAME_TO_SCHEMA.get(timeframe)
        if schema is None:
            logger.warning(
                "Databento has no native %s schema; skipping. Use M1 and aggregate upstream.",
                timeframe,
            )
            return

        db_syms = self._register_db_symbols(symbols)
        subscribe_kwargs: dict = dict(
            dataset=self._settings.dataset,
            schema=schema,
            symbols=db_syms,
            stype_in=self._settings.stype_in,
        )
        if start is not None:
            subscribe_kwargs["start"] = start.isoformat()
            logger.info("Live replay from %s for schema=%s", start.isoformat(), schema)
        self._client.subscribe(**subscribe_kwargs)
        self._sub_specs[schema].update(db_syms)
        for ticker in symbols:
            self._bar_handlers[(ticker, schema)] = handler

        logger.info("Databento: subscribed to %s bars for %s", timeframe, symbols)

    async def subscribe_trades(
        self,
        symbols: list[str],
        handler: TradeHandlerT,
    ) -> None:
        if self._client is None:
            raise RuntimeError("Call connect() before subscribing")

        db_syms = self._register_db_symbols(symbols)
        self._client.subscribe(
            dataset=self._settings.dataset,
            schema="trades",
            symbols=db_syms,
            stype_in=self._settings.stype_in,
        )
        self._sub_specs["trades"].update(db_syms)
        for ticker in symbols:
            self._trade_handlers[ticker] = handler

        logger.info("Databento: subscribed to trades for %s", symbols)

    async def subscribe_quotes(
        self,
        symbols: list[str],
        handler: QuoteHandlerT,
    ) -> None:
        if self._client is None:
            raise RuntimeError("Call connect() before subscribing")

        db_syms = self._register_db_symbols(symbols)
        self._client.subscribe(
            dataset=self._settings.dataset,
            schema="mbp-1",
            symbols=db_syms,
            stype_in=self._settings.stype_in,
        )
        self._sub_specs["mbp-1"].update(db_syms)
        for ticker in symbols:
            self._quote_handlers[ticker] = handler

        logger.info("Databento: subscribed to mbp-1 quotes for %s", symbols)

    async def subscribe_order_book(
        self,
        symbols: list[str],
        handler: BookHandlerT,
        depth: int = 10,
    ) -> None:
        if self._client is None:
            raise RuntimeError("Call connect() before subscribing")

        db_syms = self._register_db_symbols(symbols)
        self._client.subscribe(
            dataset=self._settings.dataset,
            schema="mbp-10",
            symbols=db_syms,
            stype_in=self._settings.stype_in,
        )
        self._sub_specs["mbp-10"].update(db_syms)
        for ticker in symbols:
            self._book_handlers[ticker] = handler

        logger.info("Databento: subscribed to mbp-10 book for %s", symbols)

    async def subscribe_tick_trades(
        self,
        symbols: list[str],
        handler: TickHandlerT,
    ) -> None:
        if self._client is None:
            raise RuntimeError("Call connect() before subscribing")

        db_syms = self._register_db_symbols(symbols)
        # Only subscribe if trades schema not already active for these symbols.
        if not any(t in self._trade_handlers for t in symbols):
            self._client.subscribe(
                dataset=self._settings.dataset,
                schema="trades",
                symbols=db_syms,
                stype_in=self._settings.stype_in,
            )
            self._sub_specs["trades"].update(db_syms)
        for ticker in symbols:
            self._tick_handlers[ticker] = handler

        logger.info("Databento: subscribed to tick trades for %s", symbols)

    def start_stream(self) -> None:
        """Start the background record loop. Call once after all subscribe_* calls are done."""
        self._ensure_thread_started()

    # ── Symbol management ─────────────────────────────────────────────────────

    async def add_symbols(self, symbols: list[str]) -> None:
        logger.info("Databento add_symbols: %s — re-subscribe required", symbols)

    async def remove_symbols(self, symbols: list[str]) -> None:
        logger.warning("Databento remove_symbols: %s — not supported mid-session", symbols)

    # ── Background thread ─────────────────────────────────────────────────────

    def _ensure_thread_started(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._record_loop,
            daemon=True,
            name="databento-live",
        )
        self._thread.start()
        logger.info("Databento live record loop started")

    def _record_loop(self) -> None:
        """Blocking record loop with auto-reconnect — runs in a dedicated background thread."""
        assert self._loop is not None

        backoff = 1.0
        max_backoff = 60.0

        while self._connected:
            if self._client is None:
                break
            try:
                for record in self._client:
                    if not self._connected:
                        return
                    if not self._loop.is_running():
                        return
                    arrival_ns = time.time_ns()
                    self._observer.observe(record, arrival_ns)
                    try:
                        self._dispatch(record)
                    except Exception:
                        logger.exception("Databento: dispatch error for %r", record)
                logger.warning("Databento live record loop: server closed the connection")
            except db.common.error.BentoError as exc:
                msg = str(exc)
                if "pop from an empty deque" in msg:
                    # Databento SDK internal bug during session teardown — treat as a
                    # normal disconnect, not an error.
                    logger.warning("Databento live session closed unexpectedly (deque)")
                else:
                    unauthorized = _UNAUTHORIZED_SCHEMA_RE.search(msg)
                    if unauthorized is not None:
                        # A single unauthorized schema (e.g. mbp-10/mbo on a plan that
                        # only covers historical, not live, for that schema) tears down
                        # the *entire* connection, not just that subscription — and
                        # _reconnect() blindly replays every entry in _sub_specs, so
                        # without this, an unauthorized schema puts the whole live feed
                        # (bars, quotes, trades — everything) into a permanent
                        # reconnect-reject loop. Drop it so the rest can recover.
                        schema = unauthorized.group(1)
                        if self._sub_specs.pop(schema, None) is not None:
                            logger.error(
                                "Databento: %s is not authorized on this account's live "
                                "plan — dropped from the live feed so reconnects can "
                                "succeed for the rest (%s). Historical/backfill access for "
                                "this schema is unaffected.",
                                schema, list(self._sub_specs.keys()),
                            )
                    else:
                        logger.warning("Databento live session error: %s", exc)
            except Exception:
                logger.exception("Databento live record loop exited with error")

            if not self._connected:
                break

            logger.info("Databento: reconnecting in %.0fs...", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

            try:
                self._reconnect()
                backoff = 1.0
            except Exception:
                logger.exception("Databento: reconnect failed — will retry")

        self._connected = False
        logger.warning("Databento live record loop terminated")

    def _reconnect(self) -> None:
        """Recreate the Live session and replay all subscriptions. Called from the background thread."""
        assert self._loop is not None

        if self._client is not None:
            try:
                self._client.stop()
            except Exception:
                pass

        self._client = db.Live(key=self._settings.api_key.get_secret_value())
        # Instrument-id mappings are session-scoped; reset so fresh SymbolMappingMsgs repopulate them.
        self._instrument_id_to_ticker.clear()

        for schema, db_syms in self._sub_specs.items():
            self._client.subscribe(
                dataset=self._settings.dataset,
                schema=schema,
                symbols=list(db_syms),
                stype_in=self._settings.stype_in,
            )

        logger.info(
            "Databento: reconnected (%d schema(s): %s)",
            len(self._sub_specs),
            list(self._sub_specs.keys()),
        )

    # ── Dispatch ──────────────────────────────────────────────────────────────

    def _dispatch(self, record: object) -> None:
        rtype = getattr(record, "rtype", None)
        if rtype is None:
            return

        # Convert RType enum to int if needed
        rtype_int = int(rtype)

        if rtype_int in _RTYPE_OHLCV:
            self._dispatch_bar(record, rtype_int)
        elif rtype_int == _RTYPE_MBP1:
            self._dispatch_quote(record)
        elif rtype_int == _RTYPE_MBP10:
            self._dispatch_book(record)
        elif rtype_int == _RTYPE_TRADE:
            self._dispatch_trade(record)
        elif rtype_int == _RTYPE_SYMBOL_MAPPING:
            self._handle_symbol_mapping(record)
        elif rtype_int == _RTYPE_SYSTEM:
            msg = getattr(record, "msg", "") or ""
            if "SKIPPED" in msg.upper():
                logger.warning("Databento SKIPPED_RECORDS: %s", msg)
                if self._skipped_records_handler is not None:
                    self._skipped_records_handler()
            else:
                logger.debug("Databento system: %s", msg)
        elif rtype_int == _RTYPE_ERROR:
            logger.error("Databento error: %s", getattr(record, "err", record))

    def _handle_symbol_mapping(self, record: object) -> None:
        """
        Build instrument_id → ticker map from gateway SymbolMappingMsg.

        The gateway sends this immediately on subscribe. stype_in_symbol is
        the continuous symbol we requested (e.g. "MNQ.c.0"); instrument_id
        is what all subsequent data records carry.
        """
        iid = getattr(record, "instrument_id", None)
        stype_in_sym = getattr(record, "stype_in_symbol", None)
        if iid is None or not stype_in_sym:
            return
        ticker = self._db_to_ticker.get(stype_in_sym)
        if ticker is not None:
            self._instrument_id_to_ticker[iid] = ticker
            logger.debug(
                "Databento symbol mapping: instrument_id=%d → %s (%s)",
                iid, ticker, stype_in_sym,
            )

    def _dispatch_bar(self, record: object, rtype_int: int) -> None:
        ticker = self._resolve_ticker(record)
        if ticker is None:
            return
        tf, schema = _RTYPE_TO_TF[rtype_int]
        handler = self._bar_handlers.get((ticker, schema))
        if handler is None:
            return

        now = datetime.now(tz=_UTC)
        event = BarEvent(
            symbol=ticker,
            timestamp=_to_datetime(record.ts_event),  # type: ignore[union-attr]
            timeframe=tf,
            open=_to_decimal(record.open),    # type: ignore[union-attr]
            high=_to_decimal(record.high),    # type: ignore[union-attr]
            low=_to_decimal(record.low),      # type: ignore[union-attr]
            close=_to_decimal(record.close),  # type: ignore[union-attr]
            volume=int(record.volume),         # type: ignore[union-attr]
            metadata=EventMetadata(
                source=DataSourceId.DATABENTO,
                received_at=now,
                is_replay=False,
            ),
        )
        assert self._loop is not None
        try:
            asyncio.run_coroutine_threadsafe(handler(event), self._loop)
        except RuntimeError:
            pass  # Loop closed during shutdown

    def _dispatch_quote(self, record: object) -> None:
        ticker = self._resolve_ticker(record)
        if ticker is None:
            return
        handler = self._quote_handlers.get(ticker)
        if handler is None:
            return

        # MBP1Msg: levels[0] is a BidAskPair with bid_px, bid_sz, ask_px, ask_sz.
        levels = getattr(record, "levels", None)
        if not levels:
            return
        lvl = levels[0]

        # bid_px/ask_px carry the same UNDEF_PRICE (INT64_MAX) sentinel as
        # record.price when one side of the book is momentarily empty (e.g.
        # thin liquidity, pre-open). Undetected, _to_decimal() turns that
        # into a ~9.2 billion "price" that flows straight through to the web
        # chart. Skip the update entirely rather than publish a bad side —
        # the previous bid/ask stays displayed until a real quote arrives.
        if lvl.bid_px == dbn.UNDEF_PRICE or lvl.ask_px == dbn.UNDEF_PRICE:
            return

        # MBP1Msg.price is the last trade price that triggered this book update.
        # Filter out the UNDEF_PRICE sentinel (INT64_MAX) which appears on
        # quote-only updates (no trade triggered the change).
        raw_price = getattr(record, "price", dbn.UNDEF_PRICE)
        last_price = (
            _to_decimal(raw_price) if raw_price != dbn.UNDEF_PRICE else None
        )

        now = datetime.now(tz=_UTC)
        event = QuoteEvent(
            symbol=ticker,
            timestamp=_to_datetime(record.ts_event),  # type: ignore[union-attr]
            bid_price=_to_decimal(lvl.bid_px),
            bid_size=int(lvl.bid_sz),
            ask_price=_to_decimal(lvl.ask_px),
            ask_size=int(lvl.ask_sz),
            last_price=last_price,
            metadata=EventMetadata(
                source=DataSourceId.DATABENTO,
                received_at=now,
                is_replay=False,
            ),
        )
        assert self._loop is not None
        try:
            asyncio.run_coroutine_threadsafe(handler(event), self._loop)
        except RuntimeError:
            pass  # Loop closed during shutdown

    def _dispatch_trade(self, record: object) -> None:
        ticker = self._resolve_ticker(record)
        if ticker is None:
            return

        # Filter out non-standard trade types so partial bar accumulation
        # matches Databento's ohlcv-* bars (which only include match_type='E').
        #
        # CME Globex match_type values:
        #   'E' = auction entry (standard CLOB match)   ← keep
        #   'B' = block trade (off-market, any price)   ← reject
        #   'T' = EFRP (exchange for related position)  ← reject
        #   'I' = implied (spread-leg synthetic print)  ← reject
        #   None/unknown → accept (be permissive for unknown venues)
        match_type = getattr(record, "match_type", None)
        if match_type is not None and str(match_type) not in {"E", ""}:
            logger.debug(
                "Databento: skipping non-standard trade match_type=%s price=%s sym=%s",
                match_type, getattr(record, "price", None), ticker,
            )
            return

        price = float(_to_decimal(record.price))  # type: ignore[union-attr]
        size = int(record.size)                    # type: ignore[union-attr]

        # Synchronous high-frequency tick handler — no asyncio overhead.
        tick_handler = self._tick_handlers.get(ticker)
        if tick_handler is not None:
            try:
                tick_handler(ticker, price, size)
            except Exception:
                logger.exception("Databento tick handler error for %s", ticker)

        trade_handler = self._trade_handlers.get(ticker)
        if trade_handler is None:
            return

        now = datetime.now(tz=_UTC)
        event = TradeEvent(
            symbol=ticker,
            timestamp=_to_datetime(record.ts_event),  # type: ignore[union-attr]
            price=_to_decimal(record.price),           # type: ignore[union-attr]
            size=size,
            taker_side=_map_taker_side(getattr(record, "side", None)),
            trade_id=str(getattr(record, "sequence", "")),
            metadata=EventMetadata(
                source=DataSourceId.DATABENTO,
                received_at=now,
                is_replay=False,
            ),
        )
        assert self._loop is not None
        try:
            asyncio.run_coroutine_threadsafe(trade_handler(event), self._loop)
        except RuntimeError:
            pass  # Loop closed during shutdown

    def _dispatch_book(self, record: object) -> None:
        ticker = self._resolve_ticker(record)
        if ticker is None:
            return
        handler = self._book_handlers.get(ticker)
        if handler is None:
            return

        levels = getattr(record, "levels", None) or []
        now = datetime.now(tz=_UTC)
        event = OrderBookEvent(
            symbol=ticker,
            timestamp=_to_datetime(record.ts_event),  # type: ignore[union-attr]
            bids=[(_to_decimal(lvl.bid_px), int(lvl.bid_sz)) for lvl in levels],
            asks=[(_to_decimal(lvl.ask_px), int(lvl.ask_sz)) for lvl in levels],
            metadata=EventMetadata(
                source=DataSourceId.DATABENTO,
                received_at=now,
                is_replay=False,
            ),
        )
        assert self._loop is not None
        try:
            asyncio.run_coroutine_threadsafe(handler(event), self._loop)
        except RuntimeError:
            pass  # Loop closed during shutdown
