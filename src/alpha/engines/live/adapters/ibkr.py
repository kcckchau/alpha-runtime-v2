"""
IBKR live feed adapter.

Bar streaming: reqHistoricalData with keepUpToDate=True
  - IBKR streams bar completions into the existing BarDataList
  - bars[-1] is always the in-progress bar
  - bars[-2] is the just-completed bar when has_new_bar=True

Quote streaming: reqMktData
  - Fires ticker.updateEvent on each bid/ask tick
  - We debounce and emit QuoteEvent only when both bid and ask are valid
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from alpha.config.settings import IBKRSettings
from alpha.engines.ibkr.connection import IBKRConnection
from alpha.engines.ibkr.contracts import (
    TIMEFRAME_TO_IBKR,
    make_contract,
    normalize_bar,
    normalize_quote,
)
from alpha.engines.live.adapters.base import (
    BarHandlerT,
    BookHandlerT,
    LiveFeedAdapter,
    QuoteHandlerT,
    TradeHandlerT,
)
from alpha.models.enums import BarTimeframe, DataSourceId
from alpha.models.symbol import Symbol

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class IBKRLiveFeedAdapter(LiveFeedAdapter):
    """
    Streams real-time bars and quotes from IBKR via ib_insync.

    One `reqHistoricalData(keepUpToDate=True)` call per symbol per timeframe.
    One `reqMktData` call per symbol for bid/ask quotes.
    """

    def __init__(
        self,
        conn: IBKRConnection,
        settings: IBKRSettings,
        symbols: list[Symbol],
    ) -> None:
        self._conn = conn
        self._settings = settings
        self._symbols = {sym.ticker: sym for sym in symbols}
        # Active ib_insync subscription handles
        self._bar_lists: dict[str, object] = {}    # ticker → BarDataList
        self._tickers: dict[str, object] = {}       # ticker → Ticker
        self._bar_callbacks: dict[str, object] = {}
        self._quote_callbacks: dict[str, object] = {}

    @property
    def source_id(self) -> DataSourceId:
        return DataSourceId.INTERACTIVE_BROKERS

    @property
    def is_connected(self) -> bool:
        return self._conn.is_connected

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        await self._conn.get()
        logger.info("IBKR live feed adapter connected")

    async def disconnect(self) -> None:
        ib = await self._conn.get()
        for symbol, bar_list in self._bar_lists.items():
            try:
                ib.cancelHistoricalData(bar_list)
            except Exception:
                pass
        for symbol, ticker in self._tickers.items():
            try:
                ib.cancelMktData(ticker.contract)
            except Exception:
                pass
        self._bar_lists.clear()
        self._tickers.clear()
        logger.info("IBKR live feed adapter disconnected")

    # ── Subscriptions ─────────────────────────────────────────────────────────

    async def subscribe_bars(
        self,
        symbols: list[str],
        timeframe: BarTimeframe,
        handler: BarHandlerT,
    ) -> None:
        ib = await self._conn.get()
        bar_size = TIMEFRAME_TO_IBKR[timeframe]

        for ticker in symbols:
            if ticker in self._bar_lists:
                logger.debug("Already subscribed to bars for %s", ticker)
                continue

            sym = self._symbols.get(ticker)
            if sym is None:
                logger.warning("Symbol %s not in registry, skipping bar subscription", ticker)
                continue

            contract = make_contract(sym)
            logger.info("Subscribing to live bars: %s [%s]", ticker, bar_size)

            bar_list = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting=bar_size,
                whatToShow=self._settings.what_to_show,
                useRTH=self._settings.use_rth,
                formatDate=2,
                keepUpToDate=True,
            )

            self._bar_lists[ticker] = bar_list

            # Capture loop variables in closure
            def make_callback(sym_ticker: str, tf: BarTimeframe, h: BarHandlerT) -> object:
                def on_bar_update(bars: object, has_new_bar: bool) -> None:
                    if not has_new_bar:
                        return
                    bar_seq = list(bars)  # type: ignore[arg-type]
                    if len(bar_seq) < 2:
                        return
                    completed = bar_seq[-2]
                    event = normalize_bar(completed, sym_ticker, tf, is_replay=False)
                    asyncio.ensure_future(h(event))
                return on_bar_update

            cb = make_callback(ticker, timeframe, handler)
            bar_list.updateEvent += cb  # type: ignore[union-attr]
            self._bar_callbacks[ticker] = cb

    async def subscribe_trades(
        self,
        symbols: list[str],
        handler: TradeHandlerT,
    ) -> None:
        # IBKR tick-by-tick trade data requires reqTickByTickData.
        # For now, trades are derived from bar closes. Implement when needed.
        logger.debug("IBKR trade tick subscription not yet implemented")

    async def subscribe_quotes(
        self,
        symbols: list[str],
        handler: QuoteHandlerT,
    ) -> None:
        ib = await self._conn.get()

        for ticker_str in symbols:
            if ticker_str in self._tickers:
                continue

            sym = self._symbols.get(ticker_str)
            if sym is None:
                continue

            contract = make_contract(sym)
            ticker = ib.reqMktData(contract, "", False, False)
            self._tickers[ticker_str] = ticker

            def make_quote_cb(sym_ticker: str, h: QuoteHandlerT) -> object:
                def on_tick(t: object) -> None:
                    event = normalize_quote(t, sym_ticker)
                    if event is not None:
                        asyncio.ensure_future(h(event))
                return on_tick

            cb = make_quote_cb(ticker_str, handler)
            ticker.updateEvent += cb  # type: ignore[union-attr]
            self._quote_callbacks[ticker_str] = cb

        logger.info("Subscribed to quotes for %d symbols", len(symbols))

    # ── Symbol management ─────────────────────────────────────────────────────

    async def add_symbols(self, symbols: list[str]) -> None:
        logger.info("add_symbols: %s (re-subscribe required)", symbols)

    async def remove_symbols(self, symbols: list[str]) -> None:
        ib = await self._conn.get()
        for ticker in symbols:
            if ticker in self._bar_lists:
                ib.cancelHistoricalData(self._bar_lists.pop(ticker))
            if ticker in self._tickers:
                t = self._tickers.pop(ticker)
                ib.cancelMktData(t.contract)  # type: ignore[union-attr]
