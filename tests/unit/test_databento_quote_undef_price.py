"""
Unit tests for DatabentoLiveFeedAdapter._dispatch_quote()'s handling of the
UNDEF_PRICE sentinel on bid/ask levels.

Reproduces a bug flagged from a live report: the web chart's price/bid/ask
was "jumping around". record.price (last trade price) was already guarded
against Databento's UNDEF_PRICE sentinel (INT64_MAX, used when one side of
the book is momentarily empty), but lvl.bid_px/lvl.ask_px were passed
straight to _to_decimal() with no such guard — turning INT64_MAX into a
~9.2 billion "price" published straight to ConnectionManager and the web
chart whenever one side of the top-of-book briefly had no quote.

Pins the fixed behavior: a quote record with either side undefined is
dropped entirely (the chart keeps showing the last real bid/ask) rather
than publishing a garbage price.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import databento_dbn as dbn
import pytest

from alpha.engines.live.adapters.databento import DatabentoLiveFeedAdapter
from alpha.models.events import QuoteEvent


def _level(bid_px: int, ask_px: int, bid_sz: int = 5, ask_sz: int = 5) -> SimpleNamespace:
    return SimpleNamespace(bid_px=bid_px, bid_sz=bid_sz, ask_px=ask_px, ask_sz=ask_sz)


def _quote_record(levels: list[SimpleNamespace], instrument_id: int = 1, price: int = dbn.UNDEF_PRICE) -> SimpleNamespace:
    return SimpleNamespace(
        instrument_id=instrument_id,
        levels=levels,
        price=price,
        ts_event=1_753_000_000_000_000_000,
    )


def _make_adapter() -> DatabentoLiveFeedAdapter:
    adapter = DatabentoLiveFeedAdapter(settings=MagicMock(), registry=MagicMock())
    adapter._instrument_id_to_ticker[1] = "MNQ-09"
    adapter._loop = asyncio.get_event_loop()
    return adapter


class TestQuoteUndefPriceGuard:
    async def test_undef_bid_px_drops_the_quote(self) -> None:
        adapter = _make_adapter()
        received: list[QuoteEvent] = []

        async def handler(event: QuoteEvent) -> None:
            received.append(event)

        adapter._quote_handlers["MNQ-09"] = handler
        record = _quote_record([_level(bid_px=dbn.UNDEF_PRICE, ask_px=27_843_250_000_000)])

        adapter._dispatch_quote(record)
        await asyncio.sleep(0.01)

        assert received == []

    async def test_undef_ask_px_drops_the_quote(self) -> None:
        adapter = _make_adapter()
        received: list[QuoteEvent] = []

        async def handler(event: QuoteEvent) -> None:
            received.append(event)

        adapter._quote_handlers["MNQ-09"] = handler
        record = _quote_record([_level(bid_px=27_842_750_000_000, ask_px=dbn.UNDEF_PRICE)])

        adapter._dispatch_quote(record)
        await asyncio.sleep(0.01)

        assert received == []

    async def test_normal_quote_still_dispatches(self) -> None:
        adapter = _make_adapter()
        received: list[QuoteEvent] = []

        async def handler(event: QuoteEvent) -> None:
            received.append(event)

        adapter._quote_handlers["MNQ-09"] = handler
        record = _quote_record([_level(bid_px=27_842_750_000_000, ask_px=27_843_250_000_000)])

        adapter._dispatch_quote(record)
        await asyncio.sleep(0.01)

        assert len(received) == 1
        assert received[0].bid_price == 27842.75
        assert received[0].ask_price == 27843.25
