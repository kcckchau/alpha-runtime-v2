"""
Unit tests for BarFlowAggregator._compute_quote_imbalance.

Verifies the incremental TWAP calculation (O(1) per update) produces
the same result as the previous O(n) loop over quote_updates.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from alpha.engines.flow.aggregator import BarFlowAggregator, _Window
from alpha.models.enums import DataSourceId
from alpha.models.events import EventMetadata, QuoteEvent

_UTC = timezone.utc
TICKER = "MNQ-09"


def _meta() -> EventMetadata:
    return EventMetadata(
        source=DataSourceId.DATABENTO,
        received_at=datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC),
        is_replay=True,
    )


def _quote(ts: datetime, bid_sz: int, ask_sz: int) -> QuoteEvent:
    return QuoteEvent(
        symbol=TICKER,
        timestamp=ts,
        bid_price=Decimal("20050.00"),
        bid_size=bid_sz,
        ask_price=Decimal("20050.25"),
        ask_size=ask_sz,
        metadata=_meta(),
    )


def _make_window() -> tuple[_Window, BarFlowAggregator]:
    t0 = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
    t1 = t0 + timedelta(minutes=1)
    w = _Window(t0, t1, large_trade_threshold=10)
    agg = BarFlowAggregator(symbol=TICKER, event_bus=None)  # type: ignore[arg-type]
    return w, agg


class TestComputeQuoteImbalance:
    def test_fewer_than_two_quotes_returns_none(self):
        w, agg = _make_window()
        assert agg._compute_quote_imbalance(w) == (None, None, None)

        t0 = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
        w.add_quote(_quote(t0, bid_sz=10, ask_sz=5))
        assert agg._compute_quote_imbalance(w) == (None, None, None)

    def test_two_quotes_equal_bid_ask(self):
        w, agg = _make_window()
        t0 = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
        w.add_quote(_quote(t0,                bid_sz=10, ask_sz=10))
        w.add_quote(_quote(t0 + timedelta(seconds=10), bid_sz=10, ask_sz=10))

        twap_bid, twap_ask, imbalance = agg._compute_quote_imbalance(w)
        assert twap_bid == pytest.approx(10.0)
        assert twap_ask == pytest.approx(10.0)
        assert imbalance == pytest.approx(0.5)

    def test_twap_weighted_by_time(self):
        """
        bid_sz=20 for 5s, then bid_sz=10 for 15s.
        TWAP bid = (20*5 + 10*15) / (5+15) = (100+150)/20 = 12.5
        ask_sz=5  for 5s, then ask_sz=5  for 15s → 5.0
        imbalance = 12.5 / (12.5 + 5.0) = 0.714...
        """
        w, agg = _make_window()
        t0 = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
        w.add_quote(_quote(t0,                        bid_sz=20, ask_sz=5))
        w.add_quote(_quote(t0 + timedelta(seconds=5), bid_sz=10, ask_sz=5))
        w.add_quote(_quote(t0 + timedelta(seconds=20), bid_sz=8, ask_sz=5))

        twap_bid, twap_ask, imbalance = agg._compute_quote_imbalance(w)
        assert twap_bid == pytest.approx(12.5, rel=1e-6)
        assert twap_ask == pytest.approx(5.0,  rel=1e-6)
        assert imbalance == pytest.approx(12.5 / 17.5, rel=1e-6)

    def test_zero_time_delta_quotes_skipped(self):
        """Quotes at identical timestamps contribute zero weight — same as old loop."""
        w, agg = _make_window()
        t0 = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
        w.add_quote(_quote(t0, bid_sz=100, ask_sz=1))
        w.add_quote(_quote(t0, bid_sz=100, ask_sz=1))   # same ts — dt=0, skipped
        w.add_quote(_quote(t0 + timedelta(seconds=10), bid_sz=5, ask_sz=5))

        # Only the first→third pair contributes (10s); second pair has dt=0
        twap_bid, twap_ask, imbalance = agg._compute_quote_imbalance(w)
        assert twap_bid == pytest.approx(100.0, rel=1e-6)
        assert twap_ask == pytest.approx(1.0,   rel=1e-6)

    def test_bid_dominant_imbalance(self):
        """Large bid size held for most of the bar → imbalance > 0.5."""
        w, agg = _make_window()
        t0 = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
        w.add_quote(_quote(t0,                         bid_sz=50, ask_sz=5))
        w.add_quote(_quote(t0 + timedelta(seconds=59), bid_sz=5,  ask_sz=50))

        twap_bid, twap_ask, imbalance = agg._compute_quote_imbalance(w)
        assert imbalance is not None
        assert imbalance > 0.5

    def test_many_quotes_accumulate_correctly(self):
        """100 evenly-spaced quotes with constant bid/ask → TWAP = constant."""
        w, agg = _make_window()
        t0 = datetime(2026, 8, 26, 14, 30, 0, tzinfo=_UTC)
        for i in range(100):
            w.add_quote(_quote(t0 + timedelta(seconds=i * 0.6), bid_sz=8, ask_sz=12))

        twap_bid, twap_ask, imbalance = agg._compute_quote_imbalance(w)
        assert twap_bid == pytest.approx(8.0,  rel=1e-4)
        assert twap_ask == pytest.approx(12.0, rel=1e-4)
        assert imbalance == pytest.approx(8.0 / 20.0, rel=1e-4)
