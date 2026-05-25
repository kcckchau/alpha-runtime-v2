"""Unit tests for core models and event contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, DataSourceId
from alpha.models.events import BarEvent, EventMetadata
from alpha.models.symbol import Symbol
from tests.conftest import make_bar_event


class TestBar:
    def test_valid_bar(self) -> None:
        bar = Bar(
            symbol="AAPL",
            timeframe=BarTimeframe.M1,
            timestamp=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
            open=Decimal("180.00"),
            high=Decimal("181.00"),
            low=Decimal("179.50"),
            close=Decimal("180.75"),
            volume=50_000,
        )
        assert bar.is_bullish
        assert bar.range == Decimal("1.50")

    def test_invalid_high_low(self) -> None:
        with pytest.raises(ValueError, match="high must be >= low"):
            Bar(
                symbol="AAPL",
                timeframe=BarTimeframe.M1,
                timestamp=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                open=Decimal("180.00"),
                high=Decimal("179.00"),   # high < low — invalid
                low=Decimal("180.00"),
                close=Decimal("179.50"),
                volume=1000,
            )

    def test_bearish_bar(self) -> None:
        bar = Bar(
            symbol="AAPL",
            timeframe=BarTimeframe.M1,
            timestamp=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
            open=Decimal("181.00"),
            high=Decimal("181.50"),
            low=Decimal("179.00"),
            close=Decimal("179.50"),
            volume=80_000,
        )
        assert bar.is_bearish
        assert not bar.is_bullish


class TestBarEvent:
    def test_event_is_frozen(self) -> None:
        event = make_bar_event()
        with pytest.raises(Exception):  # frozen pydantic model
            event.symbol = "TSLA"  # type: ignore[misc]

    def test_event_discriminator(self) -> None:
        from alpha.models.enums import EventType
        event = make_bar_event()
        assert event.event_type == EventType.BAR


class TestSymbol:
    def test_ticker_normalized_uppercase(self) -> None:
        from alpha.models.enums import AssetClass
        sym = Symbol(ticker="aapl", exchange="nasdaq", asset_class=AssetClass.EQUITY)
        assert sym.ticker == "AAPL"
        assert sym.exchange == "NASDAQ"

    def test_equality_by_ticker(self) -> None:
        from alpha.models.enums import AssetClass
        s1 = Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.EQUITY)
        s2 = Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.EQUITY)
        assert s1 == s2
