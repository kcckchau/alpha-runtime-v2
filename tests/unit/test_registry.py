"""Unit tests for SymbolRegistry."""

from __future__ import annotations

import pytest

from alpha.core.registry import SymbolNotFoundError, SymbolRegistry
from alpha.models.enums import AssetClass
from alpha.models.symbol import Symbol


def make_symbol(ticker: str, exchange: str = "NASDAQ") -> Symbol:
    return Symbol(ticker=ticker, exchange=exchange, asset_class=AssetClass.EQUITY)


class TestSymbolRegistry:
    def test_register_and_get(self) -> None:
        reg = SymbolRegistry()
        sym = make_symbol("AAPL")
        reg.register(sym)
        assert reg.get("AAPL") == sym

    def test_get_missing_raises(self) -> None:
        reg = SymbolRegistry()
        with pytest.raises(SymbolNotFoundError):
            reg.get("NONEXISTENT")

    def test_contains(self) -> None:
        reg = SymbolRegistry()
        reg.register(make_symbol("SPY"))
        assert reg.contains("SPY")
        assert not reg.contains("QQQ")

    def test_active_filters_inactive(self) -> None:
        reg = SymbolRegistry()
        active = make_symbol("AAPL")
        inactive = Symbol(
            ticker="DEAD", exchange="NYSE", asset_class=AssetClass.EQUITY, is_active=False
        )
        reg.register(active)
        reg.register(inactive)
        assert len(reg.active()) == 1
        assert reg.active()[0].ticker == "AAPL"

    def test_by_asset_class(self) -> None:
        reg = SymbolRegistry()
        reg.register(Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.EQUITY))
        reg.register(Symbol(ticker="SPY", exchange="ARCA", asset_class=AssetClass.ETF))
        equities = reg.by_asset_class(AssetClass.EQUITY)
        etfs = reg.by_asset_class(AssetClass.ETF)
        assert len(equities) == 1
        assert len(etfs) == 1
