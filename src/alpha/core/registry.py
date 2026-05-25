"""
Symbol registry: the single source of truth for active symbols at runtime.

All engines obtain Symbol definitions from this registry rather than config
files directly, so there is one place to update, validate, and query symbols.
"""

from __future__ import annotations

import logging

from alpha.models.enums import AssetClass
from alpha.models.symbol import Symbol, SymbolConfig


logger = logging.getLogger(__name__)


class SymbolNotFoundError(KeyError):
    pass


class SymbolRegistry:
    """In-memory symbol registry. Thread-safe for asyncio single-thread use."""

    def __init__(self) -> None:
        self._symbols: dict[str, Symbol] = {}
        self._configs: dict[str, SymbolConfig] = {}

    # ── Registration ──────────────────────────────────────────────────────────

    def register(self, symbol: Symbol, config: SymbolConfig | None = None) -> None:
        if symbol.ticker in self._symbols:
            logger.warning("Re-registering symbol %s", symbol.ticker)
        self._symbols[symbol.ticker] = symbol
        if config is not None:
            self._configs[symbol.ticker] = config
        logger.debug("Registered symbol %s", symbol.ticker)

    def register_many(
        self,
        symbols: list[Symbol],
        configs: list[SymbolConfig] | None = None,
    ) -> None:
        cfg_map = {c.ticker: c for c in (configs or [])}
        for sym in symbols:
            self.register(sym, cfg_map.get(sym.ticker))

    def deregister(self, ticker: str) -> None:
        self._symbols.pop(ticker, None)
        self._configs.pop(ticker, None)

    # ── Queries ───────────────────────────────────────────────────────────────

    def get(self, ticker: str) -> Symbol:
        try:
            return self._symbols[ticker.upper()]
        except KeyError:
            raise SymbolNotFoundError(ticker) from None

    def get_config(self, ticker: str) -> SymbolConfig | None:
        return self._configs.get(ticker.upper())

    def all(self) -> list[Symbol]:
        return list(self._symbols.values())

    def active(self) -> list[Symbol]:
        return [s for s in self._symbols.values() if s.is_active]

    def tickers(self) -> list[str]:
        return list(self._symbols.keys())

    def by_asset_class(self, asset_class: AssetClass) -> list[Symbol]:
        return [s for s in self._symbols.values() if s.asset_class == asset_class]

    def by_exchange(self, exchange: str) -> list[Symbol]:
        return [s for s in self._symbols.values() if s.exchange == exchange.upper()]

    def contains(self, ticker: str) -> bool:
        return ticker.upper() in self._symbols

    def __len__(self) -> int:
        return len(self._symbols)

    def __repr__(self) -> str:
        return f"SymbolRegistry({list(self._symbols.keys())})"
