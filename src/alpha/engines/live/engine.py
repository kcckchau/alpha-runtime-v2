"""
Engine 3 — Live Ingestion Engine

Responsibilities:
  - Manage multi-symbol streaming subscriptions
  - Ingest bars, trades, bid/ask quotes, and order book updates
  - Emit ONLY normalized events (BarEvent, TradeEvent, QuoteEvent, OrderBookEvent)
    into the EventBus — no raw vendor data flows downstream

All events emitted here carry `metadata.is_replay = False`.

The engine supports runtime symbol add/remove without restart.
"""

from __future__ import annotations

import logging

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.live.adapters.base import LiveFeedAdapter
from alpha.models.enums import BarTimeframe, HealthStatus
from alpha.models.events import BarEvent, OrderBookEvent, QuoteEvent, TradeEvent

logger = logging.getLogger(__name__)


class LiveIngestionEngine(BaseEngine):
    """
    Subscribes to live market data and fans out normalized events.

    Usage::

        engine.register_adapter(AlpacaFeedAdapter(settings))
        await engine.initialize()
        await engine.start()
        # EventBus now receives BarEvents, TradeEvents, QuoteEvents
    """

    def __init__(
        self,
        settings: AlphaSettings,
        event_bus: EventBus,
        registry: SymbolRegistry,
    ) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._registry = registry
        self._adapters: dict[str, LiveFeedAdapter] = {}
        self._bars_received: int = 0
        self._trades_received: int = 0
        self._quotes_received: int = 0
        self._latest_bars: dict[str, BarEvent] = {}
        self._latest_partial_bars: dict[str, BarEvent] = {}
        self._latest_quotes: dict[str, QuoteEvent] = {}

    @property
    def name(self) -> str:
        return "LiveIngestionEngine"

    # ── Adapter registration ──────────────────────────────────────────────────

    def register_adapter(self, adapter: LiveFeedAdapter) -> None:
        self._adapters[adapter.source_id] = adapter
        logger.info("Registered live adapter: %s", adapter.source_id)

    @property
    def primary_adapter(self) -> LiveFeedAdapter:
        sid = self._settings.live.primary_source
        if sid not in self._adapters:
            raise RuntimeError(f"Primary live adapter '{sid}' not registered")
        return self._adapters[sid]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        pass

    async def _on_start(self) -> None:
        symbols = self._registry.tickers()
        if not symbols or not self._adapters:
            logger.warning("LiveIngestionEngine: no adapters or symbols configured")
            return

        adapter = self.primary_adapter
        try:
            await adapter.connect()
        except Exception as exc:
            logger.error(
                "LiveIngestionEngine: could not connect to %s — %s\n"
                "  Runtime will continue without live data. "
                "Fix the connection and restart.",
                adapter.source_id, exc,
            )
            return

        await adapter.subscribe_bars(symbols, BarTimeframe.M1, self._on_bar)
        await adapter.subscribe_trades(symbols, self._on_trade)
        await adapter.subscribe_quotes(symbols, self._on_quote)

        logger.info(
            "Live subscriptions active: %d symbols via %s",
            len(symbols), adapter.source_id,
        )

    async def _on_stop(self) -> None:
        for adapter in self._adapters.values():
            try:
                await adapter.disconnect()
            except Exception:
                logger.exception("Error disconnecting adapter %s", adapter.source_id)

    async def _health_check(self) -> EngineHealth:
        connected = [
            sid for sid, a in self._adapters.items() if a.is_connected
        ]
        details = {
            "adapters_connected": connected,
            "bars_received": self._bars_received,
            "trades_received": self._trades_received,
            "quotes_received": self._quotes_received,
        }
        status = HealthStatus.HEALTHY if connected else HealthStatus.UNHEALTHY
        return EngineHealth(status, self.name, details)

    # ── Runtime symbol management ─────────────────────────────────────────────

    async def add_symbol(self, ticker: str) -> None:
        for adapter in self._adapters.values():
            await adapter.add_symbols([ticker])

    async def remove_symbol(self, ticker: str) -> None:
        for adapter in self._adapters.values():
            await adapter.remove_symbols([ticker])

    def latest_bars(self) -> dict[str, BarEvent]:
        return dict(self._latest_bars)

    def latest_partial_bars(self) -> dict[str, BarEvent]:
        return dict(self._latest_partial_bars)

    def latest_quotes(self) -> dict[str, QuoteEvent]:
        return dict(self._latest_quotes)

    # ── Handlers — called by adapter, forwarded to EventBus ──────────────────

    async def _on_bar(self, event: BarEvent) -> None:
        self._bars_received += 1
        if event.is_partial:
            # Partial (in-progress) bars are only used for live display — they
            # are not published to the event bus so storage/downstream engines
            # never see incomplete bar data.
            self._latest_partial_bars[event.symbol] = event
            return
        self._latest_bars[event.symbol] = event
        await self._event_bus.publish(event)

    async def _on_trade(self, event: TradeEvent) -> None:
        self._trades_received += 1
        await self._event_bus.publish(event)

    async def _on_quote(self, event: QuoteEvent) -> None:
        self._quotes_received += 1
        self._latest_quotes[event.symbol] = event
        await self._event_bus.publish(event)

    async def _on_order_book(self, event: OrderBookEvent) -> None:
        await self._event_bus.publish(event)
