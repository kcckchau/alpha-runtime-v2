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
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.live.adapters.base import LiveFeedAdapter, TickHandlerT
from alpha.models.enums import BarTimeframe, HealthStatus
from alpha.models.events import BarEvent, OrderBookEvent, QuoteEvent, TradeEvent

logger = logging.getLogger(__name__)


@dataclass
class _PartialAccum:
    """Running OHLCV state for the current 1-minute bar, built from trade ticks."""
    minute_ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


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
        # Last published bid/ask prices per symbol — used to debounce quote events.
        # mbp-1 from Databento fires on every book change including size-only updates
        # (hundreds/sec for MNQ). We only publish to the EventBus when the price changes.
        self._last_published_bid: dict[str, object] = {}
        self._last_published_ask: dict[str, object] = {}
        # Partial bar accumulators — built from trade ticks when the adapter does not
        # natively stream in-progress bars (e.g. Databento ohlcv-1m only delivers
        # completed bars). Keyed by symbol; reset on each new minute boundary.
        self._partial_accum: dict[str, _PartialAccum] = {}

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
        await adapter.subscribe_bars(symbols, BarTimeframe.S1, self._on_bar)
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

    async def subscribe_tick_trades(self, handler: TickHandlerT) -> None:
        """Wire a sync tick handler to the primary adapter's tick-by-tick feed."""
        symbols = self._registry.tickers()
        adapter = self.primary_adapter
        await adapter.subscribe_tick_trades(symbols, handler)

    # ── Handlers — called by adapter, forwarded to EventBus ──────────────────

    async def _on_bar(self, event: BarEvent) -> None:
        self._bars_received += 1
        if event.is_partial:
            # Partial bars are only for live display — not published to EventBus
            # so downstream engines (storage, feature, setup) never see incomplete data.
            self._latest_partial_bars[event.symbol] = event
            return
        # Only M1+ bars update the snapshot state used by the dashboard and REST API.
        # Sub-minute bars (S1) are published to the EventBus for the push WS only.
        _SNAPSHOT_TIMEFRAMES = {BarTimeframe.M1, BarTimeframe.M5, BarTimeframe.H1, BarTimeframe.D1}
        if event.timeframe in _SNAPSHOT_TIMEFRAMES:
            self._latest_bars[event.symbol] = event
        await self._event_bus.publish(event)

    async def _on_trade(self, event: TradeEvent) -> None:
        self._trades_received += 1
        await self._event_bus.publish(event)
        self._update_partial_bar(event)

    def _update_partial_bar(self, event: TradeEvent) -> None:
        """
        Accumulate trade ticks into a partial 1m bar stored in _latest_partial_bars.

        This provides a live forming candle for data sources (like Databento ohlcv-1m)
        that only deliver completed bars. The partial bar is used by the dashboard
        to show the current minute's OHLC in real-time without going through the EventBus.
        """
        from alpha.models.events import EventMetadata
        minute_ts = event.timestamp.replace(second=0, microsecond=0)
        sym = event.symbol
        acc = self._partial_accum.get(sym)

        if acc is None or acc.minute_ts != minute_ts:
            acc = _PartialAccum(
                minute_ts=minute_ts,
                open=event.price,
                high=event.price,
                low=event.price,
                close=event.price,
                volume=event.size,
            )
        else:
            acc.high = max(acc.high, event.price)
            acc.low = min(acc.low, event.price)
            acc.close = event.price
            acc.volume += event.size

        self._partial_accum[sym] = acc
        self._latest_partial_bars[sym] = BarEvent(
            symbol=sym,
            timestamp=minute_ts,
            timeframe=BarTimeframe.M1,
            open=acc.open,
            high=acc.high,
            low=acc.low,
            close=acc.close,
            volume=acc.volume,
            is_partial=True,
            metadata=EventMetadata(
                source=event.metadata.source,
                received_at=event.metadata.received_at,
                is_replay=False,
            ),
        )

    async def _on_quote(self, event: QuoteEvent) -> None:
        self._quotes_received += 1
        self._latest_quotes[event.symbol] = event
        # Only publish to the EventBus when bid or ask price changes.
        # mbp-1 also fires on size-only changes (hundreds/sec for MNQ) which
        # would overflow subscriber queues. Downstream engines only care about price.
        sym = event.symbol
        if (
            event.bid_price != self._last_published_bid.get(sym)
            or event.ask_price != self._last_published_ask.get(sym)
        ):
            self._last_published_bid[sym] = event.bid_price
            self._last_published_ask[sym] = event.ask_price
            await self._event_bus.publish(event)

    async def _on_order_book(self, event: OrderBookEvent) -> None:
        await self._event_bus.publish(event)
