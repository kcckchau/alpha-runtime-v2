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

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.live.adapters.base import LiveFeedAdapter, TickHandlerT
from alpha.engines.live.monitor import IngestionMonitor
from alpha.models.enums import BarTimeframe, HealthStatus
from alpha.models.events import BarEvent, OrderBookEvent, QuoteEvent, TradeEvent

logger = logging.getLogger(__name__)

# Outlier-tick guard for the live partial bar. CME trade tapes (via Databento's
# `trades` schema) include off-market prints — block trades, spread-implied fills,
# late-reported negotiated trades — that can sit far from the current market. A
# single such tick folded into acc.high/acc.low would spike the live wick until
# the next completed exchange bar (which excludes these prints) corrects it,
# producing a visible flicker to an extreme value. We bound accepted ticks to a
# band around the current best bid/ask (or the last accepted price if no quote
# is available yet), sized adaptively off the live spread.
_OUTLIER_SPREAD_MULT = Decimal("30")
_OUTLIER_PRICE_PCT = Decimal("0.001")      # 0.1% of mid when a quote is available
_OUTLIER_FALLBACK_PCT = Decimal("0.002")   # 0.2% of last price when no quote yet


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
        ingestion_monitor: IngestionMonitor | None = None,
    ) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._registry = registry
        self._monitor = ingestion_monitor
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
        self._health_task: asyncio.Task[None] | None = None

    @property
    def name(self) -> str:
        return "LiveIngestionEngine"

    # ── Adapter registration ──────────────────────────────────────────────────

    def register_adapter(self, adapter: LiveFeedAdapter) -> None:
        self._adapters[adapter.source_id] = adapter
        if self._monitor is not None and hasattr(adapter, "set_skipped_records_handler"):
            adapter.set_skipped_records_handler(self._monitor.on_skipped_records)
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

        if self._monitor is not None:
            self._health_task = asyncio.create_task(self._health_check_loop())

        logger.info(
            "Live subscriptions active: %d symbols via %s",
            len(symbols), adapter.source_id,
        )

    async def _on_stop(self) -> None:
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        for adapter in self._adapters.values():
            try:
                await adapter.disconnect()
            except Exception:
                logger.exception("Error disconnecting adapter %s", adapter.source_id)

    async def _health_check(self) -> EngineHealth:
        connected = [
            sid for sid, a in self._adapters.items() if a.is_connected
        ]
        queue_depths = self._event_bus.queue_depths()
        any_backed_up = any(d["depth"] > d["maxsize"] * 0.5 for d in queue_depths)
        if any_backed_up:
            for d in queue_depths:
                if d["depth"] > d["maxsize"] * 0.5:
                    logger.warning(
                        "EventBus queue backed up: %s/%s depth=%d/%d"
                        " full_count=%d drop_count=%d",
                        d["event_type"], d["symbol"],
                        d["depth"], d["maxsize"],
                        d["full_count"], d["drop_count"],
                    )
        details = {
            "adapters_connected": connected,
            "bars_received": self._bars_received,
            "trades_received": self._trades_received,
            "quotes_received": self._quotes_received,
            "queue_depths": queue_depths,
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
        # Completed 1m bar supersedes any trade-accumulated partial for that minute.
        if event.timeframe == BarTimeframe.M1:
            sym = event.symbol
            self._partial_accum.pop(sym, None)
            stale_partial = self._latest_partial_bars.get(sym)
            if stale_partial is not None and stale_partial.timestamp <= event.timestamp:
                del self._latest_partial_bars[sym]
            if self._monitor is not None:
                self._monitor.on_bar_received(sym, event.timestamp, datetime.now(timezone.utc))
        # Only M1+ bars update the snapshot state used by the dashboard and REST API.
        # Sub-minute bars (S1) are published to the EventBus for the push WS only.
        _SNAPSHOT_TIMEFRAMES = {BarTimeframe.M1, BarTimeframe.M5, BarTimeframe.H1, BarTimeframe.D1}
        if event.timeframe in _SNAPSHOT_TIMEFRAMES:
            self._latest_bars[event.symbol] = event
        await self._event_bus.publish(event)

    async def _on_trade(self, event: TradeEvent) -> None:
        self._trades_received += 1
        if self._monitor is not None:
            self._monitor.on_record_received(event.symbol)
        await self._event_bus.publish(event)
        self._update_partial_bar(event)

    def _is_outlier_trade(self, sym: str, price: Decimal, reference: Decimal | None) -> bool:
        """Return True if `price` is implausibly far from the current market.

        Prefers an anchor derived from the live bid/ask (adaptive to current
        spread/volatility); falls back to the last accepted trade price when no
        quote has arrived yet. Returns False (accept) if neither is available —
        we never want to silently drop every tick for a symbol we know nothing about.
        """
        quote = self._latest_quotes.get(sym)
        if quote is not None and quote.bid_price is not None and quote.ask_price is not None:
            anchor = (quote.bid_price + quote.ask_price) / 2
            spread = quote.ask_price - quote.bid_price
            band = max(anchor * _OUTLIER_PRICE_PCT, spread * _OUTLIER_SPREAD_MULT)
        elif reference is not None:
            anchor = reference
            band = anchor * _OUTLIER_FALLBACK_PCT
        else:
            return False
        return band > 0 and abs(price - anchor) > band

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
        reference_close = acc.close if acc is not None else None

        if self._is_outlier_trade(sym, event.price, reference_close):
            logger.debug(
                "LiveIngestionEngine: rejecting outlier trade tick for %s partial bar (price=%s)",
                sym, event.price,
            )
            return

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
        if self._monitor is not None:
            self._monitor.on_record_received(event.symbol)
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

    async def _health_check_loop(self) -> None:
        """Runs every second to detect RTH silence. Stopped in _on_stop."""
        while True:
            if self._monitor is not None:
                self._monitor.check_health()
            await asyncio.sleep(1.0)
