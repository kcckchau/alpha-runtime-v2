"""
Engine 0 — Bootstrap Engine

Responsibilities:
  - Load configuration and validate runtime mode
  - Populate the symbol registry
  - Wire dependency graph (engines, adapters, event bus)
  - Select and initialize the session calendar
  - Start all engines in dependency order
  - Implement catch-up-then-live transition for LIVE mode

Startup sequence (LIVE / PAPER):
  1. initialize()  → load config, build engines
  2. start()       → historical catch-up, then hand off to live feed
  3. Running       → all engines processing live events

Startup sequence (HISTORICAL_BACKFILL):
  1. initialize()
  2. start()       → replay historical range, write to storage
  3. stop()        → clean shutdown

Startup sequence (REPLAY):
  1. initialize()
  2. start()       → feed historical bars through pipeline at replay speed
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from alpha.calendar.base import SessionCalendar
from alpha.calendar.nyse import NYSECalendar
from alpha.config.loader import get_settings
from alpha.config.settings import AlphaSettings
from alpha.core.clock import Clock, ReplayClock, WallClock
from alpha.core.engine import BaseEngine
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.instruments import resolve_symbol
from alpha.models.enums import BarTimeframe, EngineState, RuntimeMode
from alpha.models.symbol import Symbol
from alpha.runtime_status import write_snapshot
from alpha.timeframe_context import build_symbol_context

if TYPE_CHECKING:
    from alpha.engines.feature.engine import FeatureEngine
    from alpha.engines.historical.engine import HistoricalDataEngine
    from alpha.engines.live.engine import LiveIngestionEngine
    from alpha.engines.market_state.engine import MarketStateEngine
    from alpha.engines.order.engine import OrderEngine
    from alpha.engines.risk.engine import RiskEngine
    from alpha.engines.scoring.engine import ScoringEngine
    from alpha.engines.setup.engine import SetupEngine
    from alpha.engines.storage.engine import StorageEngine

logger = logging.getLogger(__name__)


class BootstrapEngine(BaseEngine):
    """
    Orchestrates the full runtime lifecycle.

    All engines are owned and managed by this class. External code
    interacts with the runtime through the BootstrapEngine and the
    shared EventBus.
    """

    def __init__(self, settings: AlphaSettings | None = None) -> None:
        super().__init__()
        self._settings = settings or get_settings()
        self._event_bus = EventBus()
        self._registry = SymbolRegistry()
        self._clock: Clock = WallClock()
        self._calendar: SessionCalendar = NYSECalendar()

        # Engine references — populated in _on_initialize
        self._storage: StorageEngine | None = None
        self._historical: HistoricalDataEngine | None = None
        self._live: LiveIngestionEngine | None = None
        self._feature: FeatureEngine | None = None
        self._market_state: MarketStateEngine | None = None
        self._setup: SetupEngine | None = None
        self._scoring: ScoringEngine | None = None
        self._risk: RiskEngine | None = None
        self._order: OrderEngine | None = None

        self._engines: list[BaseEngine] = []
        self._status_task: asyncio.Task[None] | None = None
        self._startup_context: dict[str, dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return "BootstrapEngine"

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def registry(self) -> SymbolRegistry().__class__:  # type: ignore[valid-type]
        return self._registry

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def calendar(self) -> SessionCalendar:
        return self._calendar

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        logger.info(
            "Bootstrapping runtime | mode=%s | symbols=%s",
            self._settings.runtime.mode,
            self._settings.runtime.symbols,
        )

        self._configure_clock()
        self._populate_registry()
        await self._event_bus.start()
        self._wire_engines()

        for engine in self._engines:
            await engine.initialize()
        self._write_runtime_snapshot()

    async def _on_start(self) -> None:
        mode = self._settings.runtime.mode
        for engine in self._engines:
            if mode in {RuntimeMode.LIVE, RuntimeMode.PAPER} and engine is self._live:
                continue
            await engine.start()

        if mode in {RuntimeMode.LIVE, RuntimeMode.PAPER}:
            await self._run_catchup()
            logger.info("Catch-up complete — transitioning to live feed")
            if self._live is not None:
                await self._live.start()
        elif mode == RuntimeMode.HISTORICAL_BACKFILL:
            await self._run_backfill()
        elif mode == RuntimeMode.REPLAY:
            await self._run_replay()
        self._write_runtime_snapshot()
        self._status_task = asyncio.create_task(self._status_loop())

    async def _on_stop(self) -> None:
        if self._status_task and not self._status_task.done():
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
        for engine in reversed(self._engines):
            await engine.stop()
        await self._event_bus.stop()
        self._write_runtime_snapshot()

    # ── Private ───────────────────────────────────────────────────────────────

    def _configure_clock(self) -> None:
        mode = self._settings.runtime.mode
        if mode == RuntimeMode.REPLAY:
            replay_cfg = self._settings.replay
            start = datetime(
                replay_cfg.start_date.year,
                replay_cfg.start_date.month,
                replay_cfg.start_date.day,
                9, 30,
                tzinfo=timezone.utc,
            ) if replay_cfg.start_date else datetime.now(timezone.utc)
            self._clock = ReplayClock(start_time=start, speed=replay_cfg.speed)
            logger.info("Using ReplayClock speed=%.1f", replay_cfg.speed)
        else:
            self._clock = WallClock()

    def _populate_registry(self) -> None:
        for ticker in self._settings.runtime.symbols:
            sym = resolve_symbol(ticker)
            self._registry.register(sym)
        logger.info("Registry loaded: %d symbols", len(self._registry))

    def _wire_engines(self) -> None:
        """Instantiate all engines and inject shared dependencies."""
        from alpha.engines.feature.engine import FeatureEngine
        from alpha.engines.historical.engine import HistoricalDataEngine
        from alpha.engines.live.engine import LiveIngestionEngine
        from alpha.engines.market_state.engine import MarketStateEngine
        from alpha.engines.order.engine import OrderEngine
        from alpha.engines.risk.engine import RiskEngine
        from alpha.engines.scoring.engine import ScoringEngine
        from alpha.engines.setup.engine import SetupEngine
        from alpha.engines.storage.engine import StorageEngine

        self._storage = StorageEngine(self._settings, self._event_bus)
        self._historical = HistoricalDataEngine(
            self._settings, self._event_bus, self._registry, self._calendar
        )
        self._live = LiveIngestionEngine(
            self._settings, self._event_bus, self._registry
        )
        self._feature = FeatureEngine(
            self._settings, self._event_bus, self._registry, self._calendar, self._clock
        )
        self._market_state = MarketStateEngine(self._settings, self._event_bus, self._registry)
        self._market_state.set_feature_engine(self._feature)
        self._setup = SetupEngine(self._settings, self._event_bus, self._registry)
        self._setup.set_feature_engine(self._feature)
        self._setup.set_market_state_engine(self._market_state)
        self._scoring = ScoringEngine(self._settings, self._event_bus)
        self._scoring.set_setup_engine(self._setup)
        self._risk = RiskEngine(self._settings, self._event_bus)
        self._risk.set_setup_engine(self._setup)
        self._order = OrderEngine(self._settings, self._event_bus, self._registry)
        self._risk.set_order_engine(self._order)

        self._wire_ibkr()

        self._engines = [
            self._storage,
            self._historical,
            self._live,
            self._feature,
            self._market_state,
            self._setup,
            self._scoring,
            self._risk,
            self._order,
        ]

    def _wire_ibkr(self) -> None:
        """Register IBKR adapters if IBKR is configured as the data/order source."""
        from alpha.models.enums import DataSourceId

        hist_src = self._settings.historical.primary_source
        live_src = self._settings.live.primary_source

        if DataSourceId.INTERACTIVE_BROKERS not in {hist_src, live_src}:
            return

        from alpha.engines.ibkr.connection import IBKRConnection
        from alpha.engines.historical.sources.ibkr import IBKRHistoricalDataSource
        from alpha.engines.live.adapters.ibkr import IBKRLiveFeedAdapter
        from alpha.engines.order.adapters.ibkr import IBKRBrokerAdapter

        conn = IBKRConnection(self._settings.ibkr)

        if hist_src == DataSourceId.INTERACTIVE_BROKERS:
            self._historical.register_source(
                IBKRHistoricalDataSource(conn, self._registry, self._settings.ibkr)
            )

        if live_src == DataSourceId.INTERACTIVE_BROKERS:
            self._live.register_adapter(
                IBKRLiveFeedAdapter(conn, self._settings.ibkr, self._registry.active())
            )

        self._order.register_adapter(
            IBKRBrokerAdapter(conn, self._settings.ibkr, self._registry)
        )
        logger.info("IBKR adapters registered (paper=%s)", self._settings.ibkr.is_paper)

    async def _run_catchup(self) -> None:
        """Load recent history before connecting the live feed."""
        logger.info(
            "Running catch-up: last %d days",
            self._settings.runtime.catchup_lookback_days,
        )
        if self._historical is None or self._storage is None:
            return

        end = datetime.now(timezone.utc)
        minute_start = end - timedelta(days=self._settings.runtime.catchup_lookback_days)
        hourly_start = end - timedelta(
            days=max(90, self._settings.historical.hourly_warmup_bars // 3)
        )
        daily_start = end - timedelta(
            days=max(
                self._settings.historical.daily_warmup_bars * 2,
                self._settings.historical.monthly_warmup_months * 32,
            )
        )

        for symbol in self._settings.runtime.symbols:
            minute_bars = await self._historical.fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.M1,
                start=minute_start,
                end=end,
                emit=True,
            )
            hourly_bars = await self._historical.fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.H1,
                start=hourly_start,
                end=end,
                emit=False,
            )
            daily_bars = await self._historical.fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.D1,
                start=daily_start,
                end=end,
                emit=False,
            )

            for bar in hourly_bars + daily_bars:
                await self._storage.save_bar(bar)

            self._startup_context[symbol] = build_symbol_context(
                symbol=symbol,
                minute_bars=minute_bars,
                hourly_bars=hourly_bars,
                daily_bars=daily_bars,
                calendar=self._calendar,
            )

    async def _status_loop(self) -> None:
        ticks = 0
        while True:
            self._write_runtime_snapshot()
            if ticks % 6 == 0:
                self._log_runtime_summary()
            ticks += 1
            await asyncio.sleep(5)

    def _write_runtime_snapshot(self) -> None:
        try:
            write_snapshot(self._settings, self._build_runtime_snapshot())
        except Exception:
            logger.exception("Failed to write runtime snapshot")

    def _build_runtime_snapshot(self) -> dict[str, Any]:
        return {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "runtime_state": self.state,
            "mode": self._settings.runtime.mode,
            "symbols": self._settings.runtime.symbols,
            "engines": [self._serialize_engine(engine) for engine in self._engines],
            "quotes": self._serialize_quotes(),
            "bars": self._serialize_bars(),
            "contexts": self._startup_context,
            "market_states": self._serialize_market_states(),
            "setups": self._serialize_setups(),
            "orders": self._serialize_orders(),
        }

    def _serialize_engine(self, engine: BaseEngine) -> dict[str, Any]:
        details = self._engine_details(engine)
        if engine.state == EngineState.RUNNING:
            health_status = "healthy"
        elif engine.state == EngineState.ERROR:
            health_status = "unhealthy"
        else:
            health_status = "degraded"
        return {
            "name": engine.name,
            "state": str(engine.state),
            "health": health_status,
            "details": details,
        }

    def _engine_details(self, engine: BaseEngine) -> dict[str, Any]:
        if engine is self._storage:
            return {
                "queue_depth": self._storage._write_queue.qsize(),  # type: ignore[union-attr]
                "writes_total": self._storage._writes_total,  # type: ignore[union-attr]
            }
        if engine is self._historical:
            return {}
        if engine is self._live:
            return {
                "bars_received": self._live._bars_received,  # type: ignore[union-attr]
                "quotes_received": self._live._quotes_received,  # type: ignore[union-attr]
                "trades_received": self._live._trades_received,  # type: ignore[union-attr]
            }
        if engine is self._feature:
            return {
                "snapshots_emitted": self._feature._snapshots_emitted,  # type: ignore[union-attr]
            }
        if engine is self._market_state:
            return {
                "classifications_total": self._market_state._classifications_total,  # type: ignore[union-attr]
            }
        if engine is self._setup:
            return {
                "active_setups": sum(len(v) for v in self._setup._active.values()),  # type: ignore[union-attr]
                "detected_total": self._setup._setups_detected,  # type: ignore[union-attr]
                "triggered_total": self._setup._setups_triggered,  # type: ignore[union-attr]
            }
        if engine is self._scoring:
            return {}
        if engine is self._risk:
            return {}
        if engine is self._order:
            return {
                "orders_submitted": self._order._orders_submitted,  # type: ignore[union-attr]
                "orders_filled": self._order._orders_filled,  # type: ignore[union-attr]
                "open_orders": len(self._order.get_open_orders()),  # type: ignore[union-attr]
            }
        return {}

    def _serialize_quotes(self) -> dict[str, Any]:
        if self._live is None:
            return {}
        quotes = {}
        for symbol, event in self._live.latest_quotes().items():
            quotes[symbol] = event.model_dump(mode="json")
        return quotes

    def _serialize_bars(self) -> dict[str, Any]:
        if self._live is None:
            return {}
        bars = {}
        for symbol, event in self._live.latest_bars().items():
            bars[symbol] = event.model_dump(mode="json")
        return bars

    def _serialize_market_states(self) -> dict[str, Any]:
        if self._market_state is None:
            return {}
        states = {}
        for symbol in self._settings.runtime.symbols:
            state = self._market_state.get_state(symbol)
            if state is not None:
                states[symbol] = state.model_dump(mode="json")
        return states

    def _serialize_setups(self) -> list[dict[str, Any]]:
        if self._setup is None:
            return []
        setups: list[dict[str, Any]] = []
        for symbol in self._settings.runtime.symbols:
            for setup in self._setup.active_setups(symbol):
                setups.append(setup.model_dump(mode="json"))
        return setups

    def _serialize_orders(self) -> list[dict[str, Any]]:
        if self._order is None:
            return []
        return [order.model_dump(mode="json") for order in self._order.get_open_orders()]

    def _log_runtime_summary(self) -> None:
        if self._live is None:
            return
        quote_chunks: list[str] = []
        for symbol in self._settings.runtime.symbols:
            quote = self._live.latest_quotes().get(symbol)
            bar = self._live.latest_bars().get(symbol)
            if quote is None and bar is None:
                continue
            close = f"{bar.close}" if bar is not None else "-"
            bid = f"{quote.bid_price}" if quote is not None else "-"
            ask = f"{quote.ask_price}" if quote is not None else "-"
            quote_chunks.append(f"{symbol} c={close} bid={bid} ask={ask}")
        if quote_chunks:
            logger.info("Runtime summary | %s", " | ".join(quote_chunks))

    async def _run_backfill(self) -> None:
        """Full historical backfill without going live."""
        logger.info("Running full historical backfill")
        # TODO: drive HistoricalDataEngine across configured date range

    async def _run_replay(self) -> None:
        """Replay historical data through the full pipeline."""
        logger.info("Running replay")
        # TODO: drive HistoricalDataEngine with ReplayClock pacing
