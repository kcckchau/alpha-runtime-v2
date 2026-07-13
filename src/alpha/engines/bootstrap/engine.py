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
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.loader import get_settings
from alpha.config.settings import AlphaSettings
from alpha.core.clock import Clock, ReplayClock, WallClock
from alpha.core.engine import BaseEngine
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.instruments import resolve_symbol
from alpha.models.enums import EventType, RuntimeMode, SetupState
from alpha.models.events import SetupEvent
from alpha.timeframe_context import build_symbol_context

from alpha.engines.bootstrap.catchup import CatchupService
from alpha.engines.bootstrap.snapshot import SnapshotMixin, _write_snapshot_sync
from alpha.engines.bootstrap.wiring import wire_all

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
    from alpha.engines.thesis.engine import ThesisEngine

logger = logging.getLogger(__name__)


class BootstrapEngine(BaseEngine, SnapshotMixin):
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
        self._thesis: ThesisEngine | None = None
        self._scoring: ScoringEngine | None = None
        self._risk: RiskEngine | None = None
        self._order: OrderEngine | None = None

        self._engines: list[BaseEngine] = []
        self._status_task: asyncio.Task[None] | None = None
        self._startup_context: dict[str, dict[str, Any]] = {}
        self._ibkr_conn: object | None = None  # IBKRConnection, kept for clean shutdown

        # Flow pipeline (not BaseEngines — no lifecycle management needed)
        self._flow_aggregators: list = []   # one BarFlowAggregator per symbol
        self._pipeline: object | None = None  # BarPipeline

        # Ingestion quality monitor — shared between LiveIngestionEngine (writes)
        # and PositionMonitor (kill switch reads)
        self._ingestion_monitor: object | None = None  # IngestionMonitor

        # Notifications
        self._telegram_notifier: object | None = None

        # Terminal setup cache: symbol → {setup_id_str → (setup_dict, bar_count_at_terminal)}
        # Keeps FAILED/INVALIDATED/EXPIRED setups visible in status.json for _TERMINAL_TTL_BARS bars.
        self._terminal_setups: dict[str, dict[str, tuple[dict, int]]] = {}
        self._terminal_setup_bar_count: int = 0  # incremented on each M1 bar
        _TERMINAL_TTL_BARS = 10
        self._terminal_ttl = _TERMINAL_TTL_BARS

        # Last PipelineOutput per symbol — authoritative snapshot for status.json
        self._last_pipeline_output: dict[str, object] = {}  # symbol → PipelineOutputEvent

        # CatchupService — created in _on_initialize
        self._catchup: CatchupService | None = None

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

    @property
    def thesis_engine(self) -> "ThesisEngine | None":
        return self._thesis

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        logger.info(
            "Bootstrapping runtime | mode=%s | symbols=%s",
            self._settings.runtime.mode,
            self._settings.runtime.symbols,
        )
        self._cleanup_stale_tmp_files()
        self._configure_clock()
        self._populate_registry()
        await self._event_bus.start()
        wire_all(self)

        for engine in self._engines:
            await engine.initialize()

        if self._storage is not None and self._historical is not None:
            self._catchup = CatchupService(
                settings=self._settings,
                storage=self._storage,
                historical=self._historical,
                event_bus=self._event_bus,
                registry=self._registry,
            )

        self._write_runtime_snapshot()

    async def _on_start(self) -> None:
        mode = self._settings.runtime.mode
        for engine in self._engines:
            if mode in {RuntimeMode.LIVE, RuntimeMode.PAPER} and engine is self._live:
                continue
            await engine.start()

        self._write_runtime_snapshot()
        self._event_bus.subscribe(EventType.SETUP, self._on_setup_event)
        self._event_bus.subscribe(EventType.PIPELINE_OUTPUT, self._on_pipeline_output)
        self._status_task = asyncio.create_task(self._status_loop())
        if self._telegram_notifier is not None:
            from alpha.notifications.telegram import TelegramNotifier
            if isinstance(self._telegram_notifier, TelegramNotifier):
                await self._telegram_notifier.start()

        if mode in {RuntimeMode.LIVE, RuntimeMode.PAPER}:
            await self._start_live()
        elif mode == RuntimeMode.REPLAY:
            await self._run_replay()
        self._write_runtime_snapshot()

    async def _on_stop(self) -> None:
        if self._status_task and not self._status_task.done():
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
        if self._telegram_notifier is not None:
            from alpha.notifications.telegram import TelegramNotifier
            if isinstance(self._telegram_notifier, TelegramNotifier):
                await self._telegram_notifier.stop()
        for engine in reversed(self._engines):
            await engine.stop()
        # Disconnect IBKR after engines stop (subscriptions are already cancelled
        # by LiveIngestionEngine._on_stop). Doing this while the event loop is
        # still running prevents IB.__del__ from firing on a closed loop.
        if self._ibkr_conn is not None:
            from alpha.engines.ibkr.connection import IBKRConnection
            if isinstance(self._ibkr_conn, IBKRConnection):
                await self._ibkr_conn.disconnect()
        await self._event_bus.stop()
        self._write_runtime_snapshot()

    # ── Private ───────────────────────────────────────────────────────────────

    async def _start_live(self) -> None:
        """LIVE/PAPER startup sequence.

        1. CatchupService fetches all warmup bars (M1 past + today, H1, D1)
           and emits them in dependency order (D1 → H1 → M5 → M1) so every
           engine has full context before live data arrives.

        2. Live feed starts from (m1_end - 1m): the Databento gateway replays
           the small gap between historical availability and real-time, then
           continues live. No buffer mode needed — history is fully warmed
           before the first live bar can arrive.

        3. Reconcile active setups so any setup that survived warmup replay
           is persisted as a live event exactly once.
        """
        assert self._catchup is not None
        symbols = list(self._settings.runtime.symbols)

        # ── Warm up all timeframes ────────────────────────────────────────────
        logger.info("Bootstrap: warming up %s", symbols)
        context_map, m1_end = await self._catchup.run(symbols)

        for symbol, ctx in context_map.items():
            self._startup_context[symbol] = build_symbol_context(
                symbol=symbol,
                minute_bars=ctx["minute_bars"],
                hourly_bars=ctx["hourly_bars"],
                daily_bars=ctx["daily_bars"],
                calendar=calendar_for_symbol(self._registry.get(symbol)),
            )

        if self._storage is not None:
            await self._storage.flush()

        # ── Start live feed from historical edge ──────────────────────────────
        # Use m1_end - 1m as a 1-bar overlap buffer against boundary precision.
        if self._live is not None:
            replay_start = m1_end - timedelta(minutes=1)
            self._live.set_replay_start(replay_start)
            await self._live.start()
            self._write_runtime_snapshot()
            if self._feature is not None:
                await self._live.subscribe_tick_trades(self._feature.record_trade)
            logger.info("Live feed started | replay_from=%s", replay_start.isoformat())

        await self._reconcile_active_setups()

        # Execution subsystem: mark market data fresh after catchup has emitted
        # bars through the pipeline and the MarketStateProjector has received at
        # least one MarketStateEvent per symbol.
        if hasattr(self, "_execution_coordinator") and self._execution_coordinator is not None:
            self._execution_coordinator.update_readiness(market_data_fresh=True)
            logger.info("Execution subsystem READY")

        logger.info("Bootstrap READY")

    def _cleanup_stale_tmp_files(self) -> None:
        """Remove temp files left behind by previous crashed runs."""
        from alpha.runtime_status import snapshot_path
        runtime_dir = snapshot_path(self._settings).parent
        removed = 0
        for p in runtime_dir.glob("tmp*"):
            try:
                p.unlink()
                removed += 1
            except OSError:
                pass
        if removed:
            logger.info("Cleaned up %d stale temp file(s) from previous run", removed)

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

    async def _reconcile_active_setups(self) -> None:
        """
        Publish active setups as non-replay events at the catchup→live transition.

        During catchup, SetupEvents are emitted with is_replay=True and filtered
        out by StorageEngine. At the transition point, we snapshot whatever setups
        survived to the present and publish them as authoritative live events so
        they land in Parquet exactly once.

        Because setup_id is now deterministic (uuid5 of symbol+type+bar_ts),
        restarting and reconciling again produces the same IDs — the upsert in
        StorageEngine/ParquetStore overwrites rather than duplicates.
        """
        if self._setup is None:
            return

        from alpha.models.enums import DataSourceId
        from alpha.models.events import EventMetadata

        now = datetime.now(timezone.utc)
        total = 0
        for symbol in self._settings.runtime.symbols:
            for setup in self._setup.active_setups(symbol):
                event = SetupEvent(
                    symbol=symbol,
                    timestamp=setup.detected_at,
                    setup_id=setup.setup_id,
                    setup_type=setup.setup_type,
                    setup_state=setup.state,
                    prev_state=None,
                    metadata=EventMetadata(
                        source=DataSourceId.UNKNOWN,
                        received_at=now,
                        is_replay=False,
                    ),
                )
                await self._event_bus.publish(event)
                total += 1

        if total:
            logger.info("Reconciled %d active setup(s) to storage at catchup→live transition", total)

    async def _status_loop(self) -> None:
        loop = asyncio.get_event_loop()
        ticks = 0
        while True:
            # Build the snapshot on the event loop (pure Python, fast), then
            # offload the JSON serialisation + file write to a thread so the
            # event loop stays free to handle HTTP / WebSocket requests.
            payload = self._build_runtime_snapshot()
            settings = self._settings
            loop.run_in_executor(None, _write_snapshot_sync, settings, payload)
            if ticks % 30 == 0:
                self._log_runtime_summary()
            ticks += 1
            await asyncio.sleep(1)

    async def _run_replay(self) -> None:
        """Replay historical data through the full pipeline."""
        logger.info("Running replay")
        # TODO: drive HistoricalDataEngine with ReplayClock pacing
