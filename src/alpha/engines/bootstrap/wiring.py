"""
Wiring functions for BootstrapEngine.

All functions take ``engine: BootstrapEngine`` as first parameter and mutate it
in-place. Using TYPE_CHECKING to avoid circular imports at runtime.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from alpha.engines.bootstrap.engine import BootstrapEngine

logger = logging.getLogger(__name__)


def wire_all(engine: "BootstrapEngine") -> None:
    """Instantiate all engines and inject shared dependencies."""
    from alpha.engines.context.engine import ContextEngine
    from alpha.engines.feature.engine import FeatureEngine
    from alpha.engines.historical.engine import HistoricalDataEngine
    from alpha.engines.live.engine import LiveIngestionEngine
    from alpha.engines.live.monitor import IngestionMonitor
    from alpha.engines.market_state.engine import MarketStateEngine
    from alpha.engines.order.engine import OrderEngine
    from alpha.engines.risk.engine import RiskEngine
    from alpha.engines.scoring.engine import ScoringEngine
    from alpha.engines.setup.engine import SetupEngine
    from alpha.engines.storage.engine import StorageEngine
    from alpha.engines.thesis.engine import ThesisEngine

    engine._ingestion_monitor = IngestionMonitor(
        symbols=list(engine._settings.runtime.symbols)
    )

    engine._storage = StorageEngine(engine._settings, engine._event_bus)
    engine._historical = HistoricalDataEngine(
        engine._settings, engine._event_bus, engine._registry, engine._calendar
    )
    engine._live = LiveIngestionEngine(
        engine._settings, engine._event_bus, engine._registry,
        ingestion_monitor=engine._ingestion_monitor,
    )
    engine._feature = FeatureEngine(
        engine._settings, engine._event_bus, engine._registry, engine._calendar, engine._clock
    )
    engine._context = ContextEngine(
        engine._settings, engine._event_bus, engine._registry, engine._calendar, engine._clock
    )
    engine._context.set_feature_engine(engine._feature)
    engine._market_state = MarketStateEngine(engine._settings, engine._event_bus, engine._registry)
    engine._market_state.set_feature_engine(engine._feature)
    engine._market_state.set_context_engine(engine._context)
    engine._setup = SetupEngine(engine._settings, engine._event_bus, engine._registry)
    engine._setup.set_feature_engine(engine._feature)
    engine._setup.set_market_state_engine(engine._market_state)
    engine._setup.set_context_engine(engine._context)
    engine._thesis = ThesisEngine(engine._settings, engine._event_bus, engine._registry)
    engine._thesis.set_feature_engine(engine._feature)
    engine._thesis.set_context_engine(engine._context)
    engine._scoring = ScoringEngine(engine._settings, engine._event_bus)
    engine._scoring.set_setup_engine(engine._setup)
    engine._scoring.set_feature_engine(engine._feature)
    engine._risk = RiskEngine(engine._settings, engine._event_bus)
    engine._risk.set_setup_engine(engine._setup)
    engine._order = OrderEngine(engine._settings, engine._event_bus, engine._registry)
    engine._risk.set_order_engine(engine._order)

    wire_ibkr(engine)
    wire_databento(engine)
    wire_pipeline(engine)
    engine._position_monitor = wire_position_monitor(engine)
    wire_notifications(engine)
    wire_execution(engine)
    wire_level_observer(engine)
    wire_interaction_engine(engine)

    engine._engines = [
        engine._storage,
        engine._historical,
        engine._live,
        engine._feature,
        engine._context,
        engine._market_state,
        engine._setup,
        engine._thesis,
        engine._scoring,
        engine._risk,
        engine._order,
        engine._position_monitor,
    ]


def wire_ibkr(engine: "BootstrapEngine") -> None:
    """Register IBKR adapters if IBKR is configured as the data/order source."""
    from alpha.models.enums import DataSourceId

    hist_src = engine._settings.historical.primary_source
    live_src = engine._settings.live.primary_source

    if DataSourceId.INTERACTIVE_BROKERS not in {hist_src, live_src}:
        return

    from alpha.engines.ibkr.connection import IBKRConnection
    from alpha.engines.historical.sources.ibkr import IBKRHistoricalDataSource
    from alpha.engines.live.adapters.ibkr import IBKRLiveFeedAdapter
    from alpha.engines.order.adapters.ibkr import IBKRBrokerAdapter

    conn = IBKRConnection(engine._settings.ibkr)
    engine._ibkr_conn = conn

    if hist_src == DataSourceId.INTERACTIVE_BROKERS:
        engine._historical.register_source(
            IBKRHistoricalDataSource(conn, engine._registry, engine._settings.ibkr)
        )

    if live_src == DataSourceId.INTERACTIVE_BROKERS:
        engine._live.register_adapter(
            IBKRLiveFeedAdapter(conn, engine._settings.ibkr, engine._registry.active())
        )

    ibkr_adapter = IBKRBrokerAdapter(conn, engine._settings.ibkr, engine._registry)
    engine._order.register_adapter(ibkr_adapter)
    # Give the risk engine direct access to the broker adapter for P&L polling
    if engine._risk is not None:
        engine._risk.set_broker_adapter(ibkr_adapter)
    logger.info("IBKR adapters registered (paper=%s)", engine._settings.ibkr.is_paper)


def wire_databento(engine: "BootstrapEngine") -> None:
    """Register Databento adapters if Databento is configured as the data source."""
    from alpha.models.enums import DataSourceId

    hist_src = engine._settings.historical.primary_source
    live_src = engine._settings.live.primary_source

    if DataSourceId.DATABENTO not in {hist_src, live_src}:
        return

    from alpha.engines.historical.sources.databento import DatabentoHistoricalDataSource
    from alpha.engines.live.adapters.databento import DatabentoLiveFeedAdapter

    if hist_src == DataSourceId.DATABENTO:
        engine._historical.register_source(
            DatabentoHistoricalDataSource(engine._registry, engine._settings.databento)
        )

    if live_src == DataSourceId.DATABENTO:
        engine._live.register_adapter(
            DatabentoLiveFeedAdapter(engine._settings.databento, engine._registry)
        )

    logger.info("Databento adapters registered (dataset=%s)", engine._settings.databento.dataset)


def wire_pipeline(engine: "BootstrapEngine") -> None:
    """
    Wire BarFlowAggregator (one per symbol) and BarPipeline into the EventBus.

    BarFlowAggregator subscribes to TRADE + QUOTE + BAR and emits BAR_BUNDLE.
    BarPipeline subscribes to BAR_BUNDLE and calls engines in explicit order.
    """
    from alpha.engines.flow.aggregator import BarFlowAggregator
    from alpha.engines.flow.pipeline import BarPipeline

    symbols = engine._settings.runtime.symbols
    large_trade_threshold = getattr(
        engine._settings.runtime, "large_trade_threshold", 10
    )

    for ticker in symbols:
        agg = BarFlowAggregator(
            symbol=ticker,
            event_bus=engine._event_bus,
            large_trade_threshold=large_trade_threshold,
        )
        agg.attach()
        engine._flow_aggregators.append(agg)

    pipeline = BarPipeline(engine._event_bus)
    if engine._feature is not None:
        pipeline.set_feature_engine(engine._feature)
    if engine._market_state is not None:
        pipeline.set_market_state_engine(engine._market_state)
    if engine._thesis is not None:
        pipeline.set_thesis_engine(engine._thesis)
    if engine._setup is not None:
        pipeline.set_setup_engine(engine._setup)
    if engine._scoring is not None:
        pipeline.set_scoring_engine(engine._scoring)
    pipeline.attach()
    engine._pipeline = pipeline

    logger.info(
        "BarPipeline wired | symbols=%s | aggregators=%d",
        symbols, len(engine._flow_aggregators),
    )


def wire_position_monitor(engine: "BootstrapEngine") -> Any:
    """Wire IntrabarFlowEngine (one per symbol) and PositionMonitor. Returns the monitor."""
    from alpha.engines.flow.intrabar import IntrabarFlowEngine
    from alpha.engines.position.engine import PositionMonitor

    symbols = engine._settings.runtime.symbols
    large_trade_threshold = getattr(engine._settings.runtime, "large_trade_threshold", 10)

    intrabar_engines: dict[str, IntrabarFlowEngine] = {}
    for ticker in symbols:
        eng = IntrabarFlowEngine(
            symbol=ticker,
            event_bus=engine._event_bus,
            large_trade_threshold=large_trade_threshold,
        )
        eng.attach()
        intrabar_engines[ticker] = eng

    from alpha.engines.live.monitor import IngestionMonitor
    ingestion_monitor = (
        engine._ingestion_monitor
        if isinstance(engine._ingestion_monitor, IngestionMonitor)
        else None
    )

    monitor = PositionMonitor(
        event_bus=engine._event_bus,
        intrabar_engines=intrabar_engines,
        ingestion_monitor=ingestion_monitor,
    )
    logger.info("PositionMonitor wired | symbols=%s", symbols)
    return monitor


def wire_notifications(engine: "BootstrapEngine") -> None:
    """Create TelegramNotifier if configured."""
    from alpha.notifications.telegram import TelegramNotifier
    engine._telegram_notifier = TelegramNotifier(
        engine._settings.telegram, engine._event_bus
    )


def _clear_research_partitions_for_today(research_root: "Path", engine: "BootstrapEngine") -> None:
    """Clear today's research partitions for all registered symbols before wiring.

    Called once at startup so that each backend run writes a clean, single-run_id
    partition for the current session. Without this, every restart appends a new
    run_id file to the same session_date directory, producing duplicate rows.

    Only clears the current session date (determined per-symbol via the calendar).
    Historical dates (replayed via research_replay.py) are left untouched.
    """
    import shutil
    from datetime import datetime, timezone
    from alpha.calendar.resolver import calendar_for_symbol

    now_utc = datetime.now(tz=timezone.utc)

    tables = [
        research_root / "level_observations",
        research_root / "interaction" / "episodes",
        research_root / "interaction" / "episode_bars",
    ]

    cleared: list[str] = []
    for sym in engine._registry.all():
        try:
            cal = calendar_for_symbol(sym)
            session_date = cal.session_date(now_utc).isoformat()
        except Exception:
            session_date = now_utc.date().isoformat()  # fallback

        for table in tables:
            part_dir = table / sym.ticker / f"session_date={session_date}"
            if part_dir.exists():
                shutil.rmtree(part_dir)
                cleared.append(str(part_dir))

    if cleared:
        for p in cleared:
            logger.info("Cleared stale research partition: %s", p)
    else:
        logger.info("No stale research partitions to clear for today's session.")


def wire_level_observer(engine: "BootstrapEngine") -> None:
    """Wire the Phase 0 shadow LevelObserver.

    The observer subscribes to PIPELINE_OUTPUT with drop_if_full=True so it
    never back-pressures the main pipeline. It is not added to engine._engines
    — it has no BaseEngine lifecycle and is flushed directly in _on_stop.
    """
    from pathlib import Path
    from alpha.models.enums import RuntimeMode
    from alpha.research.level_observer import LevelObserver, RunMode
    from alpha.research.parquet_writer import LevelObservationWriter

    mode = engine._settings.runtime.mode
    if mode == RuntimeMode.LIVE or mode == RuntimeMode.PAPER:
        run_mode = RunMode.LIVE
    elif mode == RuntimeMode.REPLAY:
        run_mode = RunMode.REPLAY
    else:
        run_mode = RunMode.BACKFILL

    research_root = Path(engine._settings.storage.parquet_root).parent / "research"
    _clear_research_partitions_for_today(research_root, engine)
    writer = LevelObservationWriter(research_root=research_root)

    from alpha.engines.live.monitor import IngestionMonitor
    ingestion_monitor = (
        engine._ingestion_monitor
        if isinstance(engine._ingestion_monitor, IngestionMonitor)
        else None
    )

    observer = LevelObserver(
        event_bus=engine._event_bus,
        registry=engine._registry,
        writer=writer,
        run_mode=run_mode,
        ingestion_monitor=ingestion_monitor,
    )
    observer.attach()
    engine._level_observer = observer
    logger.info("LevelObserver wired (shadow research pipeline, Phase 0)")


def wire_interaction_engine(engine: "BootstrapEngine") -> None:
    """Wire Phase 1 Level Interaction Engine as shadow research subscriber."""
    from pathlib import Path
    from alpha.research.interaction.engine import LevelInteractionEngine

    research_root = Path(engine._settings.storage.parquet_root).parent / "research"
    # Partitions already cleared by wire_level_observer (called first).
    interaction_engine = LevelInteractionEngine(
        event_bus=engine._event_bus,
        registry=engine._registry,
        research_root=research_root,
    )
    interaction_engine.attach()
    engine._interaction_engine = interaction_engine
    logger.info("LevelInteractionEngine wired (shadow research, Phase 1)")


def wire_execution(engine: "BootstrapEngine") -> None:
    """
    Wire the execution subsystem (MarketStateProjector + coordinator).

    The coordinator starts with all readiness flags False. Flags are updated
    as the bootstrap sequence progresses:
      - broker_connected: set by wire_ibkr / paper mode
      - positions/orders/executions reconciled: set after _start_live catchup
      - account_state_loaded: set after first RiskEngine P&L sync
      - market_data_fresh: set by MarketStateProjector when first snapshot arrives

    For pure paper mode (no live broker), broker_connected and reconciliation
    flags are set to True immediately since paper has no broker state to sync.
    """
    from alpha.engines.execution.coordinator import ExecutionCoordinator
    from alpha.engines.execution.paper_router import PaperOrderRouter
    from alpha.engines.execution.projector import MarketStateProjector
    from alpha.engines.execution.stores import InMemoryIdempotencyStore, InMemoryIntentJournal
    from alpha.engines.execution.v1_risk import V1RiskEvaluator

    assert engine._feature is not None, "FeatureEngine must be wired before execution"

    projector = MarketStateProjector(
        event_bus=engine._event_bus,
        feature_engine=engine._feature,
    )

    risk_evaluator = V1RiskEvaluator(
        registry=engine._registry,
        max_contracts=1,
    )

    # V1: always paper router regardless of runtime mode
    paper_router = PaperOrderRouter(fill_delay_ms=50)

    def _account_state_fn():
        """Pull current account state from RiskEngine for coordinator snapshots."""
        import time
        from datetime import datetime, timezone
        from decimal import Decimal
        from alpha.engines.execution.models import AccountSnapshot

        if engine._risk is not None:
            try:
                state = engine._risk.daily_state("default")
                return AccountSnapshot(
                    snapshot_id=f"acct-{int(time.time() * 1000)}",
                    as_of=datetime.now(timezone.utc),
                    account_id="default",
                    net_liquidation=Decimal(str(state.net_liquidation or 0)),
                    realized_pnl=state.realized_pnl,
                    unrealized_pnl=state.unrealized_pnl,
                    daily_loss_limit=state.daily_loss_limit,
                    session_high_pnl=state.session_high_pnl,
                    is_halted=state.is_halted,
                    halt_reason=state.halt_reason,
                )
            except Exception:
                pass
        # Fallback safe default
        import time
        from datetime import datetime, timezone
        from decimal import Decimal
        from alpha.engines.execution.models import AccountSnapshot
        return AccountSnapshot(
            snapshot_id=f"acct-{int(time.time() * 1000)}",
            as_of=datetime.now(timezone.utc),
            account_id="paper",
            net_liquidation=Decimal("25000.00"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            daily_loss_limit=Decimal("500.00"),
            session_high_pnl=Decimal("0"),
            is_halted=False,
        )

    coordinator = ExecutionCoordinator(
        market_store=projector,
        risk_evaluator=risk_evaluator,
        order_router=paper_router,
        idempotency_store=InMemoryIdempotencyStore(),
        journal=InMemoryIntentJournal(),
        account_state_fn=_account_state_fn,
    )

    # Paper mode: no broker to reconcile — mark broker/reconciliation flags ready.
    # market_data_fresh starts False and flips to True on the first MarketStateEvent.
    coordinator.update_readiness(
        broker_connected=True,
        positions_reconciled=True,
        orders_reconciled=True,
        executions_reconciled=True,
        account_state_loaded=True,
    )

    engine._execution_coordinator = coordinator
    engine._market_state_projector = projector
    logger.info("Execution subsystem wired (paper router, V1 risk evaluator)")
