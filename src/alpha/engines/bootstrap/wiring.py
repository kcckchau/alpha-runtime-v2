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
    engine._market_state = MarketStateEngine(engine._settings, engine._event_bus, engine._registry)
    engine._market_state.set_feature_engine(engine._feature)
    engine._setup = SetupEngine(engine._settings, engine._event_bus, engine._registry)
    engine._setup.set_feature_engine(engine._feature)
    engine._setup.set_market_state_engine(engine._market_state)
    engine._thesis = ThesisEngine(engine._settings, engine._event_bus, engine._registry)
    engine._thesis.set_feature_engine(engine._feature)
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

    engine._engines = [
        engine._storage,
        engine._historical,
        engine._live,
        engine._feature,
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
