"""
BarPipeline
===========
Sequential coordinator for the 1-minute bar processing pipeline.

Subscribes to BAR_BUNDLE (emitted by BarFlowAggregator) and calls each
engine stage in explicit dependency order:

  Stage 1  FeatureEngine.process_bar(bundle)                    → BarSnapshot
  Stage 2  MarketStateEngine.process_bar(snap, bundle)          → MarketState
  Stage 3  ThesisEngine.process_bar(snap, ms, bundle)           → ActiveThesis | None
  Stage 4  SetupEngine.process_bar(snap, ms, thesis, bundle)    → list[Setup]
  Stage 5  ScoringEngine.process_bar(snap, ms, setups)          → list[Setup] (scored)

This eliminates the EventBus subscription-order fragility: FeatureEngine is
guaranteed to finish before MarketState reads the snapshot, regardless of
which engine registered first.

Migration approach:
  - Engines are migrated one at a time. When an engine is registered with the
    pipeline via set_*_engine(), its _pipeline_mode flag is enabled so its own
    BAR subscription becomes a no-op.
  - Engines not yet registered continue to receive BAR events via their existing
    EventBus subscriptions (they call get_snapshot() which is now guaranteed
    current because FeatureEngine ran first in process_bar).
  - The final state (all engines migrated) removes BAR subscriptions entirely.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from alpha.models.enums import BarTimeframe, EventType
from alpha.models.events import BarBundleEvent

if TYPE_CHECKING:
    from alpha.engines.feature.engine import FeatureEngine
    from alpha.engines.market_state.engine import MarketStateEngine
    from alpha.engines.scoring.engine import ScoringEngine
    from alpha.engines.setup.engine import SetupEngine
    from alpha.engines.thesis.engine import ThesisEngine
    from alpha.models.market_state import MarketState
    from alpha.models.setup import Setup
    from alpha.models.snapshot import BarSnapshot
    from alpha.models.thesis import ActiveThesis

logger = logging.getLogger(__name__)


class BarPipeline:
    """
    Wire engines in order and call them sequentially per BAR_BUNDLE event.

    Usage (in BootstrapEngine._initialize_engines):
        pipeline = BarPipeline(event_bus)
        pipeline.set_feature_engine(feature)
        pipeline.set_market_state_engine(market_state)
        pipeline.attach()   # subscribe to BAR_BUNDLE
    """

    def __init__(self, event_bus) -> None:
        self._bus = event_bus
        self._feature: FeatureEngine | None = None
        self._market_state: MarketStateEngine | None = None
        self._thesis: ThesisEngine | None = None
        self._setup: SetupEngine | None = None
        self._scoring: ScoringEngine | None = None

    # ── Registration ──────────────────────────────────────────────────────────

    def set_feature_engine(self, engine: "FeatureEngine") -> None:
        self._feature = engine
        engine._pipeline_mode = True
        logger.info("BarPipeline: FeatureEngine registered (pipeline_mode=True)")

    def set_market_state_engine(self, engine: "MarketStateEngine") -> None:
        self._market_state = engine
        engine._pipeline_mode = True
        logger.info("BarPipeline: MarketStateEngine registered (pipeline_mode=True)")

    def set_thesis_engine(self, engine: "ThesisEngine") -> None:
        self._thesis = engine
        engine._pipeline_mode = True
        logger.info("BarPipeline: ThesisEngine registered (pipeline_mode=True)")

    def set_setup_engine(self, engine: "SetupEngine") -> None:
        self._setup = engine
        engine._pipeline_mode = True
        logger.info("BarPipeline: SetupEngine registered (pipeline_mode=True)")

    def set_scoring_engine(self, engine: "ScoringEngine") -> None:
        self._scoring = engine
        engine._pipeline_mode = True
        logger.info("BarPipeline: ScoringEngine registered (pipeline_mode=True)")

    def attach(self) -> None:
        """Subscribe to the EventBus. Call after all engines are registered."""
        self._bus.subscribe(EventType.BAR_BUNDLE, self._process)
        logger.info("BarPipeline attached — subscribed to BAR_BUNDLE")

    # ── Pipeline ──────────────────────────────────────────────────────────────

    async def _process(self, bundle: BarBundleEvent) -> None:
        if bundle.timeframe != BarTimeframe.M1:
            return  # only 1m bundles drive the pipeline

        sym = bundle.symbol

        # ── Stage 1: FeatureEngine ────────────────────────────────────────────
        snap: BarSnapshot | None = None
        if self._feature is not None:
            snap = self._feature.process_bar(bundle)
            if snap is None:
                logger.warning("BarPipeline: FeatureEngine returned None for %s — skipping", sym)
                return
        else:
            logger.error("BarPipeline: no FeatureEngine registered — cannot process %s", sym)
            return

        # ── Stage 2: MarketStateEngine ────────────────────────────────────────
        market_state: MarketState | None = None
        if self._market_state is not None:
            market_state = await self._market_state.process_bar(snap, bundle)

        # ── Stage 3: ThesisEngine ─────────────────────────────────────────────
        thesis: ActiveThesis | None = None
        if self._thesis is not None and market_state is not None:
            thesis = await self._thesis.process_bar(snap, market_state, bundle)

        # ── Stage 4: SetupEngine ──────────────────────────────────────────────
        setups: list[Setup] = []
        if self._setup is not None and market_state is not None:
            setups = await self._setup.process_bar(snap, market_state, thesis, bundle)

        # ── Stage 5: ScoringEngine ────────────────────────────────────────────
        if self._scoring is not None and setups:
            self._scoring.process_bar(snap, market_state, setups)

        # ── Publish BarEvent for any engines not yet migrated ────────────────
        # If any stage above is None (engine not registered), publish BarEvent
        # so those engines still receive it via their BAR subscription.
        # Once all five stages are registered, this publish becomes unnecessary
        # and can be removed.
        if self._thesis is None or self._setup is None or market_state is None:
            bar_event = bundle.to_bar_event()
            await self._bus.publish(bar_event)
