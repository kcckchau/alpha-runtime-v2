"""
BarPipeline
===========
Sequential coordinator for the 1-minute bar processing pipeline.

Subscribes to BAR_BUNDLE (emitted by BarFlowAggregator) and calls each
engine stage in explicit dependency order:

  Stage 1  FeatureEngine.process_bar(bundle)      → BarSnapshot
  Stage 2  MarketStateEngine.process_bar(snap, bundle)  → MarketState
  Stage 3  ThesisEngine.process_bar(...)           → (pending migration)
  Stage 4  SetupEngine.process_bar(...)            → (pending migration)

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
    from alpha.models.market_state import MarketState
    from alpha.models.snapshot import BarSnapshot

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
        # ThesisEngine and SetupEngine will be added in subsequent migrations

    # ── Registration ──────────────────────────────────────────────────────────

    def set_feature_engine(self, engine: "FeatureEngine") -> None:
        self._feature = engine
        engine._pipeline_mode = True
        logger.info("BarPipeline: FeatureEngine registered (pipeline_mode=True)")

    def set_market_state_engine(self, engine: "MarketStateEngine") -> None:
        self._market_state = engine
        engine._pipeline_mode = True
        logger.info("BarPipeline: MarketStateEngine registered (pipeline_mode=True)")

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

        # ── Stage 3: ThesisEngine (not yet migrated) ──────────────────────────
        # ThesisEngine still consumes BAR via EventBus subscription.
        # It calls feature_engine.get_snapshot() which is now guaranteed current.
        # Migration: add thesis.process_bar(snap, market_state, bundle) here.

        # ── Stage 4: SetupEngine (not yet migrated) ───────────────────────────
        # Same as above.
        # Migration: add setup.process_bar(snap, market_state, thesis, bundle) here.

        # ── Publish BarEvent for engines not yet migrated ────────────────────
        # ThesisEngine and SetupEngine still subscribe to BAR. Publish a bare
        # BarEvent derived from the bundle so they continue to receive it.
        # This is removed once all stages are migrated to process_bar().
        bar_event = bundle.to_bar_event()
        await self._bus.publish(bar_event)
