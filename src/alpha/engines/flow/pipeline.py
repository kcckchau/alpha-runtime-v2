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
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from alpha.models.enums import BarTimeframe, EventType
from alpha.models.events import BarBundleEvent, PipelineOutputEvent

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
        t0 = time.perf_counter()

        # ── Stage 1: FeatureEngine ────────────────────────────────────────────
        snap: BarSnapshot | None = None
        if self._feature is not None:
            t1 = time.perf_counter()
            snap = self._feature.process_bar(bundle)
            feature_ms = (time.perf_counter() - t1) * 1000
            if snap is None:
                logger.warning("BarPipeline: FeatureEngine returned None for %s — skipping", sym)
                return
        else:
            logger.error("BarPipeline: no FeatureEngine registered — cannot process %s", sym)
            return

        # ── Stage 2: MarketStateEngine ────────────────────────────────────────
        market_state: MarketState | None = None
        market_state_ms = 0.0
        if self._market_state is not None:
            t2 = time.perf_counter()
            market_state = await self._market_state.process_bar(snap, bundle)
            market_state_ms = (time.perf_counter() - t2) * 1000

        # ── Stage 3: ThesisEngine ─────────────────────────────────────────────
        thesis: ActiveThesis | None = None
        thesis_ms = 0.0
        if self._thesis is not None and market_state is not None:
            t3 = time.perf_counter()
            thesis = await self._thesis.process_bar(snap, market_state, bundle)
            thesis_ms = (time.perf_counter() - t3) * 1000

        # ── Stage 4: SetupEngine ──────────────────────────────────────────────
        setups: list[Setup] = []
        setup_ms = 0.0
        if self._setup is not None and market_state is not None:
            t4 = time.perf_counter()
            setups = await self._setup.process_bar(snap, market_state, thesis, bundle)
            setup_ms = (time.perf_counter() - t4) * 1000

        # ── Stage 5: ScoringEngine ────────────────────────────────────────────
        scoring_ms = 0.0
        if self._scoring is not None and setups:
            t5 = time.perf_counter()
            self._scoring.process_bar(snap, market_state, setups)
            scoring_ms = (time.perf_counter() - t5) * 1000

        total_ms = (time.perf_counter() - t0) * 1000
        logger.debug(
            "BarPipeline %s | total=%.1fms feature=%.1f market_state=%.1f"
            " thesis=%.1f setup=%.1f scoring=%.1f",
            sym, total_ms, feature_ms, market_state_ms, thesis_ms, setup_ms, scoring_ms,
        )
        if total_ms > 1000:
            logger.warning(
                "BarPipeline %s SLOW: total=%.1fms feature=%.1f market_state=%.1f"
                " thesis=%.1f setup=%.1f scoring=%.1f",
                sym, total_ms, feature_ms, market_state_ms, thesis_ms, setup_ms, scoring_ms,
            )

        # ── Publish PipelineOutput ────────────────────────────────────────────
        now_utc = datetime.now(timezone.utc)
        e2e_ms = (now_utc - bundle.metadata.received_at).total_seconds() * 1000
        if e2e_ms > 2000:
            logger.warning(
                "BarPipeline %s: bar-to-signal latency=%.0fms (bar_ts=%s received_at=%s)",
                sym, e2e_ms, bundle.timestamp, bundle.metadata.received_at,
            )

        scored_setups = [s for s in setups if s.grade is not None]
        output = PipelineOutputEvent(
            symbol=sym,
            timestamp=bundle.timestamp,
            pipeline_ts=now_utc,
            bar_snapshot=snap,
            flow_context=bundle.flow,
            market_state=market_state,
            thesis=thesis,
            setups=setups,
            scored_setups=scored_setups,
            metadata=bundle.metadata,
        )
        await self._bus.publish(output)

        # ── Publish BarEvent for any engines not yet migrated ────────────────
        # If any stage above is None (engine not registered), publish BarEvent
        # so those engines still receive it via their BAR subscription.
        # Once all five stages are registered, this publish becomes unnecessary
        # and can be removed.
        if self._thesis is None or self._setup is None or market_state is None:
            bar_event = bundle.to_bar_event()
            await self._bus.publish(bar_event)
