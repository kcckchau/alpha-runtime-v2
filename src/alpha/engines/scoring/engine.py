"""
Engine 7 — Scoring Engine

Responsibilities:
  - Subscribe to SetupEvent (FORMING / CONFIRMED transitions)
  - Score each setup candidate 0–100
  - Assign a grade (SSS / A+ / A / B / C)
  - Record which conditions are met vs. missing
  - Re-score when market state changes significantly

Input:  SetupEvent, MarketStateEvent
Output: Updated Setup score + grade (stored on Setup object, re-emitted
        via SetupEvent with enriched data)

The scoring model is deterministic and rule-based at this layer.
ML-based scoring will be a separate pluggable component.
"""

from __future__ import annotations

import logging

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.models.enums import EventType, HealthStatus, SetupGrade, SetupState
from alpha.models.events import AnyEvent, SetupEvent
from alpha.models.setup import Setup

logger = logging.getLogger(__name__)


class ScoringRubric:
    """
    Declarative scoring rubric.

    Each condition contributes a point value. Max possible = 100.
    Grade thresholds mirror SSS/A+/A/B/C used in live trading review.
    """

    CONDITIONS: list[tuple[str, int]] = [
        # Trend alignment
        ("trend_aligned_with_market", 15),
        ("price_above_ema20", 10),
        ("price_above_vwap", 10),
        # Volume
        ("relative_volume_above_1_5", 10),
        ("volume_expanding_on_move", 10),
        # Setup specifics
        ("clean_level_break", 15),
        ("orb_confirmed", 10),
        ("no_overhead_resistance", 10),
        # Risk quality
        ("tight_stop_available", 10),
    ]

    GRADE_THRESHOLDS: list[tuple[SetupGrade, float]] = [
        (SetupGrade.SSS, 90.0),
        (SetupGrade.A_PLUS, 80.0),
        (SetupGrade.A, 70.0),
        (SetupGrade.B, 55.0),
        (SetupGrade.C, 0.0),
    ]

    @classmethod
    def grade_for(cls, score: float) -> SetupGrade:
        for grade, threshold in cls.GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return SetupGrade.C


class ScoringEngine(BaseEngine):
    """
    Assigns confidence scores and letter grades to detected setups.
    """

    def __init__(self, settings: AlphaSettings, event_bus: EventBus) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._setup_engine: object | None = None
        self._scored_total: int = 0

    @property
    def name(self) -> str:
        return "ScoringEngine"

    def set_setup_engine(self, engine: object) -> None:
        self._setup_engine = engine

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        pass

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.SETUP, self._handle_setup)

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        return EngineHealth(
            HealthStatus.HEALTHY, self.name, {"scored_total": self._scored_total}
        )

    # ── Handler ───────────────────────────────────────────────────────────────

    async def _handle_setup(self, event: AnyEvent) -> None:
        if not isinstance(event, SetupEvent):
            return
        if event.setup_state not in {SetupState.FORMING, SetupState.CONFIRMED}:
            return

        setup = self._get_setup(event.symbol, event.setup_id)
        if setup is None:
            return

        score, met, missing = self._score(setup)
        grade = ScoringRubric.grade_for(score)

        logger.debug(
            "Score: %s %s → %.1f (%s) met=%s missing=%s",
            setup.symbol, setup.setup_type, score, grade, met, missing,
        )
        self._scored_total += 1
        # TODO: update setup object via SetupEngine and re-emit SetupEvent

    # ── Scoring ───────────────────────────────────────────────────────────────

    def _score(self, setup: Setup) -> tuple[float, list[str], list[str]]:
        """Return (score 0-100, conditions_met, conditions_missing)."""
        snap = setup.bar_snapshot
        ms = setup.market_state
        met: list[str] = []
        missing: list[str] = []
        total = 0.0

        checks: dict[str, bool] = {
            "trend_aligned_with_market": ms.trend.value.startswith("trending"),
            "price_above_ema20": snap.is_above_ema20 or False,
            "price_above_vwap": snap.is_above_vwap,
            "relative_volume_above_1_5": (snap.relative_volume or 0.0) >= 1.5,
            "volume_expanding_on_move": False,          # TODO
            "clean_level_break": False,                 # TODO
            "orb_confirmed": ms.orb_volume_confirmed,
            "no_overhead_resistance": False,            # TODO
            "tight_stop_available": snap.atr_14 is not None,
        }

        for name, points in ScoringRubric.CONDITIONS:
            if checks.get(name, False):
                total += points
                met.append(name)
            else:
                missing.append(name)

        return total, met, missing

    def _get_setup(self, symbol: str, setup_id: object) -> Setup | None:
        from alpha.engines.setup.engine import SetupEngine
        if isinstance(self._setup_engine, SetupEngine):
            for s in self._setup_engine.active_setups(symbol):
                if s.setup_id == setup_id:
                    return s
        return None
