"""
Engine 7 — Scoring Engine (side-aware, point-based)

Scores CONFIRMED setups on an integer point scale (no upper bound, but practical
range 0–10).  Grade thresholds:

  B   ≤ 2   observe only
  A   3–4   small size
  A+  5–6   normal size
  SSS ≥ 7   full-size / primary trade

Short setups:
  base score varies by setup type (VWAP_REJECTION=1, others=3)
  + add-ons for confirmatory conditions (+1 each)
  − penalties for contradictory conditions (−1 or −2)

Long setups:
  base score = 3
  + add-ons symmetric to the short rubric

Hard caps (applied after raw scoring):
  VWAP_REJECTION         → max A   (initial cross-down; never SSS/A+)
  live_bias BULLISH      → max A   for all short setups
  live_bias BEARISH      → max A   for all long setups

Scoring happens at CONFIRMED state.  The confirmation bar snapshot
(fetched from FeatureEngine at event time) is used for volume and
close-position checks; the forming bar snapshot (stored in Setup) is
used for structural regime checks.

NOTE: single-timeframe 1m prototype — no 5m bias input yet.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import ClassVar
from uuid import UUID

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.models.enums import (
    EventType,
    HealthStatus,
    LiveBias,
    SetupGrade,
    SetupState,
    SetupType,
    TrendState,
)
from alpha.models.events import AnyEvent, SetupEvent
from alpha.models.market_state import MarketState
from alpha.models.setup import Setup
from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)

# ── Grade order (lowest → highest) ────────────────────────────────────────────
_GRADE_ORDER: list[SetupGrade] = [
    SetupGrade.C,
    SetupGrade.B,
    SetupGrade.A,
    SetupGrade.A_PLUS,
    SetupGrade.SSS,
]


def _grade_index(grade: SetupGrade) -> int:
    try:
        return _GRADE_ORDER.index(grade)
    except ValueError:
        return 0


def _grade_from_score(score: int) -> SetupGrade:
    if score >= 7:
        return SetupGrade.SSS
    if score >= 5:
        return SetupGrade.A_PLUS
    if score >= 3:
        return SetupGrade.A
    if score >= 1:
        return SetupGrade.B
    return SetupGrade.C


def _apply_cap(grade: SetupGrade, cap: SetupGrade) -> SetupGrade:
    """Return the lower of grade and cap."""
    return _GRADE_ORDER[min(_grade_index(grade), _grade_index(cap))]


# ── Short rubric ───────────────────────────────────────────────────────────────

_SHORT_BASE: dict[SetupType, int] = {
    SetupType.VWAP_REJECTION:            1,
    SetupType.VWAP_FAILED_RECLAIM_SHORT: 3,
    SetupType.TREND_PULLBACK_SHORT:      3,
    SetupType.ORB_BREAKDOWN:             2,
}

# (condition_name, points, human-readable reason string)
_SHORT_ADD_ONS: list[tuple[str, int, str]] = [
    ("vwap_slope_down",       1, "VWAP slope down"),
    ("ema9_slope_down",       1, "EMA9 slope down"),
    ("ema20_slope_down",      1, "EMA20 slope down"),
    ("recent_lower_low",      1, "recent lower low in session"),
    ("live_bias_bearish",     1, "live bias bearish"),
    ("trend_down",            1, "market trending down"),
    ("confirm_lower_40pct",   1, "confirm bar closed in lower 40%"),
    ("rvol_above_1_0",        1, "RVOL ≥ 1.0"),
    ("rvol_above_1_3",        1, "RVOL ≥ 1.3"),   # stacks with rvol_above_1_0 for +2 total
    ("tight_risk_width",      1, "risk width ≤ 1× ATR"),
    # Flow bonuses (require BarFlowContext; skipped when flow=None)
    ("flow_net_selling",      1, "net sell delta on confirmation bar"),
    ("flow_large_sellers",    1, "≥2 large sell trades on confirmation bar"),
]

_SHORT_PENALTIES: list[tuple[str, int, str]] = [
    ("vwap_slope_rising",    -1, "VWAP slope rising"),
    ("ema20_slope_rising",   -1, "EMA20 slope rising"),
    ("live_bias_bullish",    -2, "live bias bullish (contra-trend short)"),
    ("chasing_extension",    -1, "price extended >0.50% below VWAP"),
    ("market_not_bearish",   -1, "market regime not bearish"),
    # Flow penalties (require BarFlowContext; skipped when flow=None)
    ("flow_absorption_at_low", -1, "buyers absorbed at low — sellers failed to hold"),
    ("flow_v_reversal_snap",   -1, "down-move snapped back (V-reversal), sellers couldn't hold"),
]

# Hard grade caps per setup type.
_SHORT_GRADE_CAPS: dict[SetupType, SetupGrade] = {
    SetupType.VWAP_REJECTION: SetupGrade.A,  # first cross-down is structurally weaker
}

# ── Long rubric ────────────────────────────────────────────────────────────────

_LONG_BASE: int = 3

_LONG_ADD_ONS: list[tuple[str, int, str]] = [
    ("trend_aligned_bullish",    1, "trend aligned bullish above VWAP"),
    ("price_above_vwap",         1, "price above VWAP"),
    ("price_above_ema20",        1, "price above EMA20"),
    ("live_bias_bullish",        1, "live bias bullish"),
    ("rvol_above_1_0",           1, "RVOL ≥ 1.0"),
    ("rvol_above_1_5",           1, "RVOL ≥ 1.5"),   # stacks with rvol_above_1_0 for +2 total
    ("orb_confirmed",            1, "ORB volume confirmed"),
    ("tight_risk_width",         1, "risk width ≤ 1× ATR"),
    # Flow bonuses (require BarFlowContext; skipped when flow=None)
    ("flow_absorption",          1, "absorption at sweep low confirmed"),
    ("flow_genuine_sweep",       2, "genuine sweep reversal: large buyers absorbed sellers"),
]

_LONG_PENALTIES: list[tuple[str, int, str]] = [
    ("live_bias_bearish",    -2, "live bias bearish (contra-trend long)"),
    ("trend_down",           -1, "market trending down"),
    ("chasing_extension_up", -1, "price extended >0.50% above VWAP"),
    ("market_not_bullish",   -1, "market regime not bullish"),
    # Flow penalties (require BarFlowContext; skipped when flow=None)
    ("flow_v_reversal",      -2, "V-reversal: sweep not genuine, no absorption"),
]

# Grade cap when V-reversal detected on confirmation bar — max B regardless of other conditions.
_LONG_V_REVERSAL_CAP = SetupGrade.B


class ScoringEngine(BaseEngine):
    """Assigns confidence scores and grades to confirmed setups."""

    _SELL_TYPES: ClassVar[frozenset[SetupType]] = frozenset({
        SetupType.VWAP_REJECTION,
        SetupType.VWAP_FAILED_RECLAIM_SHORT,
        SetupType.TREND_PULLBACK_SHORT,
        SetupType.ORB_BREAKDOWN,
    })

    def __init__(self, settings: AlphaSettings, event_bus: EventBus) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._setup_engine: object | None = None
        self._feature_engine: object | None = None
        self._scored_total: int = 0
        self._pipeline_mode: bool = False

    @property
    def name(self) -> str:
        return "ScoringEngine"

    def set_setup_engine(self, engine: object) -> None:
        self._setup_engine = engine

    def set_feature_engine(self, engine: object) -> None:
        """Inject FeatureEngine so we can read the confirmation bar snapshot."""
        self._feature_engine = engine

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
        if self._pipeline_mode:
            return
        if not isinstance(event, SetupEvent):
            return
        # Score only at CONFIRMED — entry/stop are set and confirmation bar is available.
        if event.setup_state != SetupState.CONFIRMED:
            return

        setup = self._get_setup(event.symbol, event.setup_id)
        if setup is None:
            return

        # Confirmation bar snapshot (current bar from FeatureEngine at event time).
        confirm_snap = self._get_current_snapshot(event.symbol)

        score, grade, reasons, penalties = self._compute(setup, confirm_snap)

        logger.info(
            "Score: %s %s → %d (%s) reasons=%s penalties=%s",
            setup.symbol, setup.setup_type, score, grade, reasons, penalties,
        )
        self._scored_total += 1
        self._apply_score(event.symbol, event.setup_id, score, grade, reasons, penalties)

    # ── Pipeline stage ────────────────────────────────────────────────────────

    def process_bar(
        self,
        confirm_snap: "BarSnapshot",
        market_state: object,
        setups: list["Setup"],
    ) -> list["Setup"]:
        """
        Stage 5 of the sequential pipeline.

        Scores any CONFIRMED setups that have not yet been graded.
        `confirm_snap` is the current bar's BarSnapshot (the confirmation bar).
        Returns the same list with scored setups updated in-place.
        """
        for setup in setups:
            if setup.setup_state != SetupState.CONFIRMED:
                continue
            if setup.grade is not None:
                continue   # already scored in a prior bar

            score, grade, reasons, penalties = self._compute(setup, confirm_snap)

            logger.info(
                "Score [pipeline]: %s %s → %d (%s) reasons=%s penalties=%s",
                setup.symbol, setup.setup_type, score, grade, reasons, penalties,
            )
            self._scored_total += 1
            self._apply_score(setup.symbol, setup.setup_id, score, grade, reasons, penalties)

        return setups

    # ── Scoring ───────────────────────────────────────────────────────────────

    def _compute(
        self,
        setup: Setup,
        confirm_snap: BarSnapshot | None,
    ) -> tuple[int, SetupGrade, list[str], list[str]]:
        """Return (raw_score, grade, reasons, penalties)."""
        snap = setup.bar_snapshot           # forming bar snapshot
        cs = confirm_snap or snap           # confirmation bar (fallback to forming)
        ms = setup.market_state

        if setup.setup_type in self._SELL_TYPES:
            return self._score_short(setup, snap, cs, ms)
        return self._score_long(setup, snap, cs, ms)

    def _score_short(
        self,
        setup: Setup,
        snap: BarSnapshot,
        cs: BarSnapshot,
        ms: MarketState,
    ) -> tuple[int, SetupGrade, list[str], list[str]]:
        setup_type = setup.setup_type
        base = _SHORT_BASE.get(setup_type, 1)
        score = base
        reasons: list[str] = [f"{setup_type.value} (base {base})"]
        penalties: list[str] = []

        bearish_bias = ms.live_bias in {LiveBias.BEARISH, LiveBias.TRANSITIONING_BEARISH}
        bullish_bias = ms.live_bias == LiveBias.BULLISH
        # Use confirmation bar's rvol when available; fall back to forming bar.
        rvol = cs.relative_volume if cs.relative_volume is not None else snap.relative_volume

        checks: dict[str, bool] = {
            "vwap_slope_down":     snap.vwap_slope_direction == "down",
            "ema9_slope_down":     snap.ema_9_slope_direction == "down",
            "ema20_slope_down":    snap.ema_20_slope_direction == "down",
            "recent_lower_low":    snap.recent_lower_low,
            "live_bias_bearish":   bearish_bias,
            "trend_down":          ms.trend == TrendState.TRENDING_DOWN,
            "confirm_lower_40pct": (
                cs.bar_close_position_pct is not None
                and cs.bar_close_position_pct <= 0.40
            ),
            "rvol_above_1_0":      rvol is not None and rvol >= 1.0,
            "rvol_above_1_3":      rvol is not None and rvol >= 1.3,
            "tight_risk_width":    self._is_tight_risk(setup, cs),
            **self._short_flow_checks(cs),
        }

        for name, pts, label in _SHORT_ADD_ONS:
            if checks.get(name, False):
                score += pts
                reasons.append(label)

        pen_checks: dict[str, bool] = {
            "vwap_slope_rising":  snap.vwap_slope_direction == "up",
            "ema20_slope_rising": snap.ema_20_slope_direction == "up",
            "live_bias_bullish":  bullish_bias,
            "chasing_extension":  snap.vwap_deviation_pct < -0.50,
            "market_not_bearish": not bearish_bias,
            **self._short_flow_checks(cs),
        }
        for name, pts, label in _SHORT_PENALTIES:
            if pen_checks.get(name, False):
                score += pts   # pts is negative
                penalties.append(label)

        grade = _grade_from_score(score)

        # Hard cap by setup type
        cap = _SHORT_GRADE_CAPS.get(setup_type)
        if cap is not None:
            grade = _apply_cap(grade, cap)

        # Regime cap: bullish market → short max A
        if bullish_bias:
            grade = _apply_cap(grade, SetupGrade.A)

        return score, grade, reasons, penalties

    def _score_long(
        self,
        setup: Setup,
        snap: BarSnapshot,
        cs: BarSnapshot,
        ms: MarketState,
    ) -> tuple[int, SetupGrade, list[str], list[str]]:
        score = _LONG_BASE
        reasons: list[str] = [f"{setup.setup_type.value} (base {_LONG_BASE})"]
        penalties: list[str] = []

        bullish_bias = ms.live_bias in {LiveBias.BULLISH, LiveBias.TRANSITIONING_BULLISH}
        bearish_bias = ms.live_bias == LiveBias.BEARISH
        rvol = cs.relative_volume if cs.relative_volume is not None else snap.relative_volume

        checks: dict[str, bool] = {
            "trend_aligned_bullish": (
                ms.trend == TrendState.TRENDING_UP and snap.is_above_vwap
            ),
            "price_above_vwap":      snap.is_above_vwap,
            "price_above_ema20":     bool(snap.is_above_ema20),
            "live_bias_bullish":     bullish_bias,
            "rvol_above_1_0":        rvol is not None and rvol >= 1.0,
            "rvol_above_1_5":        rvol is not None and rvol >= 1.5,
            "orb_confirmed":         ms.orb_volume_confirmed,
            "tight_risk_width":      self._is_tight_risk(setup, cs),
            **self._long_flow_checks(cs),
        }
        for name, pts, label in _LONG_ADD_ONS:
            if checks.get(name, False):
                score += pts
                reasons.append(label)

        pen_checks: dict[str, bool] = {
            "live_bias_bearish":    bearish_bias,
            "trend_down":           ms.trend == TrendState.TRENDING_DOWN,
            "chasing_extension_up": snap.vwap_deviation_pct > 0.50,
            "market_not_bullish":   not bullish_bias,
            **self._long_flow_checks(cs),
        }
        for name, pts, label in _LONG_PENALTIES:
            if pen_checks.get(name, False):
                score += pts
                penalties.append(label)

        grade = _grade_from_score(score)

        # Regime cap: bearish market → long max A
        if bearish_bias:
            grade = _apply_cap(grade, SetupGrade.A)

        # Flow cap: V-reversal → max B regardless of other conditions
        if checks.get("flow_v_reversal", False):
            grade = _apply_cap(grade, _LONG_V_REVERSAL_CAP)

        return score, grade, reasons, penalties

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _long_flow_checks(cs: BarSnapshot) -> dict[str, bool]:
        """Flow-based checks for long setups. All False when flow data unavailable."""
        flow = cs.flow
        if flow is None:
            return {}
        return {
            "flow_absorption":    flow.absorption.detected,
            "flow_genuine_sweep": flow.is_genuine_sweep_reversal,
            "flow_v_reversal":    flow.is_v_reversal,
        }

    @staticmethod
    def _short_flow_checks(cs: BarSnapshot) -> dict[str, bool]:
        """Flow-based checks for short setups. All False when flow data unavailable."""
        flow = cs.flow
        if flow is None:
            return {}
        return {
            "flow_net_selling":      flow.delta < 0 and flow.has_trade_data,
            "flow_large_sellers":    flow.large_sell_count >= 2,
            "flow_absorption_at_low": flow.absorption.detected,
            "flow_v_reversal_snap":  flow.is_v_reversal,
        }

    @staticmethod
    def _is_tight_risk(setup: Setup, snap: BarSnapshot) -> bool:
        """True when the setup's risk (|entry - stop|) is ≤ 1× ATR-14."""
        if setup.entry_trigger is None or setup.stop_reference is None:
            return False
        if snap.atr_14 is None:
            return False
        risk = abs(setup.entry_trigger - setup.stop_reference)
        return risk <= snap.atr_14 * Decimal("1.0")

    def _apply_score(
        self,
        symbol: str,
        setup_id: UUID,
        score: int,
        grade: SetupGrade,
        reasons: list[str],
        penalties: list[str],
    ) -> None:
        from alpha.engines.setup.engine import SetupEngine
        if isinstance(self._setup_engine, SetupEngine):
            self._setup_engine.patch_setup_score(
                symbol=symbol,
                setup_id=setup_id,
                score=float(score),
                grade=grade,
                conditions_met=reasons,
                conditions_missing=[],
                score_reasons=reasons,
                score_penalties=penalties,
            )

    def _get_setup(self, symbol: str, setup_id: object) -> Setup | None:
        from alpha.engines.setup.engine import SetupEngine
        if isinstance(self._setup_engine, SetupEngine):
            for s in self._setup_engine.active_setups(symbol):
                if s.setup_id == setup_id:
                    return s
        return None

    def _get_current_snapshot(self, symbol: str) -> BarSnapshot | None:
        if self._feature_engine is None:
            return None
        from alpha.engines.feature.engine import FeatureEngine
        if isinstance(self._feature_engine, FeatureEngine):
            return self._feature_engine.get_snapshot(symbol)
        return None
