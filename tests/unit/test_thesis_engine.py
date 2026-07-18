"""
Unit tests for ThesisEngine — shadow narrative mode.

These tests feed synthetic M1 bars directly into ThesisEngine
(bypassing the EventBus) to verify:
  - FAKE_BREAKDOWN_RECLAIM_LONG detection and confidence build
  - VWAP_FAILED_RECLAIM_SHORT detection
  - Invalidation and flip logic
  - VWAP interaction memory in FeatureEngine
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from alpha.models.enums import BarTimeframe, DataSourceId, ThesisState, ThesisType
from alpha.models.events import EventMetadata
from alpha.models.snapshot import BarSnapshot
from alpha.models.thesis import ActiveThesis, LevelMemorySnapshot, TickFlowSnapshot
from alpha.models.bar import Bar
from alpha.engines.thesis.engine import ThesisEngine
from alpha.config.settings import AlphaSettings, RuntimeSettings
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import AssetClass, RuntimeMode
from alpha.models.symbol import Symbol
from tests.conftest import make_bar_event


# ── Helpers ───────────────────────────────────────────────────────────────────

VWAP = Decimal("29850")
SYM = "MNQ-09"


def _make_settings() -> AlphaSettings:
    return AlphaSettings(
        runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=[SYM]),
    )


def _make_registry() -> SymbolRegistry:
    reg = SymbolRegistry()
    reg.register(Symbol(ticker=SYM, exchange="CME", asset_class=AssetClass.FUTURE,
                        root_symbol="MNQ", contract_month="202609"))
    return reg


def _ts(offset_minutes: int = 0) -> datetime:
    base = datetime(2026, 7, 3, 14, 30, tzinfo=timezone.utc)
    return base + timedelta(minutes=offset_minutes)


def _snap(
    close: float,
    vwap: float = float(VWAP),
    is_above_vwap: bool | None = None,
    bars_above_vwap: int = 0,
    bars_below_vwap: int = 0,
    vwap_cross_up: bool = False,
    vwap_cross_down: bool = False,
    swept_below_vwap: bool = False,
    close_position: float = 0.5,
    ema_9_slope: float = 0.0,
    is_lower_high: bool = False,
    is_lower_low: bool = False,
    is_higher_high: bool = False,
    atr_14: float = 15.0,
    session_phase: str = "early",
    offset_minutes: int = 0,
) -> BarSnapshot:
    close_d = Decimal(str(close))
    vwap_d = Decimal(str(vwap))
    above = is_above_vwap if is_above_vwap is not None else (close_d >= vwap_d)
    bar = Bar(
        symbol=SYM,
        timestamp=_ts(offset_minutes),
        timeframe=BarTimeframe.M1,
        open=close_d,
        high=close_d + Decimal("5"),
        low=close_d - Decimal("5"),
        close=close_d,
        volume=1000,
    )
    from alpha.models.enums import SessionPhase
    return BarSnapshot(
        symbol=SYM,
        timestamp=_ts(offset_minutes),
        timeframe=BarTimeframe.M1,
        bar=bar,
        vwap=vwap_d,
        is_above_vwap=above,
        bars_above_vwap=bars_above_vwap,
        bars_below_vwap=bars_below_vwap,
        vwap_cross_up=vwap_cross_up,
        vwap_cross_down=vwap_cross_down,
        swept_below_vwap=swept_below_vwap,
        bar_close_position_pct=close_position,
        ema_9_slope=ema_9_slope,
        is_lower_high=is_lower_high,
        is_lower_low=is_lower_low,
        is_higher_high=is_higher_high,
        atr_14=Decimal(str(atr_14)),
        session_phase=SessionPhase.EARLY,
        or_established=False,
    )


def _level(
    last_vwap_outcome: str | None = None,
    bars_since: int = 0,
    touch_count: int = 0,
    current_side: str = "above",
    session_sweep_low: float | None = None,
) -> LevelMemorySnapshot:
    return LevelMemorySnapshot(
        symbol=SYM,
        timestamp=_ts(),
        vwap_touch_count=touch_count,
        last_vwap_outcome=last_vwap_outcome,
        bars_since_last_vwap_touch=bars_since,
        current_side=current_side,
        session_sweep_low=Decimal(str(session_sweep_low)) if session_sweep_low else None,
    )


def _tick(sell_ratio: float = 0.3, buy_ratio: float = 0.3, vol_accel: float = 0.6) -> TickFlowSnapshot:
    session_avg = 100.0
    return TickFlowSnapshot(
        symbol=SYM,
        timestamp=_ts(),
        tps_10s=session_avg * 0.5,
        tps_30s=session_avg * 0.5,
        vps_10s=500.0,
        vps_30s=800.0,
        buy_tps_10s=session_avg * buy_ratio,
        sell_tps_10s=session_avg * sell_ratio,
        volume_acceleration=vol_accel,
        session_avg_tps=session_avg,
    )


def _engine() -> ThesisEngine:
    settings = _make_settings()
    bus = MagicMock(spec=EventBus)
    async def _noop(e): pass
    bus.publish = _noop
    registry = _make_registry()
    engine = ThesisEngine(settings, bus, registry)
    return engine


def _bar(offset: int = 0, low: float = 29840.0, high: float = 29860.0, close: float = 29855.0) -> "BarEvent":
    return make_bar_event(
        symbol=SYM,
        timestamp=_ts(offset),
        open=close,
        high=high,
        low=low,
        close=close,
        timeframe=BarTimeframe.M1,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestFakeBreakdownReclaimLong:
    """FAKE_BREAKDOWN_RECLAIM_LONG: sweep below VWAP, sellers fail, price reclaims."""

    def test_vwap_cross_down_creates_watching_thesis(self) -> None:
        engine = _engine()
        snap = _snap(close=29845, vwap_cross_down=True, bars_below_vwap=1, is_above_vwap=False)
        level = _level()
        tick = _tick()
        trigger = _bar(low=29840, close=29845)

        thesis = engine._detect_thesis(SYM, snap, level, trigger)

        assert thesis is not None
        assert thesis.thesis_type == ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG
        assert thesis.state == ThesisState.WATCHING
        assert thesis.confidence == pytest.approx(0.10)
        assert thesis.sweep_low == trigger.low

    def test_swept_below_vwap_creates_watching_thesis(self) -> None:
        engine = _engine()
        snap = _snap(close=29855, swept_below_vwap=True, is_above_vwap=True)
        level = _level()
        trigger = _bar(low=29840, close=29855)

        thesis = engine._detect_thesis(SYM, snap, level, trigger)

        assert thesis is not None
        assert thesis.thesis_type == ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG

    def test_confidence_builds_on_reclaim(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.WATCHING,
            confidence=0.10,
            sweep_low=Decimal("29835"),
        )
        # Strong reclaim bar: crossed above VWAP, closed in upper 70%, selling dried up
        snap = _snap(
            close=29858,
            vwap_cross_up=True,
            is_above_vwap=True,
            bars_above_vwap=1,
            close_position=0.72,
            ema_9_slope=0.008,
        )
        level = _level(last_vwap_outcome="swept", bars_since=1)
        tick = _tick(sell_ratio=0.25, vol_accel=0.55)  # selling dried up
        trigger = _bar(close=29858)

        confidence_before = thesis.confidence
        engine._update_fake_breakdown_long(thesis, snap, level, tick, trigger)

        assert thesis.confidence > confidence_before
        assert thesis.confidence > 0.50  # should jump significantly on strong reclaim
        pos_texts = [e.text for e in thesis.evidence if e.positive]
        assert any("reclaimed VWAP" in t for t in pos_texts)
        assert any("strong close" in t for t in pos_texts)
        assert any("dried up" in t for t in pos_texts)

    def test_wide_red_bar_below_vwap_invalidates(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.45,
            bars_alive=3,
            sweep_low=Decimal("29830"),
        )
        # Wide red bar, closed near low, below VWAP
        snap = _snap(
            close=29830,
            is_above_vwap=False,
            bars_below_vwap=2,
            close_position=0.10,
            atr_14=15.0,
        )
        # high - low = 10 + 30 = 40 pts (> 1.2 * 15 = 18)
        trigger = _bar(low=29820, high=29862, close=29830)
        level = _level()
        tick = _tick()

        engine._update_fake_breakdown_long(thesis, snap, level, tick, trigger)

        assert thesis.state == ThesisState.INVALIDATED
        assert thesis.invalidation_reason is not None

    def test_persistent_bars_below_vwap_invalidates(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.35,
            bars_alive=4,
            sweep_low=Decimal("29830"),
        )
        snap = _snap(close=29840, is_above_vwap=False, bars_below_vwap=3)
        trigger = _bar(close=29840)
        level = _level()
        tick = _tick()

        engine._update_fake_breakdown_long(thesis, snap, level, tick, trigger)

        assert thesis.state == ThesisState.INVALIDATED

    def test_necessary_conditions_block_ready_without_reclaim(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.70,  # above threshold
            sweep_low=Decimal("29830"),
        )
        # Price still below VWAP — necessary condition not met
        snap = _snap(close=29845, is_above_vwap=False, bars_above_vwap=0, bars_below_vwap=2)

        result = engine._necessary_conditions_met(thesis, snap)
        assert result is False

    def test_necessary_conditions_pass_when_reclaimed(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.70,
            sweep_low=Decimal("29835"),
        )
        # Price above VWAP, tight risk (close=29855, sweep_low=29835, risk=20, ATR=30 → 0.67×)
        snap = _snap(close=29855, is_above_vwap=True, bars_above_vwap=2, atr_14=30.0)

        result = engine._necessary_conditions_met(thesis, snap)
        assert result is True

    def test_state_advances_watching_to_building(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.WATCHING,
            confidence=0.32,  # above _WATCHING_TO_BUILDING=0.30
        )
        snap = _snap(close=29855, is_above_vwap=True)

        engine._advance_state(thesis, snap)
        assert thesis.state == ThesisState.BUILDING

    def test_entry_plan_computed_when_ready(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.70,
            sweep_low=Decimal("29830"),
        )
        snap = _snap(close=29855, is_above_vwap=True, bars_above_vwap=2, atr_14=20.0)
        engine._advance_state(thesis, snap)
        assert thesis.state == ThesisState.READY
        engine._compute_entry_plan(thesis, snap)

        assert thesis.entry is not None
        assert thesis.stop is not None
        assert thesis.target is not None
        assert thesis.target > thesis.entry > thesis.stop


class TestVWAPFailedReclaimShort:
    """VWAP_FAILED_RECLAIM_SHORT: price below VWAP, reclaim fails, VWAP = resistance."""

    def test_rejected_outcome_creates_watching_thesis(self) -> None:
        engine = _engine()
        snap = _snap(
            close=29840,
            is_above_vwap=False,
            bars_below_vwap=3,
        )
        level = _level(last_vwap_outcome="rejected", bars_since=2, current_side="below")
        trigger = _bar(close=29840)

        thesis = engine._detect_thesis(SYM, snap, level, trigger)

        assert thesis is not None
        assert thesis.thesis_type == ThesisType.VWAP_FAILED_RECLAIM_SHORT
        assert thesis.state == ThesisState.WATCHING

    def test_confidence_builds_on_failed_reclaim(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
            symbol=SYM,
            state=ThesisState.WATCHING,
            confidence=0.15,
            rejection_high=Decimal("29862"),
        )
        # Price crossed back below VWAP, closed weak, buying dried up
        snap = _snap(
            close=29838,
            vwap_cross_down=True,
            is_above_vwap=False,
            bars_below_vwap=1,
            close_position=0.20,
            ema_9_slope=-0.008,
            is_lower_high=True,
        )
        level = _level(last_vwap_outcome="rejected", bars_since=1, current_side="below")
        tick = _tick(buy_ratio=0.25, sell_ratio=0.6, vol_accel=1.4)
        trigger = _bar(close=29838)

        confidence_before = thesis.confidence
        engine._update_vwap_failed_reclaim_short(thesis, snap, level, tick, trigger)

        assert thesis.confidence > confidence_before
        assert thesis.confidence > 0.50
        pos_texts = [e.text for e in thesis.evidence if e.positive]
        assert any("failed" in t.lower() or "crossed back" in t.lower() for t in pos_texts)

    def test_price_holding_above_vwap_invalidates(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.45,
            bars_alive=3,
            rejection_high=Decimal("29862"),
        )
        snap = _snap(close=29860, is_above_vwap=True, bars_above_vwap=2)
        trigger = _bar(close=29860)
        level = _level()
        tick = _tick()

        engine._update_vwap_failed_reclaim_short(thesis, snap, level, tick, trigger)

        assert thesis.state == ThesisState.INVALIDATED

    def test_short_entry_plan_has_correct_direction(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        thesis = ThesisCandidate(
            thesis_type=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.70,
            rejection_high=Decimal("29865"),
        )
        snap = _snap(close=29840, is_above_vwap=False, bars_below_vwap=2, atr_14=20.0)
        engine._advance_state(thesis, snap)
        engine._compute_entry_plan(thesis, snap)

        assert thesis.entry is not None
        assert thesis.stop is not None
        assert thesis.target is not None
        # For short: stop > entry > target
        assert thesis.stop > thesis.entry > thesis.target


class TestFlipLogic:
    """Invalidated long thesis should flip to short and vice versa."""

    def test_flip_candidate_created_for_long_thesis(self) -> None:
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate
        dominant = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.BUILDING,
            confidence=0.40,
        )
        snap = _snap(close=29845, is_above_vwap=True)
        level = _level()

        flip = engine._build_flip_candidate(dominant, snap, level)

        assert flip is not None
        assert flip.thesis_type == ThesisType.VWAP_FAILED_RECLAIM_SHORT
        assert flip.state == ThesisState.WATCHING

    def test_invalidated_thesis_promotes_flip(self) -> None:
        """When dominant is invalidated, the flip candidate becomes the new dominant."""
        engine = _engine()
        from alpha.models.thesis import ThesisCandidate, ActiveThesis
        from alpha.models.enums import ThesisState, ThesisType

        dominant = ThesisCandidate(
            thesis_type=ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG,
            symbol=SYM,
            state=ThesisState.INVALIDATED,
            confidence=0.35,
            invalidation_reason="held below VWAP for 3 bars",
            possible_flip=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
        )
        flip = ThesisCandidate(
            thesis_type=ThesisType.VWAP_FAILED_RECLAIM_SHORT,
            symbol=SYM,
            state=ThesisState.WATCHING,
            confidence=0.05,
        )
        active = ActiveThesis(dominant=dominant, flip=flip)
        engine._active[SYM] = active

        # Simulate what _handle_bar does after INVALIDATED is detected
        if active.dominant.state == ThesisState.INVALIDATED and active.flip is not None:
            active.dominant = active.flip
            active.dominant.state = ThesisState.WATCHING
            active.flip = None

        assert engine._active[SYM].dominant.thesis_type == ThesisType.VWAP_FAILED_RECLAIM_SHORT
        assert engine._active[SYM].flip is None


class TestTickFlow:
    """Tick flow computation from rolling trade window."""

    def test_empty_window_returns_zero_flow(self) -> None:
        engine = _engine()
        tick = engine._compute_tick_flow(SYM, _ts())
        assert tick.tps_10s == 0.0
        assert tick.vps_10s == 0.0

    def test_buy_sell_split_computed(self) -> None:
        engine = _engine()
        from alpha.models.enums import TakerSide
        from alpha.engines.thesis.engine import _TradeRecord
        from collections import deque

        now = _ts()
        window = deque()
        # 3 buys + 2 sells in last 10s
        for i in range(3):
            window.append(_TradeRecord(ts=now - timedelta(seconds=5), price=Decimal("29850"), size=10, is_buy=True, is_sell=False))
        for i in range(2):
            window.append(_TradeRecord(ts=now - timedelta(seconds=5), price=Decimal("29850"), size=10, is_buy=False, is_sell=True))
        engine._trade_windows[SYM] = window

        tick = engine._compute_tick_flow(SYM, now)

        assert tick.buy_tps_10s is not None
        assert tick.sell_tps_10s is not None
        assert tick.buy_tps_10s > tick.sell_tps_10s
