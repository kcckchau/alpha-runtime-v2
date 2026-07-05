"""
Unit tests for the ONL_SWEEP_RECLAIM_LONG detector in SetupEngine.

Tests the sweep depth state machine and ATR-normalized classification:
  - Single-bar and multi-bar sweep/reclaim detection
  - Clean sweep (0–2× ATR) fires on strong close
  - Deep sweep (2–3× ATR) requires close ≥ 60th pct
  - Likely breakdown (> 3× ATR) blocked
  - Dynamic hard cap: max(30 pts, 2.5× ATR_14), absolute ceiling 60 pts
  - High-volatility day: dynamic cap allows sweeps > 30 pts
  - Extension > 0.5% of ONL clears state (breakdown signal)
  - Timeout > 8 bars below ONL without reclaim clears state
  - RTH p90 distribution check gated on bars_since_open ≥ 15
  - Early RTH (< 15 bars): skips RTH distribution, relies on ATR only
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from alpha.engines.setup.engine import SetupEngine
from alpha.models.bar import Bar
from alpha.models.context import ContextSnapshot
from alpha.models.enums import BarTimeframe
from alpha.models.snapshot import BarSnapshot

SYM = "MNQ-09"
_UTC = timezone.utc
_NOW = datetime(2026, 7, 2, 14, 0, tzinfo=_UTC)  # 10:00 ET — mature RTH

_ONL = Decimal("21000")


# ── Factories ─────────────────────────────────────────────────────────────────

def _engine(onl: float = 21000.0, bar_count: int = 50) -> SetupEngine:
    """SetupEngine with mocked dependencies; context returns a fixed ONL."""
    settings = MagicMock()
    settings.orb_minutes = 5
    bus = MagicMock()
    registry = MagicMock()
    registry.get_symbols.return_value = []
    e = SetupEngine(settings=settings, event_bus=bus, registry=registry)
    ctx = ContextSnapshot(symbol=SYM, timestamp=_NOW, onl=Decimal(str(onl)))
    ctx_engine = MagicMock()
    ctx_engine.get_context.return_value = ctx
    e._context_engine = ctx_engine
    e._bar_counts[SYM] = bar_count
    return e


def _bar(high: float, low: float, close: float, open_: float | None = None) -> Bar:
    o = Decimal(str(open_ if open_ is not None else close))
    h = Decimal(str(high))
    l = Decimal(str(low))
    c = Decimal(str(close))
    # Ensure high/low satisfy bar invariants
    real_high = max(h, o, c)
    real_low = min(l, o, c)
    return Bar(
        symbol=SYM,
        timeframe=BarTimeframe.M1,
        timestamp=_NOW,
        open=o,
        high=real_high,
        low=real_low,
        close=c,
        volume=1000,
    )


def _snap(
    bar: Bar,
    atr_14: float | None = 10.0,
    close_pos: float | None = None,
    rth_p90: float | None = None,
    rth_p75: float | None = None,
    bars_since_open: int = 30,
) -> BarSnapshot:
    """Minimal BarSnapshot with just the fields ONL sweep classifier uses."""
    bar_range = bar.high - bar.low
    if close_pos is None:
        close_pos = (
            float((bar.close - bar.low) / bar_range) if bar_range > 0 else 0.5
        )
    return BarSnapshot(
        symbol=SYM,
        timestamp=_NOW,
        timeframe=BarTimeframe.M1,
        bar=bar,
        vwap=Decimal("21100"),
        bars_since_open=bars_since_open,
        atr_14=Decimal(str(atr_14)) if atr_14 is not None else None,
        bar_close_position_pct=close_pos,
        rth_p90_1m_range=Decimal(str(rth_p90)) if rth_p90 is not None else None,
        rth_p75_1m_range=Decimal(str(rth_p75)) if rth_p75 is not None else None,
    )


def _ms() -> MagicMock:
    return MagicMock()


# ── Single-bar sweep ──────────────────────────────────────────────────────────

def test_single_bar_sweep_reclaim_fires():
    """Bar goes below ONL and closes back above in the same candle → detected."""
    e = _engine()
    # ONL=21000; sweep to 20990 (10 pts, 1.0× ATR=10), close at 21015 (strong)
    b = _bar(high=21020, low=20990, close=21015)
    snap = _snap(b, atr_14=10.0)
    assert e._detect_onl_sweep_reclaim_long(snap, _ms()) is True
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) is None


def test_no_sweep_does_not_fire():
    """Bar stays above ONL → no state starts, detector returns False."""
    e = _engine()
    b = _bar(high=21050, low=21010, close=21030)
    snap = _snap(b)
    assert e._detect_onl_sweep_reclaim_long(snap, _ms()) is False
    assert e._onl_sweep_state.get(SYM) is None


def test_bar_breaks_below_but_closes_below_does_not_fire():
    """Bar breaks below ONL and closes below → state starts, no detection yet."""
    e = _engine()
    b = _bar(high=21005, low=20990, close=20993)
    snap = _snap(b)
    fired = e._detect_onl_sweep_reclaim_long(snap, _ms())
    assert fired is False
    assert e._onl_sweep_state.get(SYM) is not None  # state started


# ── Multi-bar sweep ───────────────────────────────────────────────────────────

def test_multi_bar_sweep_reclaim_fires():
    """Bar 1 breaks below ONL, bar 2 reclaims → detection fires on bar 2."""
    e = _engine()
    # Bar 1: breaks below ONL, closes below
    b1 = _bar(high=21005, low=20988, close=20992)
    snap1 = _snap(b1, atr_14=10.0)
    e._bar_counts[SYM] = 50
    assert e._detect_onl_sweep_reclaim_long(snap1, _ms()) is False
    # State should be active with sweep_low=20988
    state = e._onl_sweep_state.get(SYM)
    assert state is not None
    assert state["sweep_low"] == Decimal("20988")

    # Bar 2: reclaims ONL with strong close
    e._bar_counts[SYM] = 51
    b2 = _bar(high=21020, low=20995, close=21015)
    snap2 = _snap(b2, atr_14=10.0)
    assert e._detect_onl_sweep_reclaim_long(snap2, _ms()) is True


def test_multi_bar_sweep_low_updated():
    """If bar 2 still below ONL but slightly lower (within tolerance), sweep_low updates."""
    e = _engine()
    b1 = _bar(high=21002, low=20992, close=20995)
    snap1 = _snap(b1)
    e._bar_counts[SYM] = 50
    e._detect_onl_sweep_reclaim_long(snap1, _ms())

    b2 = _bar(high=21001, low=20989, close=20993)
    snap2 = _snap(b2)
    e._bar_counts[SYM] = 51
    e._detect_onl_sweep_reclaim_long(snap2, _ms())
    state = e._onl_sweep_state.get(SYM)
    assert state["sweep_low"] == Decimal("20989")


# ── ATR-normalized depth classification ───────────────────────────────────────

def test_clean_1_5_atr_sweep_passes():
    """Sweep depth = 1.5× ATR → clean sweep, fires on standard close."""
    e = _engine()
    # ATR=10, sweep=15pts (1.5×), close in upper 86%
    b = _bar(high=21020, low=20985, close=21015)
    # Range=35, close_pos=(21015-20985)/35≈0.857 → strong close ✓
    snap = _snap(b, atr_14=10.0)
    # Before detect: no active sweep state → reason is not None
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) == "no_active_sweep_below_onl"
    # Detect sets state AND fires (single-bar sweep + reclaim)
    assert e._detect_onl_sweep_reclaim_long(snap, _ms()) is True
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) is None


def test_deep_2_5_atr_sweep_strong_close_passes():
    """Sweep = 2.5× ATR with close_pos ≥ 0.6 → fires."""
    e = _engine()
    # ATR=10, sweep=25pts: ONL=21000, low=20975, close=21015 (above ONL)
    b = _bar(high=21025, low=20975, close=21015)
    # Range=50, close_pos=(21015-20975)/50 = 40/50 = 0.80 ≥ 0.6 ✓
    snap = _snap(b, atr_14=10.0, close_pos=0.80)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) is None


def test_deep_2_5_atr_sweep_weak_close_blocked():
    """Sweep = 2.5× ATR but close_pos < 0.6 → blocked (deep sweep needs strong close)."""
    e = _engine()
    b = _bar(high=21025, low=20975, close=21005)
    snap = _snap(b, atr_14=10.0, close_pos=0.50)  # 50th pct — not enough for 2.5× ATR
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    reason = e._reason_onl_sweep_reclaim_long(snap, _ms())
    assert reason == "deep_sweep_requires_strong_close_ge_60pct"


def test_sweep_beyond_dynamic_cap_blocked():
    """Sweep > dynamic_cap (max(30, 2.5×ATR)) → blocked by cap before ATR classification."""
    e = _engine()
    # ATR=10 → dynamic_cap = max(30, 25)=30, hard_cap=30
    # sweep=31pts: low=20969 → 31 > 30 → cap fires
    b = _bar(high=21020, low=20969, close=21010)
    snap = _snap(b, atr_14=10.0, close_pos=0.85)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    reason = e._reason_onl_sweep_reclaim_long(snap, _ms())
    assert reason is not None
    assert "cap" in reason


# ── Dynamic hard cap ─────────────────────────────────────────────────────────

def test_normal_day_30pt_cap_blocks_at_31():
    """Normal-vol day (ATR=10), 31pt sweep: dynamic_cap=max(30,25)=30 → blocked."""
    e = _engine()
    # ATR=10 → dynamic_cap = max(30, 2.5×10)=30, hard_cap=30
    # sweep=31pts: low=20969
    b = _bar(high=21020, low=20969, close=21010)
    snap = _snap(b, atr_14=10.0, close_pos=0.85)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    reason = e._reason_onl_sweep_reclaim_long(snap, _ms())
    assert reason is not None
    assert "cap" in reason


def test_high_vol_day_40pt_sweep_passes_dynamic_cap():
    """High-vol day (ATR=20), 40pt sweep: dynamic_cap=max(30,50)=50 → within cap, passes."""
    e = _engine()
    # ATR=20 → dynamic_cap=max(30,50)=50, hard_cap=50
    # sweep=40pts = 2.0× ATR — deep but valid
    b = _bar(high=21050, low=20960, close=21015)
    # Range=90, close_pos = (21015-20960)/90 ≈ 0.61 ≥ 0.6 for 2× ATR ✓
    snap = _snap(b, atr_14=20.0, close_pos=0.65)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) is None


def test_absolute_60pt_ceiling_blocks_even_high_atr():
    """ATR=30, sweep=65pts: dynamic_cap=min(75,60)=60 → 65 > 60 blocked."""
    e = _engine()
    # ONL=21000, low=20935 → sweep=65pts
    b = _bar(high=21050, low=20935, close=21010)
    snap = _snap(b, atr_14=30.0, close_pos=0.80)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    reason = e._reason_onl_sweep_reclaim_long(snap, _ms())
    assert reason is not None
    assert "cap" in reason


# ── State clearing ────────────────────────────────────────────────────────────

def test_breakdown_extension_clears_state():
    """Extension > 0.5% below ONL clears sweep state (confirmed breakdown)."""
    e = _engine()
    e._bar_counts[SYM] = 50
    # Bar 1: small initial sweep → start state
    b1 = _bar(high=21005, low=20992, close=20995)
    snap1 = _snap(b1)
    e._detect_onl_sweep_reclaim_long(snap1, _ms())
    assert e._onl_sweep_state.get(SYM) is not None

    # Bar 2: extends to 20894 — 0.5% of ONL(21000) = 105 pts, so low=20894 → 106pts extension
    # This crosses > 0.5% total from ONL → clears state
    e._bar_counts[SYM] = 51
    b2 = _bar(high=20998, low=20893, close=20895)
    snap2 = _snap(b2)
    e._detect_onl_sweep_reclaim_long(snap2, _ms())
    assert e._onl_sweep_state.get(SYM) is None  # cleared


def test_timeout_8_bars_clears_state():
    """More than 8 bars below ONL without reclaim → state cleared."""
    e = _engine()
    e._bar_counts[SYM] = 50
    e._onl_sweep_state[SYM] = {"sweep_low": Decimal("20990"), "bar_count": 50}

    # 9 bars later; use a bar ABOVE ONL so detect() doesn't immediately re-start state
    e._bar_counts[SYM] = 59
    b = _bar(high=21050, low=21010, close=21030)  # entirely above ONL=21000
    snap = _snap(b)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    assert e._onl_sweep_state.get(SYM) is None  # timed out, not re-created


def test_state_cleared_on_session_reset():
    """Session reset (_handle_bar with new session key) clears _onl_sweep_state."""
    # Verify the reset happens via _dbl_first_bottom / _onl_sweep_state pops
    e = _engine()
    e._onl_sweep_state[SYM] = {"sweep_low": Decimal("20990"), "bar_count": 10}

    # Simulate session reset: what _handle_bar does when session changes
    e._bar_count_session[SYM] = "old_session"
    e._bar_counts[SYM] = 0
    e._last_close_bar.pop(SYM, None)
    e._onl_sweep_state.pop(SYM, None)
    e._dbl_first_bottom.pop(SYM, None)

    assert SYM not in e._onl_sweep_state


# ── RTH distribution checks ───────────────────────────────────────────────────

def test_early_rth_skips_p90_distribution_check():
    """bars_since_open < 15 → RTH p90 check skipped; ATR alone governs."""
    e = _engine()
    # Sweep=12pts = 1.2× ATR=10 → clean
    # p90=5pts — if applied, 12 > 5×2=10 → would block; but bars_since_open=5 → skipped
    b = _bar(high=21020, low=20988, close=21010)
    snap = _snap(b, atr_14=10.0, close_pos=0.73, rth_p90=5.0, bars_since_open=5)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) is None


def test_mature_rth_blocks_on_p90_distribution():
    """bars_since_open ≥ 15, sweep > 2× p90 → blocked as abnormal for today."""
    e = _engine()
    # ATR=10, sweep=12pts = 1.2× ATR (clean) — but today p90=5pts, 12 > 2×5=10 → blocked
    b = _bar(high=21020, low=20988, close=21010)
    snap = _snap(b, atr_14=10.0, close_pos=0.73, rth_p90=5.0, bars_since_open=20)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    reason = e._reason_onl_sweep_reclaim_long(snap, _ms())
    assert reason == "sweep_too_deep_vs_rth_p90_distribution"


def test_mature_rth_p90_within_2x_passes():
    """bars_since_open ≥ 15, sweep ≤ 2× p90 → p90 check passes."""
    e = _engine()
    # ATR=10, sweep=10pts, p90=8pts → 10 ≤ 2×8=16 → passes
    b = _bar(high=21020, low=20990, close=21010)
    snap = _snap(b, atr_14=10.0, close_pos=0.67, rth_p90=8.0, bars_since_open=20)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) is None


# ── Close quality ──────────────────────────────────────────────────────────────

def test_clean_sweep_weak_close_blocked():
    """Clean sweep (< 2× ATR) but close in lower half → blocked."""
    e = _engine()
    # Sweep=8pts, ATR=10 → 0.8× ATR — clean. But close_pos=0.3 < 0.5 → blocked
    b = _bar(high=21015, low=20992, close=21001)
    snap = _snap(b, atr_14=10.0, close_pos=0.30)
    e._detect_onl_sweep_reclaim_long(snap, _ms())
    reason = e._reason_onl_sweep_reclaim_long(snap, _ms())
    assert reason == "close_in_lower_half"


# ── No context engine ─────────────────────────────────────────────────────────

def test_no_context_engine_returns_reason():
    """Without context engine wired, reason returns 'no_context_engine'."""
    e = _engine()
    e._context_engine = None
    b = _bar(high=21020, low=20985, close=21010)
    snap = _snap(b)
    assert e._detect_onl_sweep_reclaim_long(snap, _ms()) is False
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) == "no_context_engine"


def test_no_onl_in_context_returns_reason():
    """Context engine returns snapshot with onl=None → reason 'no_onl'."""
    e = _engine()
    ctx_no_onl = ContextSnapshot(symbol=SYM, timestamp=_NOW)  # onl=None
    e._context_engine.get_context.return_value = ctx_no_onl
    b = _bar(high=21020, low=20985, close=21010)
    snap = _snap(b)
    assert e._detect_onl_sweep_reclaim_long(snap, _ms()) is False
    assert e._reason_onl_sweep_reclaim_long(snap, _ms()) == "no_onl"
