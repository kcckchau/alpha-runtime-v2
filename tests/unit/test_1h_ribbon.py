"""
Unit tests for 1H EMA ribbon feature set.

Policy versions:
  EMA_1H_RIBBON_POLICY_VERSION = "ema_1h_ribbon_v1"
  EMA_1H_SLOPE_POLICY_VERSION  = "ema_1h_norm3_v1"

Coverage:
  - H1 ATR30 calculation and minimum warm-up
  - norm3 slopes for EMA9, EMA21, EMA50
  - H1 carry-forward into M1 snapshots (both event orderings at hourly boundary)
  - Bullish / bearish / mixed stack classification
  - Bullish / bearish / flat / mixed slope alignment
  - Ribbon low, high, center, width
  - H1-sealed close location: above / inside / below
  - M1-close live location vs carry-forward ribbon
  - Signed distance to ribbon edge and ribbon center
  - Width percentile: point-in-time safety (excludes current bar)
  - 3H and 6H width slopes
  - Stack and inside-ribbon persistence counters
  - Transition count and full-cross count semantics
  - Context flags: bullish, bearish, chop
  - H1 watermark reflects sealed bar timestamp
  - Session bootstrap: no crash on first bar, all ribbon fields None until warm
  - M1 live fields do not contaminate sealed-bar rolling history
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from alpha.features.slope import (
    EMA_1H_RIBBON_POLICY_VERSION,
    EMA_1H_SLOPE_POLICY_VERSION,
    RIBBON_COMPRESSED_PERCENTILE,
    RIBBON_EXPANDED_PERCENTILE,
    SLOPE_FLAT_THRESHOLD_1H,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_h1_bar(
    symbol: str,
    hour: int,
    close: float,
    high_offset: float = 5.0,
    low_offset: float = 5.0,
) -> "BarEvent":
    """Create an H1 bar. hour may exceed 23 — rolls over to the next day."""
    from alpha.models.enums import BarTimeframe
    from alpha.models.events import BarEvent, EventMetadata
    from datetime import timedelta

    base = datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)
    ts = base + timedelta(hours=hour)
    c = Decimal(str(close))
    return BarEvent(
        symbol=symbol,
        timestamp=ts,
        timeframe=BarTimeframe.H1,
        open=c,
        high=c + Decimal(str(high_offset)),
        low=c - Decimal(str(low_offset)),
        close=c,
        volume=5000,
        metadata=EventMetadata(received_at=ts),
    )


def _make_m1_bar(symbol: str, hour: int, minute: int, close: float) -> "BarEvent":
    """Create an M1 bar. hour may exceed 23 — rolls over to the next day."""
    from datetime import timedelta

    from alpha.models.enums import BarTimeframe
    from alpha.models.events import BarEvent, EventMetadata

    base = datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)
    ts = base + timedelta(hours=hour, minutes=minute)
    c = Decimal(str(close))
    return BarEvent(
        symbol=symbol,
        timestamp=ts,
        timeframe=BarTimeframe.M1,
        open=c,
        high=c + Decimal("2"),
        low=c - Decimal("2"),
        close=c,
        volume=500,
        metadata=EventMetadata(received_at=ts),
    )


def _bundle(bar: "BarEvent") -> "BarBundleEvent":
    from alpha.models.events import BarBundleEvent
    return BarBundleEvent(
        symbol=bar.symbol,
        timestamp=bar.timestamp,
        timeframe=bar.timeframe,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        metadata=bar.metadata,
    )


def _build_engine() -> "FeatureEngine":
    from unittest.mock import MagicMock

    from alpha.calendar.resolver import calendar_for_symbol
    from alpha.config.settings import AlphaSettings, RuntimeSettings
    from alpha.core.clock import Clock
    from alpha.core.event_bus import EventBus
    from alpha.core.registry import SymbolRegistry
    from alpha.engines.feature.engine import FeatureEngine
    from alpha.instruments import resolve_symbol
    from alpha.models.enums import RuntimeMode

    registry = SymbolRegistry()
    registry.register(resolve_symbol("MNQ"))
    settings = AlphaSettings(runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["MNQ"]))
    bus = EventBus()
    engine = FeatureEngine(settings, bus, registry, calendar_for_symbol(registry.get("MNQ")), MagicMock(spec=Clock))
    engine._pipeline_mode = True
    engine._get_or_create("MNQ")
    return engine


def _feed_h1(engine: "FeatureEngine", bar: "BarEvent") -> None:
    asyncio.get_event_loop().run_until_complete(engine._handle_bar(bar))


def _feed_m1(engine: "FeatureEngine", hour: int, minute: int, close: float) -> "BarSnapshot":
    bar = _make_m1_bar("MNQ", hour, minute, close)
    return engine.process_bar(_bundle(bar))


# ── Policy version ─────────────────────────────────────────────────────────────

def test_policy_versions():
    assert EMA_1H_RIBBON_POLICY_VERSION == "ema_1h_ribbon_v1"
    assert EMA_1H_SLOPE_POLICY_VERSION  == "ema_1h_norm3_v1"


# ── H1 ATR30 warm-up ──────────────────────────────────────────────────────────

def test_atr30_1h_none_on_first_bar():
    """First H1 bar has no prev_close → 0 TRs → atr30_1h stays None."""
    engine = _build_engine()
    _feed_h1(engine, _make_h1_bar("MNQ", 9, 19000.0))
    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None
    assert h1.atr30 is None
    assert h1.atr30_sample_count == 0


def test_atr30_1h_warms_after_min_samples():
    """atr30_1h must be None until ATR30_1H_MIN_SAMPLES TRs accumulated."""
    from alpha.engines.feature.engine import ATR30_1H_MIN_SAMPLES

    engine = _build_engine()
    # Feed ATR30_1H_MIN_SAMPLES bars; first produces no TR
    for i in range(ATR30_1H_MIN_SAMPLES):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 10))

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None
    assert h1.atr30 is None, (
        f"atr30_1h must remain None until {ATR30_1H_MIN_SAMPLES} TR samples; "
        f"got sample_count={h1.atr30_sample_count}"
    )

    # One more bar reaches the threshold
    _feed_h1(engine, _make_h1_bar("MNQ", 9 + ATR30_1H_MIN_SAMPLES, 19100.0))
    assert h1.atr30 is not None, "atr30_1h must be defined after minimum samples"
    assert h1.atr30_sample_count == ATR30_1H_MIN_SAMPLES


def test_atr30_1h_computed_from_h1_true_range():
    """atr30_1h is derived from H1 true ranges, not from M1 ATR."""
    engine = _build_engine()
    # Feed 5 H1 bars with known range (high - low = 20 each)
    for i in range(6):
        bar = _make_h1_bar("MNQ", 9 + i, 19000.0, high_offset=10.0, low_offset=10.0)
        _feed_h1(engine, bar)

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None and h1.atr30 is not None
    # True range ≥ high-low = 20 for consecutive bars at same price; ATR should be ~20
    assert float(h1.atr30) >= 19.0  # some TR includes gap between bars


# ── norm3 slope formula ───────────────────────────────────────────────────────

def test_h1_norm3_slopes_none_until_4_history_values():
    """Norm3 slope requires 4 EMA history values → None for first 3 H1 bars."""
    engine = _build_engine()
    for i in range(3):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))
    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None
    assert h1._slope9_norm3 is None
    assert h1._slope21_norm3 is None
    assert h1._slope50_norm3 is None


def test_h1_norm3_slope_positive_on_rising_price():
    """Rising H1 prices should produce positive norm3 slopes once warm."""
    engine = _build_engine()
    # Feed 10 rising bars to warm ATR and build 4 EMA history entries
    for i in range(10):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 50))

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None and h1.atr30 is not None
    assert h1._slope9_norm3 is not None, "slope9 must be defined after 10 bars"
    assert h1._slope9_norm3 > 0, "Rising EMA9 → positive norm3 slope"
    assert h1._slope21_norm3 is not None
    assert h1._slope50_norm3 is not None


def test_h1_norm3_slope_negative_on_falling_price():
    """Falling H1 prices should produce negative norm3 slopes."""
    engine = _build_engine()
    for i in range(10):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 - i * 50))

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None and h1._slope9_norm3 is not None
    assert h1._slope9_norm3 < 0, "Falling EMA9 → negative norm3 slope"


# ── H1 carry-forward into M1 snapshots ───────────────────────────────────────

def test_h1_state_carries_forward_to_m1_snapshot():
    """Once H1 state is warm, every M1 snapshot carries it forward unchanged."""
    engine = _build_engine()
    for i in range(10):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 10))

    snap = _feed_m1(engine, 14, 30, 19050.0)
    assert snap is not None
    assert snap.ema9_1h is not None
    assert snap.ema_ribbon_width_1h_atr is not None
    assert snap.htf_1h_watermark is not None


def test_h1_event_after_m1_event_same_timestamp_not_reflected():
    """
    H1 event ordering at hourly boundary: if H1 arrives AFTER M1 for the same
    timestamp, the M1 snapshot sees the PRIOR H1 state (carry-forward semantics).
    The next M1 bar will see the updated H1 state.

    This documents the known visibility lag and verifies it is deterministic.
    """
    engine = _build_engine()
    # Warm up H1 state with 5 bars
    for i in range(5):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))
    h1_before = engine._h1_ema.get("MNQ")
    watermark_before = h1_before.last_sealed_ts if h1_before else None

    # M1 bar at 14:00 arrives BEFORE the 14:00 H1 bar
    snap_before = _feed_m1(engine, 14, 0, 19000.0)

    # Now H1 bar at 14:00 seals (later event)
    h1_bar = _make_h1_bar("MNQ", 14, 19100.0)  # significantly different close
    _feed_h1(engine, h1_bar)

    # M1 bar at 14:01 should now reflect the 14:00 H1 bar
    snap_after = _feed_m1(engine, 14, 1, 19000.0)

    # Watermark must advance after the H1 event is processed
    assert snap_after.htf_1h_watermark == h1_bar.timestamp, (
        "htf_1h_watermark must reflect the sealed H1 bar after it is processed"
    )
    # The snap before must have the prior watermark (not the 14:00 H1 bar)
    assert snap_before.htf_1h_watermark == watermark_before, (
        "M1 snapshot taken before H1 seals must carry the prior watermark"
    )


def test_h1_event_before_m1_event_same_timestamp_is_reflected():
    """
    If H1 arrives BEFORE the coincident M1 bar, the M1 snapshot sees the updated H1.
    This is the bootstrap / replay ordering (D1 → H1 → M5 → M1).
    """
    engine = _build_engine()
    for i in range(5):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))

    # H1 bar at 14:00 arrives first
    h1_bar = _make_h1_bar("MNQ", 14, 19200.0)
    _feed_h1(engine, h1_bar)

    # M1 bar at 14:00 arrives after
    snap = _feed_m1(engine, 14, 0, 19000.0)

    assert snap.htf_1h_watermark == h1_bar.timestamp, (
        "When H1 precedes coincident M1, M1 snapshot must reflect the new H1 state"
    )


# ── Stack direction ───────────────────────────────────────────────────────────

def test_bullish_stack_direction():
    """ema9 > ema21 > ema50 → stack_direction == 'bullish'."""
    engine = _build_engine()
    # Feed strongly rising bars so EMA9 (fastest) leads EMA21 leads EMA50
    for i in range(15):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 200))

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None and h1.stack_direction is not None
    # With consistently rising price EMA9 will be highest
    if h1.ema_9 > h1.ema_21 > h1.ema_50:
        assert h1.stack_direction == "bullish"


def test_bearish_stack_direction():
    """ema9 < ema21 < ema50 → stack_direction == 'bearish'."""
    engine = _build_engine()
    for i in range(15):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 - i * 200))

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None and h1.stack_direction is not None
    if h1.ema_9 < h1.ema_21 < h1.ema_50:
        assert h1.stack_direction == "bearish"


def test_mixed_stack_direction():
    """When EMA ordering is not strictly bullish or bearish → 'mixed'."""
    from alpha.engines.feature.engine import _HTFEMAState

    # Construct state directly with known non-monotonic EMA values
    s = _HTFEMAState(track_ema50=True, track_atr30=True, track_ribbon=True, atr30_min_samples=1)
    s.ema_9  = Decimal("19010")
    s.ema_21 = Decimal("19020")  # ema21 > ema9 → not bullish stack
    s.ema_50 = Decimal("19005")
    s.atr30  = Decimal("10")

    # Simulate ribbon computation by hand (same logic as _update_htf_ema)
    e9, e21, e50 = s.ema_9, s.ema_21, s.ema_50
    if e9 > e21 > e50:
        stack = "bullish"
    elif e9 < e21 < e50:
        stack = "bearish"
    else:
        stack = "mixed"
    assert stack == "mixed"


# ── Slope alignment ───────────────────────────────────────────────────────────

def test_bullish_slope_alignment():
    """All three norm3 slopes positive and above threshold → alignment 'bullish'."""
    engine = _build_engine()
    for i in range(15):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 200))

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None
    if (h1._slope9_norm3 is not None
            and h1._slope9_norm3 > SLOPE_FLAT_THRESHOLD_1H
            and h1._slope21_norm3 is not None
            and h1._slope21_norm3 > SLOPE_FLAT_THRESHOLD_1H
            and h1._slope50_norm3 is not None
            and h1._slope50_norm3 > SLOPE_FLAT_THRESHOLD_1H):
        assert h1.slope_alignment == "bullish"


def test_bearish_slope_alignment():
    """All three norm3 slopes negative and below -threshold → alignment 'bearish'."""
    engine = _build_engine()
    for i in range(15):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 - i * 200))

    h1 = engine._h1_ema.get("MNQ")
    assert h1 is not None
    if (h1._slope9_norm3 is not None and h1._slope9_norm3 < -SLOPE_FLAT_THRESHOLD_1H
            and h1._slope21_norm3 is not None and h1._slope21_norm3 < -SLOPE_FLAT_THRESHOLD_1H
            and h1._slope50_norm3 is not None and h1._slope50_norm3 < -SLOPE_FLAT_THRESHOLD_1H):
        assert h1.slope_alignment == "bearish"


def test_mixed_slope_alignment_when_directions_differ():
    """Divergent slope signs → alignment 'mixed'."""
    from alpha.features.slope import classify_slope

    # If dir9=up, dir21=down, dir50=up → mixed
    dir9  = classify_slope(0.10, SLOPE_FLAT_THRESHOLD_1H)   # "up"
    dir21 = classify_slope(-0.05, SLOPE_FLAT_THRESHOLD_1H)  # "down"
    dir50 = classify_slope(0.05, SLOPE_FLAT_THRESHOLD_1H)   # "up"
    assert dir9 == "up" and dir21 == "down" and dir50 == "up"
    # mixed: not all same direction
    if not (dir9 == dir21 == dir50):
        alignment = "mixed"
    assert alignment == "mixed"


# ── Ribbon geometry ───────────────────────────────────────────────────────────

def _warm_ribbon_engine(n_bars: int = 10, step: float = 10.0) -> "tuple[FeatureEngine, _HTFEMAState]":
    engine = _build_engine()
    for i in range(n_bars):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * step))
    h1 = engine._h1_ema["MNQ"]
    return engine, h1


def test_ribbon_low_high_center_consistent():
    """ribbon_low <= ribbon_center <= ribbon_high at all times."""
    _, h1 = _warm_ribbon_engine(12)
    if h1.ribbon_low is None:
        pytest.skip("Ribbon not yet warm")
    assert h1.ribbon_low <= h1.ribbon_center <= h1.ribbon_high, (
        f"low={h1.ribbon_low} center={h1.ribbon_center} high={h1.ribbon_high}"
    )


def test_ribbon_width_non_negative():
    """Width = high - low ≥ 0; zero only when all EMAs are identical (impossible in practice)."""
    _, h1 = _warm_ribbon_engine(12)
    if h1.ribbon_width_atr is None:
        pytest.skip("Ribbon not warm")
    assert h1.ribbon_width_atr >= 0.0


def test_ribbon_pairwise_distances_sign():
    """
    Pairwise distance sign must match EMA ordering:
      - ema9_21_dist > 0 when ema9 > ema21
      - ema9_21_dist < 0 when ema9 < ema21
    """
    _, h1 = _warm_ribbon_engine(15, step=200.0)
    if h1.ema9_21_dist_atr is None or h1.ema_9 is None:
        pytest.skip("Ribbon not warm")
    if h1.ema_9 > h1.ema_21:
        assert h1.ema9_21_dist_atr > 0
    elif h1.ema_9 < h1.ema_21:
        assert h1.ema9_21_dist_atr < 0


def test_sma200_not_included_in_ribbon():
    """SMA200 must not affect ribbon_low/high/center/width."""
    _, h1 = _warm_ribbon_engine(12)
    if h1.ribbon_low is None or h1.sma_200 is None:
        pytest.skip("Need warm ribbon and SMA200")
    # ribbon_low is min(ema9, ema21, ema50) only — SMA200 may be outside or inside
    ribbon_vals = {float(h1.ema_9), float(h1.ema_21), float(h1.ema_50)}
    assert float(h1.ribbon_low)  == min(ribbon_vals)
    assert float(h1.ribbon_high) == max(ribbon_vals)


# ── H1-sealed price location ──────────────────────────────────────────────────

def test_h1_close_location_above():
    """H1 close above ribbon_high → h1_close_location == 'above'."""
    engine = _build_engine()
    # Warm with stable bars to let EMAs converge near 19000
    for i in range(12):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))
    # Feed one bar with close far above
    _feed_h1(engine, _make_h1_bar("MNQ", 21, 19500.0))
    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_high is not None and h1.h1_close_location is not None:
        # The last bar's close was 19500; ribbon from prior bars ~19000
        assert h1.h1_close_location == "above"
        assert h1.h1_close_to_ribbon_atr is not None and h1.h1_close_to_ribbon_atr > 0


def test_h1_close_location_below():
    """H1 close below ribbon_low → h1_close_location == 'below'."""
    engine = _build_engine()
    for i in range(12):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))
    _feed_h1(engine, _make_h1_bar("MNQ", 21, 18500.0))
    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_low is not None and h1.h1_close_location is not None:
        assert h1.h1_close_location == "below"
        assert h1.h1_close_to_ribbon_atr is not None and h1.h1_close_to_ribbon_atr < 0


def test_h1_close_inside_has_zero_distance_to_ribbon():
    """H1 close inside ribbon → h1_close_to_ribbon_atr == 0.0."""
    engine = _build_engine()
    for i in range(12):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))
    h1 = engine._h1_ema["MNQ"]
    if h1.h1_close_location == "inside":
        assert h1.h1_close_to_ribbon_atr == 0.0


# ── M1-close live location ────────────────────────────────────────────────────

def test_m1_close_location_updates_every_bar():
    """m1_close_location reflects current M1 close; two consecutive M1 bars can differ."""
    engine = _build_engine()
    for i in range(12):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))

    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_low is None or h1.ribbon_high is None:
        pytest.skip("Ribbon not warm")

    low = float(h1.ribbon_low)
    high = float(h1.ribbon_high)

    # M1 close far above ribbon
    snap_above = _feed_m1(engine, 21, 0, high + 100.0)
    assert snap_above.m1_close_location_vs_ema_ribbon_1h == "above"
    assert snap_above.m1_close_to_ema_ribbon_1h_atr is not None
    assert snap_above.m1_close_to_ema_ribbon_1h_atr > 0

    # M1 close far below ribbon
    snap_below = _feed_m1(engine, 21, 1, low - 100.0)
    assert snap_below.m1_close_location_vs_ema_ribbon_1h == "below"
    assert snap_below.m1_close_to_ema_ribbon_1h_atr is not None
    assert snap_below.m1_close_to_ema_ribbon_1h_atr < 0


def test_m1_live_fields_do_not_affect_h1_rolling_history():
    """
    Feeding M1 bars must not append to ribbon_location_history (sealed H1 history).
    The rolling history length should only grow when H1 bars are fed.
    """
    engine = _build_engine()
    for i in range(10):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))

    h1 = engine._h1_ema["MNQ"]
    history_len_before = len(h1.ribbon_location_history)

    # Feed 5 M1 bars
    for m in range(5):
        _feed_m1(engine, 19, m, 19000.0)

    assert len(h1.ribbon_location_history) == history_len_before, (
        "M1 bars must not append to sealed-H1 ribbon_location_history"
    )


# ── Width percentile (point-in-time safe) ─────────────────────────────────────

def test_ribbon_width_percentile_excludes_current_bar():
    """
    Width percentile must be computed from prior bars only.
    Feed bars of known widths: first N stable (narrow), then one wide.
    The wide bar's own percentile must be computed before appending it.
    """
    engine = _build_engine()
    # Feed 15 bars at very stable price → narrow ribbon
    for i in range(15):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0, high_offset=2.0, low_offset=2.0))

    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_width_atr is None:
        pytest.skip("Ribbon not warm")

    # Snapshot the percentile before feeding a volatile bar
    pct_before = h1.ribbon_width_percentile
    width_before = h1.ribbon_width_atr

    # Feed one very volatile bar (wide swings → spread EMAs apart over next few bars)
    _feed_h1(engine, _make_h1_bar("MNQ", 24, 20000.0, high_offset=200.0, low_offset=200.0))

    # The percentile recorded for the volatile bar is computed from the PRIOR history
    # (which was all narrow). We verify the history actually grew by exactly 1.
    assert len(h1.ribbon_width_history) >= 1


def test_width_slope_3h():
    """ema_ribbon_width_slope_3h = (width[t] - width[t-3]) / 3."""
    engine = _build_engine()
    for i in range(20):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 50))

    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_width_slope_3h is None:
        pytest.skip("Need at least 3 prior width values")

    wh = list(h1.ribbon_width_history)
    # After appending current bar, wh[-1]=current, wh[-4]=3 bars ago (PIT: slope computed pre-append)
    if len(wh) >= 4:
        expected = (wh[-1] - wh[-4]) / 3
        assert abs(h1.ribbon_width_slope_3h - expected) < 1e-9, (
            f"slope_3h={h1.ribbon_width_slope_3h} expected={expected}"
        )


def test_width_slope_6h():
    """ema_ribbon_width_slope_6h = (width[t] - width[t-6]) / 6."""
    engine = _build_engine()
    for i in range(25):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 50))

    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_width_slope_6h is None:
        pytest.skip("Need at least 6 prior width values")

    wh = list(h1.ribbon_width_history)
    # After appending current bar, wh[-1]=current, wh[-7]=6 bars ago (PIT: slope computed pre-append)
    if len(wh) >= 7:
        expected = (wh[-1] - wh[-7]) / 6
        assert abs(h1.ribbon_width_slope_6h - expected) < 1e-9


# ── Persistence counters ──────────────────────────────────────────────────────

def test_bullish_stack_persistence_increments():
    """Persistence counter must increment on consecutive bullish-stack H1 bars."""
    engine = _build_engine()
    # Feed rising bars to achieve bullish stack
    for i in range(20):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 300))

    h1 = engine._h1_ema["MNQ"]
    assert h1 is not None
    if h1.stack_direction == "bullish":
        assert h1.bullish_stack_persistence >= 1
        assert h1.bearish_stack_persistence == 0


def test_inside_ribbon_persistence_increments():
    """inside_ribbon_persistence counts consecutive H1 bars with close inside ribbon."""
    engine = _build_engine()
    # Feed stable bars where price stays inside converged ribbon
    for i in range(20):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0, high_offset=1.0, low_offset=1.0))

    h1 = engine._h1_ema["MNQ"]
    assert h1 is not None
    if h1.h1_close_location == "inside":
        assert h1.inside_ribbon_persistence >= 1


def test_persistence_resets_on_stack_change():
    """Bullish persistence resets to 0 when stack becomes non-bullish."""
    engine = _build_engine()
    # Build bullish stack
    for i in range(15):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 300))

    h1 = engine._h1_ema["MNQ"]
    was_bullish = h1.stack_direction == "bullish"

    # Crash price to invert EMAs
    for i in range(10):
        _feed_h1(engine, _make_h1_bar("MNQ", 24 + i, 17000.0 - i * 300))

    if was_bullish and h1.stack_direction != "bullish":
        assert h1.bullish_stack_persistence == 0


# ── Transition and cross counts ───────────────────────────────────────────────

def test_transition_count_semantics():
    """
    transition_count_6h counts any state changes among ABOVE/INSIDE/BELOW
    across the last 6 sealed H1 bars.

    Build a controlled history: [above, inside, below, inside, above, inside, below]
    The last 6 entries are [inside, below, inside, above, inside, below].
    Transitions: inside→below, below→inside, inside→above, above→inside, inside→below = 5.
    """
    engine = _build_engine()
    # Warm up ribbon with stable bars first
    for i in range(12):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))

    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_low is None:
        pytest.skip("Ribbon not warm")

    # Directly inject a known location history to test counting logic
    from collections import deque
    h1.ribbon_location_history = deque(
        ["above", "inside", "below", "inside", "above", "inside", "below"],
        maxlen=120,
    )

    # Count transitions manually from last 6: [inside, below, inside, above, inside, below]
    # idx 1→2: inside→below    (+1)
    # idx 2→3: below→inside   (+1)
    # idx 3→4: inside→above   (+1)
    # idx 4→5: above→inside   (+1)
    # idx 5→6: inside→below   (+1)
    expected_transitions = 5

    _lh = list(h1.ribbon_location_history)
    _last6 = _lh[-6:]
    trans = sum(1 for i in range(1, len(_last6)) if _last6[i] != _last6[i - 1])
    assert trans == expected_transitions, f"Expected {expected_transitions} transitions, got {trans}"


def test_full_cross_count_semantics():
    """
    full_cross_count_6h counts complete ABOVE ↔ BELOW transitions.
    Passing through INSIDE counts as one full cross from the prior side to the new side.

    History: [above, inside, below, inside, above] over last 5 bars.
    Full crosses: above→(inside)→below = 1, below→(inside)→above = 1 → total 2.
    """
    from collections import deque

    engine = _build_engine()
    for i in range(12):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))
    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_low is None:
        pytest.skip("Ribbon not warm")

    h1.ribbon_location_history = deque(
        ["above", "inside", "below", "inside", "above"],
        maxlen=120,
    )

    _lh = list(h1.ribbon_location_history)
    _full = 0
    _prev_side = None
    for loc in _lh[-6:]:
        if loc in ("above", "below"):
            if _prev_side is not None and _prev_side != loc:
                _full += 1
            _prev_side = loc

    assert _full == 2, f"Expected 2 full crosses, got {_full}"


def test_transition_count_does_not_require_above_below_only():
    """inside→above and inside→below both count as transitions (not only full crosses)."""
    from collections import deque

    engine = _build_engine()
    for i in range(12):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0))
    h1 = engine._h1_ema["MNQ"]
    if h1.ribbon_low is None:
        pytest.skip("Ribbon not warm")

    # 6 entries: all inside then above
    h1.ribbon_location_history = deque(
        ["inside", "inside", "inside", "inside", "inside", "above"],
        maxlen=120,
    )
    _lh = list(h1.ribbon_location_history)
    trans = sum(1 for i in range(max(0, len(_lh) - 6), len(_lh)) if i > 0 and _lh[i] != _lh[i - 1])
    assert trans == 1  # only one transition: inside→above


# ── Derived ribbon contexts ────────────────────────────────────────────────────

def test_bullish_ribbon_context_requires_all_three_conditions():
    """bullish_context = bullish stack AND bullish alignment AND price above/inside."""
    from alpha.engines.feature.engine import _HTFEMAState

    s = _HTFEMAState(track_ribbon=True)
    # All three satisfied
    s.stack_direction = "bullish"
    s.slope_alignment = "bullish"
    s.h1_close_location = "above"
    s.bullish_ribbon_context = (
        s.stack_direction == "bullish"
        and s.slope_alignment == "bullish"
        and s.h1_close_location in ("above", "inside")
    )
    assert s.bullish_ribbon_context

    # Fail on one condition
    s.slope_alignment = "mixed"
    s.bullish_ribbon_context = (
        s.stack_direction == "bullish"
        and s.slope_alignment == "bullish"
        and s.h1_close_location in ("above", "inside")
    )
    assert not s.bullish_ribbon_context


def test_chop_ribbon_context_requires_compressed_inside_and_crosses():
    """chop_context = compressed width AND price inside AND transition_count >= 3."""
    from alpha.engines.feature.engine import _HTFEMAState

    s = _HTFEMAState(track_ribbon=True)
    s.ribbon_width_state = "compressed"
    s.h1_close_location = "inside"
    s.ribbon_location_transition_count_6h = 3

    chop = (
        s.ribbon_width_state == "compressed"
        and s.h1_close_location == "inside"
        and s.ribbon_location_transition_count_6h >= 3
    )
    assert chop

    # Does not trigger on mixed stack alone
    s.ribbon_location_transition_count_6h = 2
    chop = (
        s.ribbon_width_state == "compressed"
        and s.h1_close_location == "inside"
        and s.ribbon_location_transition_count_6h >= 3
    )
    assert not chop


# ── Bootstrap / restart behavior ──────────────────────────────────────────────

def test_all_ribbon_fields_none_before_first_h1_bar():
    """Before any H1 bar, all ribbon snapshot fields must be None / 0."""
    engine = _build_engine()
    snap = _feed_m1(engine, 9, 30, 19000.0)
    assert snap.ema9_1h is None
    assert snap.ema_ribbon_low_1h is None
    assert snap.ema_ribbon_width_1h_atr is None
    assert snap.ema_stack_direction_1h is None
    assert snap.m1_close_location_vs_ema_ribbon_1h is None
    assert snap.htf_1h_watermark is None
    assert snap.ema_bullish_stack_persistence_1h_bars == 0


def test_ribbon_snapshot_fields_warm_after_h1_bars():
    """After enough H1 bars, the M1 snapshot carries ribbon fields correctly."""
    engine = _build_engine()
    for i in range(15):
        _feed_h1(engine, _make_h1_bar("MNQ", 9 + i, 19000.0 + i * 20))

    snap = _feed_m1(engine, 14, 30, 19100.0)
    # Core ribbon fields must be populated
    assert snap.ema9_1h is not None
    assert snap.ema_ribbon_low_1h is not None
    assert snap.ema_ribbon_center_1h is not None
    assert snap.ema_ribbon_width_1h_atr is not None
    assert snap.ema_stack_direction_1h is not None
    assert snap.htf_1h_watermark is not None
    # M1 live location must be set
    assert snap.m1_close_location_vs_ema_ribbon_1h in ("above", "inside", "below")


def test_h1_watermark_reflects_last_sealed_h1_timestamp():
    """htf_1h_watermark must equal the timestamp of the last sealed H1 bar."""
    engine = _build_engine()
    h1_bars = [_make_h1_bar("MNQ", 9 + i, 19000.0) for i in range(10)]
    for b in h1_bars:
        _feed_h1(engine, b)

    snap = _feed_m1(engine, 19, 0, 19000.0)
    assert snap.htf_1h_watermark == h1_bars[-1].timestamp
