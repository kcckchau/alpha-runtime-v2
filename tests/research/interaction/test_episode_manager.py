"""
Tests for Phase 1 Level Interaction Engine.

Acceptance criterion: "On deterministic M1 replay, can the engine reliably keep
noisy VWAP crossings inside one episode, close only after meaningful separation,
and create a later second episode on re-approach?"
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Tuple

import pytest

from alpha.research.interaction.config import LevelDistanceConfig
from alpha.research.interaction.episode import InteractionEpisodeManager
from alpha.research.interaction.models import (
    EpisodeBarRecord,
    EpisodeSummary,
    InteractionFrame,
    LevelSnapshot,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

SYMBOL = "MNQ"
SESSION_ID = "MNQ:2026-07-10"
SESSION_DATE = "2026-07-10"
LEVEL_VALUE = Decimal("21000.00")
TICK_SIZE = Decimal("0.25")
ATR_14 = Decimal("50.00")   # 50 points = 200 ticks

# With default config (proximity=20% ATR = 10 ticks, separation=50% ATR = 25 ticks)
# proximity_ticks = int(200 * 0.20) = 40 ticks
# separation_ticks = int(200 * 0.50) = 100 ticks

VWAP_LEVEL = LevelSnapshot(
    level_id=f"vwap:{SYMBOL}:{SESSION_ID}",
    symbol=SYMBOL,
    session_id=SESSION_ID,
    level_type="vwap",
    level_value=LEVEL_VALUE,
    tick_size=TICK_SIZE,
    is_dynamic=True,
    sampling_note="end_of_bar_cumulative_vwap",
)

BASE_TS = datetime(2026, 7, 10, 14, 0, 0, tzinfo=timezone.utc)


def _cfg(**kwargs) -> LevelDistanceConfig:
    defaults = dict(
        proximity_atr_fraction=0.20,
        separation_atr_fraction=0.50,
        min_separation_bars=3,
        max_episode_bars=60,
        max_bar_gap_seconds=90,
        fallback_proximity_ticks=10,
        fallback_separation_ticks=25,
    )
    defaults.update(kwargs)
    return LevelDistanceConfig(**defaults)


def _frame(
    ts: datetime,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    level_value: float = 21000.0,
    atr_14: float = 50.0,
) -> InteractionFrame:
    level = LevelSnapshot(
        level_id=f"vwap:{SYMBOL}:{SESSION_ID}",
        symbol=SYMBOL,
        session_id=SESSION_ID,
        level_type="vwap",
        level_value=Decimal(str(level_value)),
        tick_size=TICK_SIZE,
        is_dynamic=True,
        sampling_note="end_of_bar_cumulative_vwap",
    )
    return InteractionFrame(
        bar_timestamp=ts,
        symbol=SYMBOL,
        session_id=SESSION_ID,
        sequence_num=None,
        open=Decimal(str(open_price)),
        high=Decimal(str(high_price)),
        low=Decimal(str(low_price)),
        close=Decimal(str(close_price)),
        volume=1000,
        atr_14=Decimal(str(atr_14)),
        session_phase="mid",
        is_replay=True,
        levels=(level,),
    )


def _ts(offset_minutes: int) -> datetime:
    return BASE_TS + timedelta(minutes=offset_minutes)


def _run_frames(frames: list[InteractionFrame]) -> tuple[list[EpisodeSummary], list[EpisodeBarRecord]]:
    """Run frames through manager, return all completed episodes and bars."""
    summaries: list[EpisodeSummary] = []
    all_bars: list[EpisodeBarRecord] = []

    def on_complete(s: EpisodeSummary, bars: list[EpisodeBarRecord]) -> None:
        summaries.append(s)
        all_bars.extend(bars)

    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=on_complete)
    for f in frames:
        mgr.process(f)
    mgr.flush()
    return summaries, all_bars


# ── Test 1: Quick separation ──────────────────────────────────────────────────

def test_quick_separation():
    """Price enters proximity, stays 1 bar, then separates → 1 episode."""
    # proximity_ticks = int(200 * 0.20) = 40 ticks
    # separation_ticks = int(200 * 0.50) = 100 ticks
    # LEVEL = 21000.00, TICK = 0.25

    frames = [
        # Bar 0: far above (300 ticks = 75 points above)
        _frame(_ts(0), 21075.0, 21080.0, 21070.0, 21075.0),
        # Bar 1: enters proximity (close = 21008 = 32 ticks above — within 40)
        _frame(_ts(1), 21010.0, 21015.0, 21005.0, 21008.0),
        # Bars 2-4: separate (close = 21030 = 120 ticks above)
        _frame(_ts(2), 21028.0, 21035.0, 21025.0, 21030.0),
        _frame(_ts(3), 21030.0, 21038.0, 21028.0, 21032.0),
        _frame(_ts(4), 21032.0, 21040.0, 21030.0, 21035.0),
    ]
    summaries, bars = _run_frames(frames)

    assert len(summaries) == 1
    s = summaries[0]
    assert s.end_reason == "separation"
    assert s.interaction_index == 1
    assert s.prior_episode_id is None
    assert s.is_valid_for_research is True
    assert s.bar_count >= 1
    assert s.level_type == "vwap"


# ── Test 2: Noisy crossings stay one episode ──────────────────────────────────

def test_noisy_crossings_one_episode():
    """Multiple bars spanning the level (range_spans_level=True) stay in one episode."""
    frames = [
        # Enter proximity
        _frame(_ts(0), 21005.0, 21010.0, 20998.0, 21002.0),
        # Cross above
        _frame(_ts(1), 21002.0, 21012.0, 20998.0, 21008.0),
        # Cross below (range_spans_level True on multiple bars)
        _frame(_ts(2), 21008.0, 21012.0, 20992.0, 20998.0),
        # Cross above again
        _frame(_ts(3), 20998.0, 21010.0, 20995.0, 21005.0),
        # Cross below again
        _frame(_ts(4), 21005.0, 21008.0, 20994.0, 20997.0),
        # Still near level
        _frame(_ts(5), 20997.0, 21003.0, 20995.0, 21001.0),
        # Finally separate upward (3 bars)
        _frame(_ts(6), 21001.0, 21035.0, 21000.0, 21032.0),
        _frame(_ts(7), 21032.0, 21040.0, 21028.0, 21036.0),
        _frame(_ts(8), 21036.0, 21042.0, 21032.0, 21038.0),
    ]
    summaries, bars = _run_frames(frames)

    assert len(summaries) == 1
    s = summaries[0]
    assert s.cross_count >= 3
    assert s.close_side_flip_count >= 2
    assert s.end_reason == "separation"


# ── Test 3: Separation counter resets on return ───────────────────────────────

def test_separation_reset_on_return():
    """Price nearly separates, briefly returns, counter resets — episode continues."""
    # separation_ticks = 100 ticks = 25 points
    # min_separation_bars = 3

    frames = [
        # Enter proximity
        _frame(_ts(0), 21005.0, 21010.0, 20998.0, 21003.0),
        # Start separating upward
        _frame(_ts(1), 21003.0, 21030.0, 21000.0, 21028.0),  # 112 ticks above — sep bar 1
        _frame(_ts(2), 21028.0, 21035.0, 21025.0, 21030.0),  # sep bar 2
        # Return to proximity — RESETS counter
        _frame(_ts(3), 21030.0, 21032.0, 20998.0, 21004.0),  # back within 40 ticks
        # Separate again — counter restarts from 0
        _frame(_ts(4), 21004.0, 21035.0, 21001.0, 21030.0),  # sep bar 1
        _frame(_ts(5), 21030.0, 21040.0, 21028.0, 21032.0),  # sep bar 2
        _frame(_ts(6), 21032.0, 21042.0, 21030.0, 21035.0),  # sep bar 3 → closes
    ]
    summaries, bars = _run_frames(frames)

    assert len(summaries) == 1
    s = summaries[0]
    assert s.end_reason == "separation"
    assert s.bar_count >= 6  # episode ran long due to reset


# ── Test 4: Separation then re-approach creates second episode ────────────────

def test_second_episode_on_reapproach():
    """After clean separation, a re-approach creates episode #2 with prior_episode_id link."""
    frames = [
        # Episode 1: enter, stay briefly, separate
        _frame(_ts(0), 21005.0, 21010.0, 20998.0, 21003.0),
        _frame(_ts(1), 21003.0, 21035.0, 21000.0, 21030.0),
        _frame(_ts(2), 21030.0, 21038.0, 21028.0, 21032.0),
        _frame(_ts(3), 21032.0, 21040.0, 21030.0, 21035.0),
        # Price far away
        _frame(_ts(4), 21035.0, 21060.0, 21035.0, 21055.0),
        _frame(_ts(5), 21055.0, 21065.0, 21050.0, 21060.0),
        # Re-approach (episode 2 starts here)
        _frame(_ts(6), 21060.0, 21065.0, 21002.0, 21005.0),
        _frame(_ts(7), 21005.0, 21012.0, 20998.0, 21003.0),
    ]
    summaries, bars = _run_frames(frames)

    # Should have at least episode 1 completed; episode 2 may be open (flushed as session_ended)
    assert len(summaries) >= 2

    ep1 = next(s for s in summaries if s.interaction_index == 1)
    ep2 = next(s for s in summaries if s.interaction_index == 2)

    assert ep1.prior_episode_id is None
    assert ep2.prior_episode_id == ep1.episode_id
    assert ep2.end_reason in ("separation", "timeout", "session_ended")


# ── Test 5: Dynamic VWAP level_id stays stable ───────────────────────────────

def test_dynamic_vwap_level_id_stable():
    """VWAP value changes each bar but level_id stays constant — no phantom episode reopen."""
    # All bars near level, VWAP drifts slightly each bar
    frames = [
        _frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0, level_value=21000.00),
        _frame(_ts(1), 21002.0, 21007.0, 20996.0, 21001.0, level_value=21000.25),  # VWAP moved
        _frame(_ts(2), 21001.0, 21006.0, 20995.0, 21000.0, level_value=21000.50),
        _frame(_ts(3), 21000.0, 21005.0, 20994.0, 20999.0, level_value=21000.75),
        # Separate
        _frame(_ts(4), 20999.0, 21000.0, 20970.0, 20972.0, level_value=21001.00),
        _frame(_ts(5), 20972.0, 20975.0, 20968.0, 20970.0, level_value=21001.25),
        _frame(_ts(6), 20970.0, 20973.0, 20966.0, 20968.0, level_value=21001.50),
    ]
    summaries, bars = _run_frames(frames)

    assert len(summaries) == 1, "VWAP drift must not create multiple episodes"
    s = summaries[0]
    assert s.level_id == f"vwap:{SYMBOL}:{SESSION_ID}"


# ── Test 6: Timeout ───────────────────────────────────────────────────────────

def test_timeout():
    """Episode times out after max_episode_bars."""
    cfg = _cfg(max_episode_bars=5)

    def on_complete(s, bars):
        completions.append((s, bars))

    completions = []
    mgr = InteractionEpisodeManager(config=cfg, on_episode_complete=on_complete)

    # Enter proximity then stay inside
    for i in range(7):
        f = _frame(_ts(i), 21003.0, 21008.0, 20997.0, 21002.0)
        mgr.process(f)

    mgr.flush()

    assert len(completions) >= 1
    timed_out = next((s for s, _ in completions if s.end_reason == "timeout"), None)
    assert timed_out is not None
    assert timed_out.bar_count == 5


# ── Test 7: Session end ───────────────────────────────────────────────────────

def test_session_end():
    """on_session_end closes active episode with session_ended reason."""
    completions = []

    def on_complete(s, bars):
        completions.append(s)

    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=on_complete)

    # Open an episode
    mgr.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))

    # Trigger session end
    mgr.on_session_end(SYMBOL, SESSION_ID, _ts(1))

    assert len(completions) == 1
    assert completions[0].end_reason == "session_ended"
    assert completions[0].ended_at == _ts(1)


# ── Test 8: Gap detection ─────────────────────────────────────────────────────

def test_gap_invalidates_episode():
    """Gap detection closes active episode as gap_detected + is_valid_for_research=False."""
    completions = []

    def on_complete(s, bars):
        completions.append(s)

    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=on_complete)

    # Open an episode
    mgr.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))

    # Trigger gap
    mgr.on_gap(SYMBOL, SESSION_ID, _ts(5))

    assert len(completions) == 1
    s = completions[0]
    assert s.end_reason == "gap_detected"
    assert s.is_valid_for_research is False


# ── Test 9: Deterministic replay ─────────────────────────────────────────────

def test_deterministic_replay():
    """Same input → identical episode boundaries on two independent runs."""
    frames = [
        _frame(_ts(0), 21075.0, 21080.0, 21070.0, 21075.0),
        _frame(_ts(1), 21010.0, 21015.0, 21005.0, 21008.0),
        _frame(_ts(2), 21005.0, 21012.0, 20998.0, 21001.0),
        _frame(_ts(3), 21001.0, 21008.0, 20996.0, 21003.0),
        _frame(_ts(4), 21003.0, 21035.0, 21000.0, 21030.0),
        _frame(_ts(5), 21030.0, 21038.0, 21028.0, 21032.0),
        _frame(_ts(6), 21032.0, 21040.0, 21030.0, 21034.0),
    ]

    summaries_1, bars_1 = _run_frames(frames)
    summaries_2, bars_2 = _run_frames(frames)

    assert len(summaries_1) == len(summaries_2)
    for s1, s2 in zip(summaries_1, summaries_2):
        assert s1.started_at == s2.started_at
        assert s1.ended_at == s2.ended_at
        assert s1.bar_count == s2.bar_count
        assert s1.cross_count == s2.cross_count
        assert s1.end_reason == s2.end_reason
        assert s1.interaction_index == s2.interaction_index
        assert s1.policy_config_hash == s2.policy_config_hash
        assert s1.geometry_version == s2.geometry_version

    # Bar-level sequences must be identical
    assert len(bars_1) == len(bars_2)
    for b1, b2 in zip(bars_1, bars_2):
        assert b1.bar_seq == b2.bar_seq
        assert b1.bar_timestamp == b2.bar_timestamp
        assert b1.close_side == b2.close_side
        assert b1.close_distance_ticks == b2.close_distance_ticks
