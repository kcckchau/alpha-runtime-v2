"""
Tests for Phase 1 Level Interaction Engine.

Acceptance criterion: "On deterministic M1 replay, can the engine reliably keep
noisy VWAP crossings inside one episode, close only after meaningful separation,
and create a later second episode on re-approach?"

Boundary rules tested:
  - Entry uses bar-range overlap with full proximity band (not just close distance)
  - Separation requires ENTIRE bar range beyond threshold (both above/below)
  - Alternating far-above/far-below bars do NOT accumulate separation
  - flush() uses last-bar timestamp, not started_at
  - on_session_end produces valid episodes; on_gap produces invalid ones
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

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

# With default config:
#   proximity_ticks  = int(200 * 0.20) = 40  ticks = 10 pts
#   separation_ticks = int(200 * 0.50) = 100 ticks = 25 pts
#
# Entry: low_d <= 40 AND high_d >= -40
# Fully separated above: low_d > 100  (low > level + 25 pts, i.e. low > 21025)
# Fully separated below: high_d < -100 (high < level - 25 pts, i.e. high < 20975)

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


def _level(level_value: float = 21000.0, session_id: str = SESSION_ID) -> LevelSnapshot:
    session_date = session_id.split(":", 1)[1]
    return LevelSnapshot(
        level_id=f"{SYMBOL}:vwap:rth:{session_date}",
        symbol=SYMBOL,
        session_id=session_id,
        level_type="vwap",
        level_value=Decimal(str(level_value)),
        tick_size=TICK_SIZE,
        is_dynamic=True,
        sampling_note="end_of_bar_cumulative_vwap",
    )


def _frame(
    ts: datetime,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    level_value: float = 21000.0,
    atr_14: float = 50.0,
    session_id: str = SESSION_ID,
) -> InteractionFrame:
    return InteractionFrame(
        bar_timestamp=ts,
        symbol=SYMBOL,
        session_id=session_id,
        sequence_num=None,
        open=Decimal(str(open_price)),
        high=Decimal(str(high_price)),
        low=Decimal(str(low_price)),
        close=Decimal(str(close_price)),
        volume=1000,
        atr_14=Decimal(str(atr_14)),
        session_phase="mid",
        is_replay=True,
        levels=(_level(level_value, session_id),),
    )


def _ts(offset_minutes: int) -> datetime:
    return BASE_TS + timedelta(minutes=offset_minutes)


def _run_frames(
    frames: list[InteractionFrame],
    cfg: LevelDistanceConfig | None = None,
) -> tuple[list[EpisodeSummary], list[EpisodeBarRecord]]:
    summaries: list[EpisodeSummary] = []
    all_bars: list[EpisodeBarRecord] = []

    def on_complete(s: EpisodeSummary, bars: list[EpisodeBarRecord]) -> None:
        summaries.append(s)
        all_bars.extend(bars)

    mgr = InteractionEpisodeManager(
        config=cfg or _cfg(),
        on_episode_complete=on_complete,
    )
    for f in frames:
        mgr.process(f)
    mgr.flush()
    return summaries, all_bars


# ── Test 1: Quick separation ──────────────────────────────────────────────────

def test_quick_separation():
    """Price enters proximity band, stays briefly, then fully separates → 1 episode."""
    # proximity_ticks=40, separation_ticks=100
    # Need low_d > 100 (low > 21025) for full separation above

    frames = [
        # Bar 0: far above — low_d=280, not in prox band
        _frame(_ts(0), 21075.0, 21080.0, 21070.0, 21075.0),
        # Bar 1: enters prox — low_d=20, high_d=60 → (20<=40 AND 60>=-40) ✓
        _frame(_ts(1), 21010.0, 21015.0, 21005.0, 21008.0),
        # Bars 2-4: fully separated above — low_d=112 > 100 each
        _frame(_ts(2), 21028.0, 21038.0, 21028.0, 21032.0),
        _frame(_ts(3), 21032.0, 21040.0, 21028.0, 21036.0),
        _frame(_ts(4), 21036.0, 21044.0, 21028.0, 21040.0),
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
        # Enter proximity (range spans level)
        _frame(_ts(0), 21005.0, 21010.0, 20998.0, 21002.0),
        # Noisy crossings — all bars near level, not fully separated
        _frame(_ts(1), 21002.0, 21012.0, 20998.0, 21008.0),
        _frame(_ts(2), 21008.0, 21012.0, 20992.0, 20998.0),
        _frame(_ts(3), 20998.0, 21010.0, 20995.0, 21005.0),
        _frame(_ts(4), 21005.0, 21008.0, 20994.0, 20997.0),
        _frame(_ts(5), 20997.0, 21003.0, 20995.0, 21001.0),
        # Finally separate upward — 3 bars with low_d=112 > 100
        _frame(_ts(6), 21028.0, 21038.0, 21028.0, 21032.0),
        _frame(_ts(7), 21032.0, 21040.0, 21028.0, 21036.0),
        _frame(_ts(8), 21036.0, 21042.0, 21028.0, 21038.0),
    ]
    summaries, bars = _run_frames(frames)

    assert len(summaries) == 1
    s = summaries[0]
    assert s.range_span_count >= 3
    assert s.close_side_flip_count >= 2
    assert s.end_reason == "separation"


# ── Test 3: Separation counter resets on wick return ─────────────────────────

def test_separation_reset_on_return():
    """
    Two separation bars, then a wick dipping within the threshold resets the
    counter. Three more separation bars are then required to close.
    """
    # separation_ticks=100: need low_d > 100 (low > 21025)
    # Reset when low_d <= 100 (wick re-enters gap between prox and sep bands)

    frames = [
        # Enter proximity
        _frame(_ts(0), 21005.0, 21010.0, 20998.0, 21003.0),
        # Two fully separated above bars (counter = 1, 2)
        _frame(_ts(1), 21028.0, 21040.0, 21028.0, 21032.0),  # low_d=112 > 100
        _frame(_ts(2), 21032.0, 21040.0, 21028.0, 21036.0),  # low_d=112 > 100
        # Wick dips to low=21020 (low_d=80 ≤ 100) → counter RESETS to 0
        _frame(_ts(3), 21036.0, 21050.0, 21020.0, 21045.0),  # low_d=80, not fully sep
        # Three more fully separated bars (counter = 1, 2, 3 → close)
        _frame(_ts(4), 21028.0, 21040.0, 21028.0, 21032.0),
        _frame(_ts(5), 21032.0, 21042.0, 21028.0, 21036.0),
        _frame(_ts(6), 21036.0, 21045.0, 21028.0, 21040.0),
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
        # Episode 1: enter, 3 fully separated bars → closes
        _frame(_ts(0), 21005.0, 21010.0, 20998.0, 21003.0),
        _frame(_ts(1), 21028.0, 21040.0, 21028.0, 21032.0),
        _frame(_ts(2), 21032.0, 21040.0, 21028.0, 21036.0),
        _frame(_ts(3), 21036.0, 21044.0, 21028.0, 21040.0),
        # Far away — low_d=160, not entering prox
        _frame(_ts(4), 21040.0, 21060.0, 21040.0, 21055.0),
        _frame(_ts(5), 21055.0, 21065.0, 21050.0, 21060.0),
        # Re-approach: wick down to low=21002 (low_d=8 ≤ 40, high_d=260 ≥ -40) → opens ep2
        _frame(_ts(6), 21060.0, 21065.0, 21002.0, 21005.0),
        _frame(_ts(7), 21005.0, 21012.0, 20998.0, 21003.0),
    ]
    summaries, bars = _run_frames(frames)

    assert len(summaries) >= 2

    ep1 = next(s for s in summaries if s.interaction_index == 1)
    ep2 = next(s for s in summaries if s.interaction_index == 2)

    assert ep1.prior_episode_id is None
    assert ep2.prior_episode_id == ep1.episode_id
    assert ep2.end_reason in ("separation", "timeout", "session_ended", "replay_completed")


# ── Test 5: Dynamic VWAP level_id stays stable ───────────────────────────────

def test_dynamic_vwap_level_id_stable():
    """VWAP value changes each bar but level_id stays constant — no phantom episode reopen."""
    frames = [
        _frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0, level_value=21000.00),
        _frame(_ts(1), 21002.0, 21007.0, 20996.0, 21001.0, level_value=21000.25),
        _frame(_ts(2), 21001.0, 21006.0, 20995.0, 21000.0, level_value=21000.50),
        _frame(_ts(3), 21000.0, 21005.0, 20994.0, 20999.0, level_value=21000.75),
        # Separate below — need high_d < -100 (high < level - 25 = 20975)
        _frame(_ts(4), 20999.0, 21000.0, 20970.0, 20972.0, level_value=21001.00),  # high_d=-4, NOT sep
        _frame(_ts(5), 20972.0, 20974.0, 20960.0, 20968.0, level_value=21001.25),  # high_d=-109 < -100 ✓
        _frame(_ts(6), 20968.0, 20972.0, 20956.0, 20965.0, level_value=21001.50),  # high_d=-117 < -100 ✓
        _frame(_ts(7), 20965.0, 20970.0, 20952.0, 20960.0, level_value=21001.75),  # high_d=-127 < -100 ✓
    ]
    summaries, bars = _run_frames(frames)

    assert len(summaries) == 1, "VWAP drift must not create multiple episodes"
    s = summaries[0]
    assert s.level_id == f"{SYMBOL}:vwap:rth:{SESSION_DATE}"


# ── Test 6: Timeout ───────────────────────────────────────────────────────────

def test_timeout():
    """Episode times out after max_episode_bars."""
    cfg = _cfg(max_episode_bars=5)

    completions = []

    def on_complete(s, bars):
        completions.append((s, bars))

    mgr = InteractionEpisodeManager(config=cfg, on_episode_complete=on_complete)

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
    """on_session_end closes active episode with 'session_ended' and is_valid=True."""
    completions = []

    def on_complete(s, bars):
        completions.append(s)

    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=on_complete)

    mgr.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))
    mgr.on_session_end(SYMBOL, SESSION_ID, _ts(1))

    assert len(completions) == 1
    s = completions[0]
    assert s.end_reason == "session_ended"
    assert s.ended_at == _ts(1)
    assert s.is_valid_for_research is True


# ── Test 8: Gap detection ─────────────────────────────────────────────────────

def test_gap_invalidates_episode():
    """on_gap closes active episode as 'gap_detected' + is_valid_for_research=False."""
    completions = []

    def on_complete(s, bars):
        completions.append(s)

    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=on_complete)

    mgr.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))
    mgr.on_gap(SYMBOL, SESSION_ID, _ts(5))

    assert len(completions) == 1
    s = completions[0]
    assert s.end_reason == "gap_detected"
    assert s.is_valid_for_research is False


# ── Test 9: Deterministic replay ─────────────────────────────────────────────

def test_deterministic_replay():
    """Same input → identical episode boundaries on two independent runs."""
    frames = [
        _frame(_ts(0), 21075.0, 21080.0, 21070.0, 21075.0),   # far above
        _frame(_ts(1), 21010.0, 21015.0, 21005.0, 21008.0),   # enters prox
        _frame(_ts(2), 21005.0, 21012.0, 20998.0, 21001.0),   # stays
        _frame(_ts(3), 21028.0, 21040.0, 21028.0, 21032.0),   # sep1
        _frame(_ts(4), 21032.0, 21040.0, 21028.0, 21036.0),   # sep2
        _frame(_ts(5), 21036.0, 21044.0, 21028.0, 21040.0),   # sep3 → close
    ]

    summaries_1, bars_1 = _run_frames(frames)
    summaries_2, bars_2 = _run_frames(frames)

    assert len(summaries_1) == len(summaries_2)
    for s1, s2 in zip(summaries_1, summaries_2):
        assert s1.started_at == s2.started_at
        assert s1.ended_at == s2.ended_at
        assert s1.bar_count == s2.bar_count
        assert s1.range_span_count == s2.range_span_count
        assert s1.end_reason == s2.end_reason
        assert s1.interaction_index == s2.interaction_index
        assert s1.policy_config_hash == s2.policy_config_hash
        assert s1.geometry_version == s2.geometry_version

    assert len(bars_1) == len(bars_2)
    for b1, b2 in zip(bars_1, bars_2):
        assert b1.bar_seq == b2.bar_seq
        assert b1.bar_timestamp == b2.bar_timestamp
        assert b1.close_side == b2.close_side
        assert b1.close_distance_ticks == b2.close_distance_ticks


# ══════════════════════════════════════════════════════════════════════════════
# Regression tests for previously-missed boundary rules
# ══════════════════════════════════════════════════════════════════════════════

# ── R1: Wick reaches proximity zone during separation resets counter ───────────

def test_wick_reaches_proximity_resets_separation_counter():
    """
    Two fully-separated bars (counter=2), then a bar whose low wick dips inside
    the separation threshold — even though close is far — resets the counter.
    Three more consecutive separation bars are then required to close.
    """
    # separation_ticks=100; wick reset: low_d=80 <= 100

    completions = []
    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=lambda s, b: completions.append(s))

    # Open episode
    mgr.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))

    # Two fully separated above bars
    mgr.process(_frame(_ts(1), 21028.0, 21040.0, 21028.0, 21032.0))  # low_d=112 > 100 → counter=1
    mgr.process(_frame(_ts(2), 21032.0, 21042.0, 21028.0, 21036.0))  # counter=2

    # Bar 3: close=21050 (far), but low wick dips to 21020 (low_d=80 ≤ 100)
    # → NOT fully separated above → counter RESETS to 0
    mgr.process(_frame(_ts(3), 21036.0, 21055.0, 21020.0, 21050.0))

    # Only 2 bars since reset — episode should NOT close yet
    mgr.process(_frame(_ts(4), 21028.0, 21040.0, 21028.0, 21032.0))  # counter=1
    mgr.process(_frame(_ts(5), 21032.0, 21042.0, 21028.0, 21036.0))  # counter=2
    assert len(completions) == 0  # still open

    # Third separation bar → closes
    mgr.process(_frame(_ts(6), 21036.0, 21045.0, 21028.0, 21040.0))  # counter=3 → close
    assert len(completions) == 1
    assert completions[0].end_reason == "separation"
    assert completions[0].bar_count == 7


# ── R2: Alternating far-above / far-below bars do NOT accumulate separation ───

def test_alternating_separation_direction_resets_counter():
    """
    Alternating fully-separated-above then fully-separated-below bars never
    accumulate toward the min_separation_bars threshold. Each direction change
    resets the counter to 1 (for the new direction), not 0.
    """
    completions = []
    mgr = InteractionEpisodeManager(config=_cfg(min_separation_bars=3),
                                    on_episode_complete=lambda s, b: completions.append(s))

    # Open episode
    mgr.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))

    # Alt: above → below → above (each resets to 1 in the new direction)
    mgr.process(_frame(_ts(1), 21028.0, 21040.0, 21028.0, 21035.0))  # above, counter=1
    mgr.process(_frame(_ts(2), 20960.0, 20970.0, 20955.0, 20960.0))  # below, counter=1
    mgr.process(_frame(_ts(3), 21028.0, 21040.0, 21028.0, 21035.0))  # above, counter=1

    # No 3 consecutive same-direction bars → episode NOT closed
    assert len(completions) == 0

    # Flush closes with replay_completed, not separation
    mgr.flush()
    assert len(completions) == 1
    assert completions[0].end_reason == "replay_completed"


# ── R3: Proximity-band overlap without exact level touch ──────────────────────

def test_proximity_band_overlap_without_level_touch():
    """
    A bar whose range overlaps the proximity band but does NOT span the exact
    level (range_spans_level=False) and has a far close still opens an episode.

    Old entry rule (abs(close_d) <= prox OR range_spans_level) would miss this.
    New rule (low_d <= prox AND high_d >= -prox) catches it.
    """
    completions = []
    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=lambda s, b: completions.append(s))

    # Bar 0: far above — low_d=280 > 40, does NOT enter prox
    mgr.process(_frame(_ts(0), 21075.0, 21080.0, 21070.0, 21075.0))
    assert len(completions) == 0

    # Bar 1: low wick enters prox band (low=21005, low_d=20 ≤ 40), close is far (21060, close_d=240)
    # range_spans_level = (low_d=20 ≤ 0 ≤ high_d=240) = FALSE — bar stays above level
    # close_d=240 > prox=40 — OLD logic would NOT open episode here
    # NEW logic: (20 ≤ 40 AND 240 ≥ -40) = TRUE → opens episode
    mgr.process(_frame(_ts(1), 21050.0, 21060.0, 21005.0, 21060.0))

    # Episode is now open at _ts(1); separate with 3 bars
    mgr.process(_frame(_ts(2), 21028.0, 21040.0, 21028.0, 21032.0))
    mgr.process(_frame(_ts(3), 21032.0, 21040.0, 21028.0, 21036.0))
    mgr.process(_frame(_ts(4), 21036.0, 21044.0, 21028.0, 21040.0))

    assert len(completions) == 1
    s = completions[0]
    assert s.started_at == _ts(1)       # opened on the wick bar, not bar 0
    assert s.interaction_index == 1


# ── R4: flush() uses last-bar timestamp, not started_at ──────────────────────

def test_flush_uses_last_bar_timestamp():
    """
    flush() must set ended_at to the last processed bar's timestamp so that
    episode duration is non-zero and meaningful for analysis.
    """
    completions = []
    mgr = InteractionEpisodeManager(config=_cfg(), on_episode_complete=lambda s, b: completions.append(s))

    ts_open = _ts(0)
    ts_last = _ts(5)

    # Open episode at ts(0)
    mgr.process(_frame(ts_open, 21003.0, 21008.0, 20997.0, 21002.0))
    # Five more bars inside episode (bars 1-5)
    for i in range(1, 6):
        mgr.process(_frame(_ts(i), 21003.0, 21008.0, 20997.0, 21002.0))

    # Flush simulates engine shutdown
    mgr.flush()

    assert len(completions) == 1
    s = completions[0]
    assert s.end_reason == "replay_completed"
    assert s.ended_at == ts_last          # last bar, NOT started_at
    assert s.ended_at != s.started_at


# ── R5: Session rollover ≠ gap — on_session_end valid, on_gap invalid ─────────

def test_session_rollover_valid_gap_invalid():
    """
    on_session_end (normal session transition) produces is_valid_for_research=True.
    on_gap (within-session missing data) produces is_valid_for_research=False.
    This distinguishes overnight breaks from genuine data outages.
    """
    # on_session_end → valid
    completions_se = []
    mgr_se = InteractionEpisodeManager(
        config=_cfg(),
        on_episode_complete=lambda s, b: completions_se.append(s),
    )
    mgr_se.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))
    mgr_se.on_session_end(SYMBOL, SESSION_ID, _ts(100))

    assert len(completions_se) == 1
    assert completions_se[0].end_reason == "session_ended"
    assert completions_se[0].is_valid_for_research is True
    assert completions_se[0].ended_at == _ts(100)

    # on_gap → invalid
    completions_gap = []
    mgr_gap = InteractionEpisodeManager(
        config=_cfg(),
        on_episode_complete=lambda s, b: completions_gap.append(s),
    )
    mgr_gap.process(_frame(_ts(0), 21003.0, 21008.0, 20997.0, 21002.0))
    mgr_gap.on_gap(SYMBOL, SESSION_ID, _ts(100))

    assert len(completions_gap) == 1
    assert completions_gap[0].end_reason == "gap_detected"
    assert completions_gap[0].is_valid_for_research is False
