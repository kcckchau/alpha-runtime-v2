"""
Unit tests for scripts/analyze_setup_overlap.py's pure clustering logic.

Documents a known limitation found while verifying this against real data
(2024-07-08..12): exact-overlap greedy merging chains transitively — setup A
overlapping B and B overlapping C merges all three even if A and C never
overlap directly — which produced a 33-setup, 3.5-hour "opportunity" on an
extended trend day. These tests pin down the current (imperfect) behavior so
a future change to the merge criterion is a deliberate decision, not a
silent regression.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

_REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts"))

from analyze_setup_overlap import _direction_for, cluster_opportunities


def _lifecycle_row(setup_id: str, setup_type: str, detected_min: int, resolved_min: int) -> dict:
    base = datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc)
    return {
        "setup_id": setup_id,
        "setup_type": setup_type,
        "detected_at": base + timedelta(minutes=detected_min),
        "resolved_at": base + timedelta(minutes=resolved_min),
        "final_state": "triggered",
        "entry_trigger": None,
        "stop_reference": None,
        "target_reference": None,
        "grade": None,
        "score": None,
        "is_replay": True,
        "session_date": "2024-01-02",
    }


def test_non_overlapping_setups_stay_separate():
    df = pd.DataFrame([
        _lifecycle_row("a", "trend_pullback", 0, 5),
        _lifecycle_row("b", "hod_breakout", 20, 25),
    ])
    clustered = cluster_opportunities(df)
    assert clustered.set_index("setup_id")["opportunity_id"].nunique() == 2


def test_overlapping_setups_merge_into_one_opportunity():
    df = pd.DataFrame([
        _lifecycle_row("a", "vwap_undercut_reclaim", 0, 10),
        _lifecycle_row("b", "fake_breakdown", 5, 15),
    ])
    clustered = cluster_opportunities(df)
    assert clustered["opportunity_id"].nunique() == 1


def test_transitive_chaining_merges_non_overlapping_endpoints():
    """
    Documents the known limitation, not a desired property: A overlaps B,
    B overlaps C, but A and C's windows never touch — yet all three land in
    one opportunity because the merge only checks against the *running*
    cluster end, not each individual prior member.
    """
    df = pd.DataFrame([
        _lifecycle_row("a", "trend_pullback", 0, 10),
        _lifecycle_row("b", "trend_pullback", 8, 20),
        _lifecycle_row("c", "trend_pullback_short", 18, 30),
    ])
    clustered = cluster_opportunities(df)
    assert clustered["opportunity_id"].nunique() == 1
    a_window = (df.loc[df["setup_id"] == "a", "detected_at"].iloc[0],
                df.loc[df["setup_id"] == "a", "resolved_at"].iloc[0])
    c_window = (df.loc[df["setup_id"] == "c", "detected_at"].iloc[0],
                df.loc[df["setup_id"] == "c", "resolved_at"].iloc[0])
    assert a_window[1] < c_window[0]  # a and c genuinely don't overlap


def test_direction_for_known_short_types():
    assert _direction_for("trend_pullback_short") == "short"
    assert _direction_for("vwap_failed_reclaim_short") == "short"
    assert _direction_for("orb_breakdown") == "short"
    assert _direction_for("vwap_rejection") == "short"


def test_direction_for_long_types():
    assert _direction_for("trend_pullback") == "long"
    assert _direction_for("hod_breakout") == "long"
    assert _direction_for("fake_breakdown") == "long"
