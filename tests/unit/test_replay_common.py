"""
Unit tests for scripts/replay_common.py's provenance functions, and for the
meta/fingerprint/config layers backtest.py and replay_day.py's save paths
persist into their output files.

Covers the reproducibility-correctness fix: config fingerprint + resolved
config must be embedded in saved results (not just printed to console), and
replay_cache.py's ReplayResultSaver must not crash on stale BarSnapshot/
MarketState attribute references (ema_20, orb_state).
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

_REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts"))

from alpha.config.settings import AlphaSettings
from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, SessionPhase
from alpha.models.market_state import MarketState
from alpha.models.snapshot import BarSnapshot

from replay_common import (
    build_config_fingerprint,
    build_resolved_config,
    config_hash,
    dataset_manifest,
)
from alpha.calendar.resolver import calendar_for_symbol
from alpha.models.enums import AssetClass
from alpha.models.symbol import Symbol

SYM = "MNQ-09"


def _ts(offset_minutes: int = 0) -> datetime:
    return datetime(2026, 7, 24, 14, 30, tzinfo=timezone.utc) + timedelta(minutes=offset_minutes)


def _snapshot() -> BarSnapshot:
    close = Decimal("28500.00")
    bar = Bar(
        symbol=SYM, timestamp=_ts(), timeframe=BarTimeframe.M1,
        open=close, high=close + Decimal("5"), low=close - Decimal("5"),
        close=close, volume=1000,
    )
    return BarSnapshot(
        symbol=SYM, timestamp=_ts(), timeframe=BarTimeframe.M1, bar=bar,
        vwap=close, is_above_vwap=True,
        atr_14=Decimal("8.0"), session_phase=SessionPhase.EARLY,
        or_established=False,
    )


def _market_state() -> MarketState:
    return MarketState(symbol=SYM, timestamp=_ts())


# ── build_config_fingerprint ────────────────────────────────────────────────

def test_config_fingerprint_available_has_expected_shape():
    fp = build_config_fingerprint()
    assert fp["available"] is True
    assert isinstance(fp["git_commit"], str) and len(fp["git_commit"]) > 0
    assert isinstance(fp["git_dirty"], bool)
    assert set(fp["policy_versions"]) == {"slope", "ema_1h_ribbon", "ema_1h_slope"}
    assert fp["fingerprint_generated_at"]  # ISO timestamp string


def test_config_fingerprint_graceful_on_git_failure():
    """A broken git call must degrade to available=False, never raise."""
    with patch("replay_common.subprocess.run", side_effect=FileNotFoundError("no git")):
        fp = build_config_fingerprint()
    assert fp["available"] is False
    assert fp["git_commit"] is None
    assert fp["git_dirty"] is None
    assert fp["dirty_trading_logic_files"] is None
    # Policy versions are real code constants, unrelated to git — still present.
    assert fp["policy_versions"]["slope"] == "norm3_v1"


# ── build_resolved_config / redaction ───────────────────────────────────────

def test_resolved_config_redacts_secrets():
    settings = AlphaSettings()
    rc = build_resolved_config(settings, {"symbol": SYM})
    dumped = json.dumps(rc)
    assert "***REDACTED***" in dumped
    # The actual default secret values must never appear, redacted or not.
    for leaked in ("alpha_dev",):
        assert leaked not in dumped


def test_resolved_config_preserves_cli_args():
    settings = AlphaSettings()
    cli_args = {"symbol": SYM, "start_date": "2026-07-20", "warmup_days": 3}
    rc = build_resolved_config(settings, cli_args)
    assert rc["cli_args"] == cli_args


# ── config_hash ──────────────────────────────────────────────────────────────

def test_config_hash_deterministic_and_sensitive_to_changes():
    settings = AlphaSettings()
    rc_a = build_resolved_config(settings, {"symbol": SYM, "min_grade": "A"})
    rc_a2 = build_resolved_config(settings, {"symbol": SYM, "min_grade": "A"})
    rc_b = build_resolved_config(settings, {"symbol": SYM, "min_grade": "B"})

    assert config_hash(rc_a) == config_hash(rc_a2)
    assert config_hash(rc_a) != config_hash(rc_b)


# ── backtest.py: summary.json includes fingerprint/config ──────────────────

def test_backtest_save_writes_fingerprint_and_config(tmp_path, monkeypatch):
    import backtest as backtest_mod

    monkeypatch.setattr(backtest_mod, "_REPO", tmp_path)

    signal = backtest_mod.SignalRecord(
        signal_id="s1", date="2026-07-24", symbol=SYM,
        entry_bar_ts="2026-07-24T14:30:00Z", setup_type="hod_breakout", grade="A",
        direction="buy", entry_price=100.0, stop=95.0, target=110.0,
        risk_pts=5.0, reward_pts=10.0, rr_setup=2.0,
        exit_bar_ts="2026-07-24T14:40:00Z", exit_price=95.0, exit_reason="stop_hit",
        pnl_pts=-5.0, hold_bars=10, outcome="loss",
    )
    settings = AlphaSettings()
    cli_args = {"symbol": SYM, "min_grade": "A"}
    manifest = {
        "source": "parquet", "symbol": SYM,
        "coverage": {"start": "2026-07-20", "end": "2026-07-24"},
        "expected_trading_days": 5, "trading_days_with_bars": 5, "missing_days": [],
    }

    out_dir = backtest_mod._save(
        [signal], {"total_pnl_pts": -5.0}, SYM, "2026-07-20", "2026-07-24",
        {"symbol": SYM, "generated_at": "2026-07-26T00:00:00Z"},
        settings, cli_args, manifest,
    )

    summary = json.loads((out_dir / "summary.json").read_text())
    assert set(summary) == {"meta", "fingerprint", "config", "config_hash", "dataset", "stats"}
    assert summary["fingerprint"]["policy_versions"]["slope"] == "norm3_v1"
    assert summary["dataset"] == manifest
    assert summary["config"]["cli_args"] == cli_args
    assert summary["config_hash"] == config_hash(summary["config"])


# ── replay_cache.py: ReplayResultSaver doesn't crash on save ───────────────

def test_replay_result_saver_full_path_no_crash(tmp_path):
    """
    Regression test for the ema_20/orb_state stale-attribute crashes: feeds a
    real (synthetic) BarSnapshot/MarketState through record_bar() + save() and
    confirms it completes and embeds fingerprint/config/config_hash.
    """
    from replay_cache import ReplayResultSaver

    saver = ReplayResultSaver(tmp_path, SYM, date(2026, 7, 24))
    snap = _snapshot()
    ms = _market_state()
    bar = snap.bar

    # record_bar() signature: (bar, snap, thesis, active_setups, market_state, prev_thesis_type, prev_thesis_state)
    saver.record_bar(bar, snap, None, [], ms, None, None)

    settings = AlphaSettings()
    resolved_config = build_resolved_config(settings, {"symbol": SYM})
    json_path, csv_path = saver.save(
        meta={"symbol": SYM, "session_date": "2026-07-24"},
        fingerprint=build_config_fingerprint(),
        config=resolved_config,
        config_hash_value=config_hash(resolved_config),
        full_signals=False,
        thesis_final=None,
        snap_final=snap,
        active_setups_at_close=[],
    )

    assert json_path.exists()
    result = json.loads(json_path.read_text())
    assert result["fingerprint"]["available"] in (True, False)
    assert result["config"]["cli_args"] == {"symbol": SYM}
    assert result["config_hash"] == config_hash(resolved_config)
    # The crash-fixed fields: ema20 duplicates ema21 (no such field on
    # BarSnapshot), orb_state maps to or_position (renamed 2026-07-18).
    bar_record = result["bars"][0]
    assert bar_record["ema20"] == bar_record["ema21"]
    assert bar_record["market_state"]["orb_state"] is None  # or_established=False


# ── dataset_manifest ─────────────────────────────────────────────────────────

def test_dataset_manifest_detects_missing_days():
    """
    Regression test for the silent-gap risk: load_m1_bars(skip_read_errors=True)
    continues past a missing day with no error, so dataset_manifest must be able
    to independently flag it from the calendar, not depend on the loader to
    have complained.
    """
    from types import SimpleNamespace

    sym_obj = Symbol(
        ticker=SYM, exchange="CME", asset_class=AssetClass.FUTURE,
        root_symbol="MNQ", lot_size=1,
        tick_size=Decimal("0.25"), point_value=Decimal("2.0"),
    )
    calendar = calendar_for_symbol(sym_obj)

    start = date(2026, 7, 20)  # Monday
    end = date(2026, 7, 24)    # Friday — 5 trading days expected
    # Bars only for Mon/Tue/Fri — Wed/Thu simulate a silently-missing gap.
    present_days = [date(2026, 7, 20), date(2026, 7, 21), date(2026, 7, 24)]
    bars = [
        SimpleNamespace(timestamp=calendar.session_open(d))
        for d in present_days
    ]

    manifest = dataset_manifest(bars, calendar, start, end, SYM)

    assert manifest["expected_trading_days"] == 5
    assert manifest["trading_days_with_bars"] == 3
    assert manifest["missing_days"] == ["2026-07-22", "2026-07-23"]


def test_dataset_manifest_no_gaps_when_fully_covered():
    sym_obj = Symbol(
        ticker=SYM, exchange="CME", asset_class=AssetClass.FUTURE,
        root_symbol="MNQ", lot_size=1,
        tick_size=Decimal("0.25"), point_value=Decimal("2.0"),
    )
    calendar = calendar_for_symbol(sym_obj)
    start = date(2026, 7, 20)
    end = date(2026, 7, 21)

    from types import SimpleNamespace
    bars = [
        SimpleNamespace(timestamp=calendar.session_open(d))
        for d in calendar.trading_days(start, end)
    ]
    manifest = dataset_manifest(bars, calendar, start, end, SYM)
    assert manifest["missing_days"] == []
    assert manifest["trading_days_with_bars"] == manifest["expected_trading_days"]
