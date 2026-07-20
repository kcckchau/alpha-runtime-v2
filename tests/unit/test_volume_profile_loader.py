"""
Unit tests for VolumeProfileLoader.

Uses a temporary directory with synthetic JSON files so no real data is needed.
"""

import json
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from alpha.features.volume_profile_loader import VolumeProfileLoader
from alpha.models.volume_profile import VolumeProfile


_SYMBOL = "MNQ-09"


def _make_profile_json(
    session_date: date,
    session_type: str,
    poc: str = "100.0",
    vah: str = "105.0",
    val: str = "95.0",
) -> dict:
    return {
        "symbol": _SYMBOL,
        "session_date": str(session_date),
        "session_type": session_type,
        "bin_size": 1.0,
        "source": "bars",
        "poc": poc,
        "vah": vah,
        "val": val,
        "total_volume": 1000,
        "value_area_volume": 700,
        "hvn_levels": ["102.0", "98.0"],
        "lvn_levels": ["97.0"],
        "distribution": {"95.0": 100, "100.0": 500, "105.0": 400},
        "delta_distribution": None,
    }


def _write_profile(profiles_dir: Path, symbol: str, d: date, session_type: str, **kwargs) -> None:
    path = profiles_dir / symbol / f"{d}_{session_type}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(_make_profile_json(d, session_type, **kwargs), f)


# ── load_prior_rth ────────────────────────────────────────────────────────────

def test_load_prior_rth_returns_most_recent():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 3)
        prior = date(2026, 7, 2)
        _write_profile(d, _SYMBOL, prior, "rth", poc="200.0")
        loader = VolumeProfileLoader(d)
        p = loader.load_prior_rth(_SYMBOL, session_date)
        assert p is not None
        assert p.poc == Decimal("200.0")
        assert p.session_date == prior


def test_load_prior_rth_skips_missing_days():
    """Loader searches backwards past missing days to find the nearest profile."""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 7)  # Monday
        # Friday profile exists, Saturday/Sunday missing
        friday = date(2026, 7, 4)
        _write_profile(d, _SYMBOL, friday, "rth", poc="300.0")
        loader = VolumeProfileLoader(d)
        p = loader.load_prior_rth(_SYMBOL, session_date)
        assert p is not None
        assert p.poc == Decimal("300.0")
        assert p.session_date == friday


def test_load_prior_rth_returns_none_when_no_history():
    with tempfile.TemporaryDirectory() as tmp:
        loader = VolumeProfileLoader(Path(tmp))
        p = loader.load_prior_rth(_SYMBOL, date(2026, 7, 3))
        assert p is None


def test_load_prior_rth_does_not_return_same_day():
    """Profile for session_date itself must not be returned as prior."""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 3)
        _write_profile(d, _SYMBOL, session_date, "rth")   # same day — should be ignored
        loader = VolumeProfileLoader(d)
        p = loader.load_prior_rth(_SYMBOL, session_date)
        assert p is None


# ── load_globex ───────────────────────────────────────────────────────────────

def test_load_globex_returns_profile_for_session_date():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 3)
        _write_profile(d, _SYMBOL, session_date, "globex", poc="99.0")
        loader = VolumeProfileLoader(d)
        p = loader.load_globex(_SYMBOL, session_date)
        assert p is not None
        assert p.poc == Decimal("99.0")
        assert p.session_type == "globex"


def test_load_globex_returns_none_when_missing():
    with tempfile.TemporaryDirectory() as tmp:
        loader = VolumeProfileLoader(Path(tmp))
        assert loader.load_globex(_SYMBOL, date(2026, 7, 3)) is None


# ── load_session_pair ─────────────────────────────────────────────────────────

def test_load_session_pair_returns_both():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 3)
        _write_profile(d, _SYMBOL, date(2026, 7, 2), "rth", poc="150.0")
        _write_profile(d, _SYMBOL, session_date, "globex", poc="148.0")
        loader = VolumeProfileLoader(d)
        rth, globex = loader.load_session_pair(_SYMBOL, session_date)
        assert rth is not None and rth.poc == Decimal("150.0")
        assert globex is not None and globex.poc == Decimal("148.0")


def test_load_session_pair_partial_missing():
    """Returns (None, profile) or (profile, None) gracefully."""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 3)
        _write_profile(d, _SYMBOL, session_date, "globex")
        loader = VolumeProfileLoader(d)
        rth, globex = loader.load_session_pair(_SYMBOL, session_date)
        assert rth is None
        assert globex is not None


# ── Deserialization ───────────────────────────────────────────────────────────

def test_loaded_profile_fields_are_correct_types():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 3)
        _write_profile(d, _SYMBOL, date(2026, 7, 2), "rth")
        loader = VolumeProfileLoader(d)
        p = loader.load_prior_rth(_SYMBOL, session_date)
        assert isinstance(p.poc, Decimal)
        assert isinstance(p.vah, Decimal)
        assert isinstance(p.val, Decimal)
        assert all(isinstance(x, Decimal) for x in p.hvn_levels)
        assert all(isinstance(x, Decimal) for x in p.lvn_levels)
        assert isinstance(p.total_volume, int)


def test_loaded_profile_with_delta_distribution():
    """delta_distribution round-trips through JSON correctly."""
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        session_date = date(2026, 7, 3)
        profile_data = _make_profile_json(date(2026, 7, 2), "rth")
        profile_data["source"] = "trades"
        profile_data["delta_distribution"] = {"95.0": -50, "100.0": 200, "105.0": -100}
        path = d / _SYMBOL / f"{date(2026, 7, 2)}_rth.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(profile_data, f)

        loader = VolumeProfileLoader(d)
        p = loader.load_prior_rth(_SYMBOL, session_date)
        assert p.source == "trades"
        assert p.delta_distribution == {"95.0": -50, "100.0": 200, "105.0": -100}
