"""
Unit tests for BarSnapshot volume profile fields.

Tests the _compute_vp_fields helper in FeatureEngine in isolation — no full
engine startup needed. Verifies distance arithmetic, location classification,
and None-safety when profile or ATR is absent.
"""

from datetime import date
from decimal import Decimal

import pytest

from alpha.engines.feature.engine import FeatureEngine
from alpha.models.volume_profile import VolumeProfile


_DATE = date(2026, 7, 3)
_SYMBOL = "MNQ-09"


def _profile(
    poc: str,
    vah: str,
    val: str,
    hvn: list[str] | None = None,
    lvn: list[str] | None = None,
    source: str = "trades",
) -> VolumeProfile:
    return VolumeProfile(
        symbol=_SYMBOL,
        session_date=_DATE,
        session_type="rth",
        bin_size=1.0,
        source=source,
        poc=Decimal(poc),
        vah=Decimal(vah),
        val=Decimal(val),
        total_volume=1000,
        value_area_volume=700,
        hvn_levels=[Decimal(x) for x in (hvn or [])],
        lvn_levels=[Decimal(x) for x in (lvn or [])],
        distribution={poc: 500, vah: 300, val: 200},
    )


def _engine_with_profiles(
    rth: VolumeProfile | None = None,
    globex: VolumeProfile | None = None,
) -> FeatureEngine:
    """Construct a minimal FeatureEngine with VP profiles pre-loaded (no event bus needed)."""
    from unittest.mock import MagicMock
    settings = MagicMock()
    settings.storage.volume_profiles_root = "/tmp/nonexistent"
    engine = FeatureEngine.__new__(FeatureEngine)
    engine._vp_rth = {_SYMBOL: rth}
    engine._vp_globex = {_SYMBOL: globex}
    return engine


# ── No profile / no ATR ───────────────────────────────────────────────────────

def test_no_profile_returns_empty():
    engine = _engine_with_profiles(rth=None, globex=None)
    result = engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), Decimal("1.0"))
    assert result == {}


def test_no_atr_returns_empty():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    assert engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), None) == {}


def test_zero_atr_returns_empty():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    assert engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), Decimal("0")) == {}


# ── Distance arithmetic ───────────────────────────────────────────────────────

def test_poc_distance_above():
    """Close above POC → positive distance."""
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("102.0"), Decimal("2.0"))
    assert result["vp_poc_distance_atr"] == pytest.approx(1.0)   # (102 - 100) / 2


def test_poc_distance_below():
    """Close below POC → negative distance."""
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("97.0"), Decimal("2.0"))
    assert result["vp_poc_distance_atr"] == pytest.approx(-1.5)  # (97 - 100) / 2


def test_vah_val_distances():
    engine = _engine_with_profiles(rth=_profile("100.0", "110.0", "90.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), Decimal("5.0"))
    # (100 - 110) / 5 = -2.0; (100 - 90) / 5 = 2.0
    assert result["vp_vah_distance_atr"] == pytest.approx(-2.0)
    assert result["vp_val_distance_atr"] == pytest.approx(2.0)


# ── Location classification ───────────────────────────────────────────────────

def test_location_above_va():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("106.0"), Decimal("1.0"))
    assert result["vp_location"] == "above_va"


def test_location_below_va():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("94.0"), Decimal("1.0"))
    assert result["vp_location"] == "below_va"


def test_location_inside_va():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("102.0"), Decimal("1.0"))
    assert result["vp_location"] == "inside_va"


def test_location_at_poc():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), Decimal("1.0"))
    assert result["vp_location"] == "at_poc"


def test_location_at_vah_boundary():
    """Close exactly at VAH is inside VA."""
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("105.0"), Decimal("1.0"))
    assert result["vp_location"] == "inside_va"


# ── HVN / LVN nearest distance ────────────────────────────────────────────────

def test_nearest_hvn_distance():
    """HVN at 108.0, close at 110.0, ATR=2.0 → (110-108)/2 = +1.0."""
    engine = _engine_with_profiles(
        rth=_profile("100.0", "105.0", "95.0", hvn=["108.0", "92.0"])
    )
    result = engine._compute_vp_fields(_SYMBOL, Decimal("110.0"), Decimal("2.0"))
    assert result["vp_nearest_hvn_distance_atr"] == pytest.approx(1.0)


def test_nearest_lvn_none_when_no_lvns():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), Decimal("1.0"))
    assert result.get("vp_nearest_lvn_distance_atr") is None


# ── Source field ──────────────────────────────────────────────────────────────

def test_source_field_passed_through():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0", source="bars"))
    result = engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), Decimal("1.0"))
    assert result["vp_source"] == "bars"


# ── Globex fields ─────────────────────────────────────────────────────────────

def test_globex_fields_present_when_loaded():
    globex = VolumeProfile(
        symbol=_SYMBOL,
        session_date=_DATE,
        session_type="globex",
        bin_size=1.0,
        source="trades",
        poc=Decimal("99.0"),
        vah=Decimal("104.0"),
        val=Decimal("94.0"),
        total_volume=5000,
        value_area_volume=3500,
        hvn_levels=[],
        lvn_levels=[],
        distribution={"99.0": 5000},
    )
    engine = _engine_with_profiles(
        rth=_profile("100.0", "105.0", "95.0"),
        globex=globex,
    )
    result = engine._compute_vp_fields(_SYMBOL, Decimal("101.0"), Decimal("2.0"))
    assert result["vp_globex_source"] == "trades"
    assert result["vp_globex_poc_distance_atr"] == pytest.approx(1.0)  # (101-99)/2
    assert result["vp_globex_location"] == "inside_va"


def test_globex_absent_no_globex_keys():
    engine = _engine_with_profiles(rth=_profile("100.0", "105.0", "95.0"), globex=None)
    result = engine._compute_vp_fields(_SYMBOL, Decimal("100.0"), Decimal("1.0"))
    assert "vp_globex_source" not in result
    assert "vp_globex_poc_distance_atr" not in result
