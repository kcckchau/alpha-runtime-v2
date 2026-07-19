"""
Unit tests for VolumeProfileBuilder.

All tests use synthetic bars or trades with known volume distributions so
results can be verified by hand.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from alpha.features.volume_profile import VolumeProfileBuilder
from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, TakerSide
from alpha.models.trade import Trade


_DATE = date(2026, 7, 3)
_BIN = Decimal("1.0")


def _bar(low: float, high: float, volume: int, close: float | None = None) -> Bar:
    """Build a synthetic 1m bar. close defaults to high."""
    c = Decimal(str(close if close is not None else high))
    return Bar(
        symbol="MNQ-09",
        timeframe=BarTimeframe.M1,
        timestamp=datetime(2026, 7, 3, 14, 0, tzinfo=timezone.utc),
        open=Decimal(str(low)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=c,
        volume=volume,
    )


def _builder() -> VolumeProfileBuilder:
    return VolumeProfileBuilder(bin_size=_BIN)


# ── Basic construction ────────────────────────────────────────────────────────

def test_empty_bars_raises():
    with pytest.raises(ValueError, match="No bars"):
        _builder().build([], "MNQ-09", _DATE)


def test_single_flat_bar_all_volume_in_one_bin():
    """A bar with high == low spans exactly one bin."""
    bars = [_bar(100.0, 100.0, 500)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.poc == Decimal("100.0")
    assert p.total_volume == 500
    assert len(p.distribution) == 1


def test_profile_fields_populated():
    bars = [
        _bar(100.0, 102.0, 300),
        _bar(101.0, 103.0, 500),
        _bar(99.0, 101.0, 200),
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.symbol == "MNQ-09"
    assert p.session_date == _DATE
    assert p.session_type == "rth"
    assert p.bin_size == 1.0
    assert p.total_volume > 0
    assert p.poc is not None
    assert p.vah >= p.poc >= p.val


# ── POC ──────────────────────────────────────────────────────────────────────

def test_poc_is_highest_volume_bin():
    """
    Two bars:
      bar1: [100, 101), volume=1000  → 500 each to bin 100, 101
      bar2: [103, 103], volume=2000  → 2000 to bin 103

    POC must be bin 103.
    """
    bars = [
        _bar(100.0, 101.0, 1000),
        _bar(103.0, 103.0, 2000),
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.poc == Decimal("103.0")


def test_poc_in_distribution():
    bars = [_bar(100.0, 104.0, 1000), _bar(102.0, 102.0, 5000)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert str(p.poc) in p.distribution


def test_poc_tie_break_lowest_price_wins():
    """When two bins share max volume, lowest price is POC (vp_v1 policy)."""
    bars = [
        _bar(100.0, 100.0, 500),
        _bar(105.0, 105.0, 500),
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.poc == Decimal("100.0")


# ── Value Area ────────────────────────────────────────────────────────────────

def test_value_area_contains_poc():
    bars = [_bar(100.0, 110.0, 1000), _bar(104.0, 106.0, 5000)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.val <= p.poc <= p.vah


def test_value_area_volume_at_least_70_pct():
    """Value area must contain at least 70% of total volume."""
    bars = [
        _bar(100.0, 100.0, 100),
        _bar(101.0, 101.0, 100),
        _bar(102.0, 102.0, 500),   # POC
        _bar(103.0, 103.0, 100),
        _bar(104.0, 104.0, 100),
        _bar(110.0, 110.0, 100),   # outlier far from POC
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.value_area_volume >= int(p.total_volume * 0.70)


def test_vah_gte_val():
    bars = [_bar(100.0, 110.0, 1000)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.vah >= p.val


def test_single_bin_value_area_equals_poc():
    """When all volume is in one bin, VAH == VAL == POC."""
    bars = [_bar(100.0, 100.0, 1000)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.val == p.poc == p.vah


# ── HVN / LVN ─────────────────────────────────────────────────────────────────

def test_hvn_are_local_maxima():
    """
    Manually construct a distribution with a clear local maximum.
    Bins: 100=100, 101=500, 102=100, 103=800(POC), 104=100, 105=400, 106=100
    Local maxima (not POC): bin 101 (500 > 100 on both sides), bin 105 (400 > 100 on both sides)
    """
    bars = [
        _bar(100.0, 100.0, 100),
        _bar(101.0, 101.0, 500),
        _bar(102.0, 102.0, 100),
        _bar(103.0, 103.0, 800),
        _bar(104.0, 104.0, 100),
        _bar(105.0, 105.0, 400),
        _bar(106.0, 106.0, 100),
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.poc == Decimal("103.0")
    assert Decimal("101.0") in p.hvn_levels
    assert Decimal("105.0") in p.hvn_levels
    # POC not in HVN list
    assert p.poc not in p.hvn_levels


def test_hvn_ranked_by_volume_descending():
    """HVN list is ordered by volume highest first."""
    bars = [
        _bar(100.0, 100.0, 100),
        _bar(101.0, 101.0, 400),   # second HVN
        _bar(102.0, 102.0, 100),
        _bar(103.0, 103.0, 900),   # POC
        _bar(104.0, 104.0, 100),
        _bar(105.0, 105.0, 600),   # first HVN
        _bar(106.0, 106.0, 100),
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    if len(p.hvn_levels) >= 2:
        # First HVN should have more volume than second
        dist = {Decimal(k): v for k, v in p.distribution.items()}
        assert dist[p.hvn_levels[0]] >= dist[p.hvn_levels[1]]


def test_lvn_are_local_minima():
    """
    Bins: 100=500, 101=50(LVN), 102=500, 103=800(POC), 104=500, 105=30(LVN), 106=500
    """
    bars = [
        _bar(100.0, 100.0, 500),
        _bar(101.0, 101.0, 50),
        _bar(102.0, 102.0, 500),
        _bar(103.0, 103.0, 800),
        _bar(104.0, 104.0, 500),
        _bar(105.0, 105.0, 30),
        _bar(106.0, 106.0, 500),
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert Decimal("101.0") in p.lvn_levels
    assert Decimal("105.0") in p.lvn_levels


def test_lvn_ranked_by_volume_ascending():
    """LVN list is ordered lowest volume first (thinnest node first)."""
    bars = [
        _bar(100.0, 100.0, 500),
        _bar(101.0, 101.0, 80),   # second LVN
        _bar(102.0, 102.0, 500),
        _bar(103.0, 103.0, 900),  # POC
        _bar(104.0, 104.0, 500),
        _bar(105.0, 105.0, 20),   # first LVN (lower volume)
        _bar(106.0, 106.0, 500),
    ]
    p = _builder().build(bars, "MNQ-09", _DATE)
    if len(p.lvn_levels) >= 2:
        dist = {Decimal(k): v for k, v in p.distribution.items()}
        assert dist[p.lvn_levels[0]] <= dist[p.lvn_levels[1]]


def test_max_hvn_lvn_capped():
    """max_hvn and max_lvn limits are respected."""
    # Build enough alternating peaks/troughs
    volumes = [500, 50, 500, 50, 800, 50, 500, 50, 500, 50, 500]
    bars = [_bar(100.0 + i, 100.0 + i, v) for i, v in enumerate(volumes)]
    builder = VolumeProfileBuilder(bin_size=_BIN, max_hvn=2, max_lvn=2)
    p = builder.build(bars, "MNQ-09", _DATE)
    assert len(p.hvn_levels) <= 2
    assert len(p.lvn_levels) <= 2


# ── Distribution ──────────────────────────────────────────────────────────────

def test_distribution_keys_are_sorted():
    bars = [_bar(105.0, 108.0, 1000), _bar(100.0, 103.0, 1000)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    keys = [Decimal(k) for k in p.distribution.keys()]
    assert keys == sorted(keys)


def test_distribution_volume_sums_to_bar_input_volume():
    """sum(distribution) must equal sum(bar.volume) — no inflation, no loss."""
    bars = [_bar(100.0, 105.0, 1000), _bar(103.0, 107.0, 2000)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.total_volume == sum(b.volume for b in bars)
    assert sum(p.distribution.values()) == p.total_volume


def test_distribution_conservation_sparse_bar():
    """A bar spanning many bins with small volume must not inflate total."""
    bars = [_bar(100.0, 119.0, 3)]  # 20 bins, volume=3 → exactly 3 distributed
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.total_volume == 3
    assert sum(p.distribution.values()) == 3


def test_volume_distributed_across_bar_range():
    """A bar spanning 5 bins distributes volume across all 5."""
    bars = [_bar(100.0, 104.0, 500)]  # bins: 100, 101, 102, 103, 104
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert len(p.distribution) == 5
    for v in p.distribution.values():
        assert v > 0


# ── Bin size ──────────────────────────────────────────────────────────────────

def test_custom_bin_size():
    """With bin_size=2.0, a bar from 100-104 spans bins 100, 102, 104."""
    bars = [_bar(100.0, 104.0, 600)]
    builder = VolumeProfileBuilder(bin_size=Decimal("2.0"))
    p = builder.build(bars, "MNQ-09", _DATE)
    assert set(Decimal(k) for k in p.distribution) == {
        Decimal("100"), Decimal("102"), Decimal("104")
    }


# ── Metadata ──────────────────────────────────────────────────────────────────

def test_session_type_stored():
    bars = [_bar(100.0, 100.0, 100)]
    p = _builder().build(bars, "MNQ-09", _DATE, session_type="globex")
    assert p.session_type == "globex"


def test_bin_size_stored():
    bars = [_bar(100.0, 100.0, 100)]
    p = VolumeProfileBuilder(bin_size=Decimal("2.0")).build(bars, "MNQ-09", _DATE)
    assert p.bin_size == 2.0


def test_bars_source_field():
    bars = [_bar(100.0, 100.0, 100)]
    p = _builder().build(bars, "MNQ-09", _DATE)
    assert p.source == "bars"
    assert p.delta_distribution is None


# ── Trades path ───────────────────────────────────────────────────────────────

def _trade(price: float, size: int, side: TakerSide) -> Trade:
    return Trade(
        symbol="MNQ-09",
        timestamp=datetime(2026, 7, 3, 14, 0, tzinfo=timezone.utc),
        price=Decimal(str(price)),
        size=size,
        taker_side=side,
    )


def test_empty_trades_raises():
    with pytest.raises(ValueError, match="No trades"):
        _builder().build_from_trades([], "MNQ-09", _DATE)


def test_trades_source_field():
    trades = [_trade(100.0, 10, TakerSide.BUY)]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    assert p.source == "trades"
    assert p.delta_distribution is not None


def test_trades_exact_volume_placement():
    """Each trade lands in its exact bin — no bar-range spreading."""
    trades = [
        _trade(100.0, 50, TakerSide.BUY),
        _trade(100.25, 30, TakerSide.SELL),  # same bin 100.0 (bin_size=1.0)
        _trade(102.0, 200, TakerSide.BUY),
    ]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    assert p.poc == Decimal("102.0")
    assert p.distribution["102.0"] == 200
    assert p.distribution["100.0"] == 80   # 50 + 30


def test_trades_delta_buy_minus_sell():
    """delta = buy_volume - sell_volume per bin."""
    trades = [
        _trade(100.0, 60, TakerSide.BUY),
        _trade(100.0, 40, TakerSide.SELL),
        _trade(101.0, 100, TakerSide.SELL),
    ]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    assert p.delta_distribution["100.0"] == 20    # 60 - 40
    assert p.delta_distribution["101.0"] == -100  # 0 - 100


def test_trades_poc_is_highest_volume_bin():
    trades = [
        _trade(100.0, 10, TakerSide.BUY),
        _trade(101.0, 500, TakerSide.SELL),
        _trade(102.0, 30, TakerSide.BUY),
    ]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    assert p.poc == Decimal("101.0")


def test_trades_value_area_volume_at_least_70_pct():
    trades = [
        _trade(100.0, 100, TakerSide.BUY),
        _trade(101.0, 100, TakerSide.BUY),
        _trade(102.0, 500, TakerSide.SELL),
        _trade(103.0, 100, TakerSide.BUY),
        _trade(104.0, 100, TakerSide.BUY),
        _trade(110.0, 100, TakerSide.SELL),
    ]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    assert p.value_area_volume >= int(p.total_volume * 0.70)


def test_trades_total_volume():
    trades = [
        _trade(100.0, 10, TakerSide.BUY),
        _trade(101.0, 20, TakerSide.SELL),
        _trade(101.0, 5, TakerSide.BUY),
    ]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    assert p.total_volume == 35


def test_trades_distribution_keys_sorted():
    trades = [
        _trade(105.0, 10, TakerSide.BUY),
        _trade(100.0, 10, TakerSide.SELL),
        _trade(103.0, 10, TakerSide.BUY),
    ]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    keys = [Decimal(k) for k in p.distribution.keys()]
    assert keys == sorted(keys)


def test_trades_delta_distribution_keys_match_distribution():
    trades = [
        _trade(100.0, 10, TakerSide.BUY),
        _trade(102.0, 20, TakerSide.SELL),
    ]
    p = _builder().build_from_trades(trades, "MNQ-09", _DATE)
    assert set(p.delta_distribution.keys()) == set(p.distribution.keys())
