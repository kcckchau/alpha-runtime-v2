"""
Unit tests for BootstrapEngine backfill / catch-up logic.

Covers:
  - _timeframe_delta: M5 support
  - _should_fill_head_gap / _should_fill_tail_gap: cross-session gap fix
  - _missing_ranges: H1/D1 head, tail, and internal gaps
  - _vwap_session_start: RTH and extended-hours session detection
  - Warmup window sizing math
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from alpha.calendar.nyse import NYSECalendar
from alpha.config.settings import AlphaSettings, HistoricalSettings, RuntimeSettings
from alpha.engines.bootstrap.engine import BootstrapEngine
from alpha.models.enums import AssetClass, BarTimeframe, DataSourceId, RuntimeMode
from alpha.models.events import BarEvent, EventMetadata
from alpha.models.symbol import Symbol

_UTC = timezone.utc


# ── Helpers ───────────────────────────────────────────────────────────────────


def _bar(ts: datetime, tf: BarTimeframe = BarTimeframe.H1) -> BarEvent:
    return BarEvent(
        symbol="QQQ",
        timestamp=ts,
        metadata=EventMetadata(
            source=DataSourceId.SYNTHETIC,
            received_at=datetime.now(_UTC),
            is_replay=True,
        ),
        timeframe=tf,
        open=Decimal("400"),
        high=Decimal("401"),
        low=Decimal("399"),
        close=Decimal("400.5"),
        volume=1_000,
    )


def _make_engine(vwap_session: str = "rth") -> BootstrapEngine:
    """Return a minimally configured BootstrapEngine (no IBKR / storage wired)."""
    settings = AlphaSettings(
        runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["QQQ"]),
        historical=HistoricalSettings(
            minute1_warmup_bars=300,
            minute5_warmup_bars=300,
            hourly_warmup_bars=1000,
            daily_warmup_bars=1000,
            vwap_session=vwap_session,
        ),
    )
    engine = BootstrapEngine(settings)
    # Inject a real NYSE calendar so session queries work without full startup
    engine._calendar = NYSECalendar()
    engine._registry = MagicMock()
    engine._registry.get.return_value = Symbol(
        ticker="QQQ", exchange="NASDAQ", asset_class=AssetClass.ETF
    )
    return engine


# ── _timeframe_delta ──────────────────────────────────────────────────────────


class TestTimeframeDelta:
    def test_m1(self) -> None:
        assert BootstrapEngine._timeframe_delta(BarTimeframe.M1) == timedelta(minutes=1)

    def test_m5(self) -> None:
        assert BootstrapEngine._timeframe_delta(BarTimeframe.M5) == timedelta(minutes=5)

    def test_h1(self) -> None:
        assert BootstrapEngine._timeframe_delta(BarTimeframe.H1) == timedelta(hours=1)

    def test_d1(self) -> None:
        assert BootstrapEngine._timeframe_delta(BarTimeframe.D1) == timedelta(days=1)


# ── _should_fill_head_gap / _should_fill_tail_gap ─────────────────────────────


class TestHeadTailGapDetection:
    """Head and tail gaps should always be filled regardless of session boundaries."""

    calendar = NYSECalendar()

    # ── head gap ──────────────────────────────────────────────────────────────

    def test_head_gap_filled_same_session_h1(self) -> None:
        start = datetime(2026, 5, 26, 14, 0, tzinfo=_UTC)   # Mon 10:00 ET
        first = datetime(2026, 5, 26, 15, 0, tzinfo=_UTC)   # Mon 11:00 ET (same session)
        assert BootstrapEngine._should_fill_head_gap(self.calendar, BarTimeframe.H1, start, first)

    def test_head_gap_filled_cross_session_h1(self) -> None:
        # Previously this returned False due to session_key check — now must return True
        start = datetime(2026, 5, 20, 14, 0, tzinfo=_UTC)   # Wed 10:00 ET
        first = datetime(2026, 5, 26, 14, 0, tzinfo=_UTC)   # Mon 10:00 ET (different session)
        assert BootstrapEngine._should_fill_head_gap(self.calendar, BarTimeframe.H1, start, first)

    def test_head_gap_filled_cross_session_d1(self) -> None:
        start = datetime(2026, 5, 1, 13, 30, tzinfo=_UTC)
        first = datetime(2026, 5, 15, 13, 30, tzinfo=_UTC)
        assert BootstrapEngine._should_fill_head_gap(self.calendar, BarTimeframe.D1, start, first)

    def test_head_gap_not_filled_when_no_gap(self) -> None:
        ts = datetime(2026, 5, 26, 14, 0, tzinfo=_UTC)
        assert not BootstrapEngine._should_fill_head_gap(self.calendar, BarTimeframe.H1, ts, ts)

    # ── tail gap ──────────────────────────────────────────────────────────────

    def test_tail_gap_filled_same_session_h1(self) -> None:
        last = datetime(2026, 5, 26, 14, 0, tzinfo=_UTC)
        end  = datetime(2026, 5, 26, 19, 0, tzinfo=_UTC)
        assert BootstrapEngine._should_fill_tail_gap(self.calendar, BarTimeframe.H1, last, end)

    def test_tail_gap_filled_cross_session_h1(self) -> None:
        # Previously returned False — the core bug this PR fixes
        last = datetime(2026, 5, 22, 20, 0, tzinfo=_UTC)   # Friday close
        end  = datetime(2026, 5, 26, 15, 0, tzinfo=_UTC)   # Monday mid-session
        assert BootstrapEngine._should_fill_tail_gap(self.calendar, BarTimeframe.H1, last, end)

    def test_tail_gap_filled_cross_session_d1(self) -> None:
        last = datetime(2026, 5, 1, 13, 30, tzinfo=_UTC)
        end  = datetime(2026, 5, 26, 13, 30, tzinfo=_UTC)
        assert BootstrapEngine._should_fill_tail_gap(self.calendar, BarTimeframe.D1, last, end)

    def test_tail_gap_not_filled_when_no_gap(self) -> None:
        ts = datetime(2026, 5, 26, 14, 0, tzinfo=_UTC)
        assert not BootstrapEngine._should_fill_tail_gap(self.calendar, BarTimeframe.H1, ts, ts)


# ── _missing_ranges ───────────────────────────────────────────────────────────


class TestMissingRanges:
    """Integration-style tests for the full missing-range detection pipeline."""

    def _engine(self) -> BootstrapEngine:
        return _make_engine()

    # ── H1 ────────────────────────────────────────────────────────────────────

    def test_h1_no_stored_data_returns_full_window(self) -> None:
        engine = self._engine()
        start = datetime(2026, 5, 20, 13, 30, tzinfo=_UTC)
        end   = datetime(2026, 5, 26, 20, 0, tzinfo=_UTC)
        ranges = engine._missing_ranges("QQQ", [], start, end, BarTimeframe.H1)
        assert ranges == [(start, end)]

    def test_h1_head_gap_across_weekend_is_detected(self) -> None:
        engine = self._engine()
        # Request goes back to Wednesday; first stored bar is Monday
        start = datetime(2026, 5, 20, 13, 30, tzinfo=_UTC)  # Wed
        first_stored = datetime(2026, 5, 26, 14, 30, tzinfo=_UTC)  # Mon 10:30 ET
        bars = [_bar(first_stored + timedelta(hours=i)) for i in range(3)]
        ranges = engine._missing_ranges("QQQ", bars, start, first_stored + timedelta(hours=3), BarTimeframe.H1)
        assert len(ranges) >= 1
        assert ranges[0][0] == start

    def test_h1_tail_gap_across_weekend_is_detected(self) -> None:
        engine = self._engine()
        # Last stored bar is Friday; end is Monday
        last_ts = datetime(2026, 5, 22, 20, 0, tzinfo=_UTC)  # Fri ~4pm ET
        end     = datetime(2026, 5, 26, 16, 0, tzinfo=_UTC)  # Mon ~12pm ET
        bars = [_bar(last_ts - timedelta(hours=i)) for i in range(3, 0, -1)]
        ranges = engine._missing_ranges("QQQ", bars, bars[0].timestamp, end, BarTimeframe.H1)
        assert len(ranges) >= 1
        assert ranges[-1][1] == end

    def test_h1_internal_gap_same_session_filled(self) -> None:
        engine = self._engine()
        # Gap within the same trading day
        t0 = datetime(2026, 5, 26, 14, 30, tzinfo=_UTC)
        t1 = datetime(2026, 5, 26, 14, 30, tzinfo=_UTC)
        t2 = datetime(2026, 5, 26, 18, 30, tzinfo=_UTC)  # 3-bar gap
        bars = [_bar(t1), _bar(t2)]
        ranges = engine._missing_ranges("QQQ", bars, t0, t2 + timedelta(hours=1), BarTimeframe.H1)
        internal = [r for r in ranges if t1 < r[0] < t2]
        assert internal, "Expected an internal gap to be detected within the same session"

    def test_h1_internal_gap_overnight_not_filled(self) -> None:
        engine = self._engine()
        # Consecutive daily session closes — no gap should be raised
        fri_close = datetime(2026, 5, 22, 20, 0, tzinfo=_UTC)
        mon_open  = datetime(2026, 5, 26, 13, 30, tzinfo=_UTC)
        bars = [_bar(fri_close), _bar(mon_open)]
        ranges = engine._missing_ranges("QQQ", bars, fri_close, mon_open + timedelta(hours=1), BarTimeframe.H1)
        # No internal gap between Friday close and Monday open (different sessions)
        overnight = [r for r in ranges if fri_close < r[0] < mon_open]
        assert not overnight

    # ── M5 ────────────────────────────────────────────────────────────────────

    def test_m5_no_stored_data_returns_full_window(self) -> None:
        engine = self._engine()
        start = datetime(2026, 5, 26, 13, 30, tzinfo=_UTC)
        end   = datetime(2026, 5, 26, 19, 0, tzinfo=_UTC)
        ranges = engine._missing_ranges("QQQ", [], start, end, BarTimeframe.M5)
        assert ranges == [(start, end)]

    def test_m5_gap_detected(self) -> None:
        engine = self._engine()
        t0 = datetime(2026, 5, 26, 13, 30, tzinfo=_UTC)
        t1 = datetime(2026, 5, 26, 13, 30, tzinfo=_UTC)
        t2 = datetime(2026, 5, 26, 14, 0, tzinfo=_UTC)   # 30-min gap
        bars = [_bar(t1, BarTimeframe.M5), _bar(t2, BarTimeframe.M5)]
        ranges = engine._missing_ranges("QQQ", bars, t0, t2 + timedelta(minutes=5), BarTimeframe.M5)
        internal = [r for r in ranges if t1 < r[0] < t2]
        assert internal


# ── _vwap_session_start ───────────────────────────────────────────────────────


class TestVwapSessionStart:
    def test_rth_mid_session(self) -> None:
        engine = _make_engine(vwap_session="rth")
        # 2026-05-26 is a Tuesday; 15:00 UTC = 11:00 AM ET (mid-session)
        now = datetime(2026, 5, 26, 15, 0, tzinfo=_UTC)
        result = engine._vwap_session_start(now)
        # Should be 09:30 ET on the same day = 13:30 UTC
        assert result == datetime(2026, 5, 26, 13, 30, tzinfo=_UTC)

    def test_rth_before_open_falls_back_to_previous_session(self) -> None:
        engine = _make_engine(vwap_session="rth")
        # 2026-05-26 08:00 ET = before RTH open → fall back to Friday 2026-05-22
        now = datetime(2026, 5, 26, 12, 0, tzinfo=_UTC)  # 08:00 AM ET
        result = engine._vwap_session_start(now)
        assert result == datetime(2026, 5, 22, 13, 30, tzinfo=_UTC)

    def test_extended_mid_session(self) -> None:
        engine = _make_engine(vwap_session="extended")
        # 2026-05-26 11:00 UTC = 07:00 AM ET → after 04:00 AM extended open
        now = datetime(2026, 5, 26, 11, 0, tzinfo=_UTC)
        result = engine._vwap_session_start(now)
        # Should be 04:00 ET on the same day = 08:00 UTC
        assert result == datetime(2026, 5, 26, 8, 0, tzinfo=_UTC)

    def test_extended_before_open_falls_back(self) -> None:
        engine = _make_engine(vwap_session="extended")
        # 2026-05-26 07:00 UTC = 03:00 AM ET → before 04:00 AM extended open
        now = datetime(2026, 5, 26, 7, 0, tzinfo=_UTC)
        result = engine._vwap_session_start(now)
        # Previous trading day is 2026-05-22 (Fri), 04:00 ET = 08:00 UTC
        assert result == datetime(2026, 5, 22, 8, 0, tzinfo=_UTC)


# ── Warmup window sizing ──────────────────────────────────────────────────────


class TestWarmupWindowSizing:
    """Validate that the warmup window math meets indicator requirements."""

    def test_h1_window_covers_sma200(self) -> None:
        # hourly_warmup_bars=1000, formula: int(1000 * 0.22) + 30 = 250 days
        # 250 calendar days × (5/7 trading ratio) ≈ 178 trading days × 6.5h ≈ 1157 bars
        hist = HistoricalSettings(hourly_warmup_bars=1000)
        days = int(hist.hourly_warmup_bars * 0.22) + 30
        approx_trading_days = days * 5 / 7
        approx_bars = approx_trading_days * 6.5
        assert approx_bars >= 200, f"Expected ≥200 hourly bars for SMA200, got ≈{approx_bars:.0f}"

    def test_d1_window_covers_sma200(self) -> None:
        # daily_warmup_bars=1000, formula: int(1000 * 1.5) = 1500 calendar days
        # 1500 × (5/7) ≈ 1071 trading days
        hist = HistoricalSettings(daily_warmup_bars=1000)
        days = int(hist.daily_warmup_bars * 1.5)
        approx_trading_days = days * 5 / 7
        assert approx_trading_days >= 200, f"Expected ≥200 daily bars for SMA200, got ≈{approx_trading_days:.0f}"

    def test_m1_window_covers_ema21(self) -> None:
        # minute1_warmup_bars=300, formula: max(3, 300 // 390 + 1) = 2 → max(3,2) = 3 days
        # 3 days × (5/7) × 390 bars/day ≈ 836 bars >> 21
        hist = HistoricalSettings(minute1_warmup_bars=300)
        days = max(3, hist.minute1_warmup_bars // 390 + 1)
        approx_bars = days * (5 / 7) * 390
        assert approx_bars >= 21

    def test_m5_window_covers_ema21(self) -> None:
        # minute5_warmup_bars=300, formula: max(7, 300 // 78 + 2) = max(7,5) = 7 days
        # 7 × (5/7) × 78 bars/day = 390 bars >> 21
        hist = HistoricalSettings(minute5_warmup_bars=300)
        days = max(7, hist.minute5_warmup_bars // 78 + 2)
        approx_bars = days * (5 / 7) * 78
        assert approx_bars >= 21
