from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

from alpha.engines.ibkr.contracts import normalize_bar
from alpha.models.enums import BarTimeframe, DataSourceId


def _raw_bar(raw_date: object) -> SimpleNamespace:
    return SimpleNamespace(
        date=raw_date,
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=1234,
        vwap=100.2,
        barCount=12,
    )


def test_normalize_intraday_bar_preserves_aware_utc_timestamp() -> None:
    raw = _raw_bar(datetime(2026, 5, 26, 22, 0, tzinfo=timezone.utc))

    event = normalize_bar(raw, "MNQ", BarTimeframe.M1, is_replay=True)

    assert event.timestamp == datetime(2026, 5, 26, 22, 0, tzinfo=timezone.utc)
    assert event.metadata.source == DataSourceId.INTERACTIVE_BROKERS


def test_normalize_intraday_bar_treats_naive_timestamp_as_utc() -> None:
    raw = _raw_bar(datetime(2026, 5, 26, 22, 0))

    event = normalize_bar(raw, "MNQ", BarTimeframe.M1, is_replay=False)

    assert event.timestamp == datetime(2026, 5, 26, 22, 0, tzinfo=timezone.utc)


def test_normalize_daily_bar_uses_session_open_in_utc() -> None:
    raw = _raw_bar(date(2026, 5, 26))

    event = normalize_bar(raw, "MNQ", BarTimeframe.D1, is_replay=True)

    assert event.timestamp == datetime(2026, 5, 26, 13, 30, tzinfo=timezone.utc)
