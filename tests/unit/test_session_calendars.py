from __future__ import annotations

from datetime import datetime, timezone

from alpha.calendar.cme import CMEEqIndexCalendar
from alpha.calendar.nyse import NYSECalendar
from alpha.models.enums import SessionPhase


def test_nyse_session_date_uses_local_trading_day() -> None:
    calendar = NYSECalendar()

    session_date = calendar.session_date(datetime(2026, 5, 26, 13, 45, tzinfo=timezone.utc))

    assert session_date.isoformat() == "2026-05-26"


def test_cme_evening_bar_rolls_into_next_trade_date() -> None:
    calendar = CMEEqIndexCalendar()

    session_date = calendar.session_date(datetime(2026, 5, 26, 22, 30, tzinfo=timezone.utc))

    assert session_date.isoformat() == "2026-05-27"
    assert calendar.session_key(datetime(2026, 5, 26, 22, 30, tzinfo=timezone.utc)) == "2026-05-27"


def test_cme_us_open_classifies_as_opening_range() -> None:
    calendar = CMEEqIndexCalendar()

    phase = calendar.session_phase(datetime(2026, 5, 26, 13, 31, tzinfo=timezone.utc))

    assert phase == SessionPhase.OPENING_RANGE
