"""
NYSE / NASDAQ session calendar backed by the `exchange_calendars` library.

This is the default calendar for US equity strategies.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import exchange_calendars as xcals  # type: ignore[import]

from alpha.calendar.base import SessionCalendar
from alpha.models.enums import SessionPhase

_ET = ZoneInfo("America/New_York")


class NYSECalendar(SessionCalendar):
    """US equity session calendar (NYSE/NASDAQ hours)."""

    # Regular hours: 09:30–16:00 ET
    # Pre-market:    04:00–09:30 ET
    # After hours:   16:00–20:00 ET
    _ORB_MINUTES = 5

    def __init__(self) -> None:
        self._cal = xcals.get_calendar("XNYS")

    @property
    def name(self) -> str:
        return "NYSE"

    @property
    def timezone(self) -> str:
        return "America/New_York"

    # ── Session boundaries ────────────────────────────────────────────────────

    def session_open(self, d: date) -> datetime:
        return datetime(d.year, d.month, d.day, 9, 30, tzinfo=_ET)

    def session_close(self, d: date) -> datetime:
        return datetime(d.year, d.month, d.day, 16, 0, tzinfo=_ET)

    def pre_market_open(self, d: date) -> datetime | None:
        return datetime(d.year, d.month, d.day, 4, 0, tzinfo=_ET)

    def after_hours_close(self, d: date) -> datetime | None:
        return datetime(d.year, d.month, d.day, 20, 0, tzinfo=_ET)

    # ── Day queries ───────────────────────────────────────────────────────────

    def is_trading_day(self, d: date) -> bool:
        return self._cal.is_session(d.isoformat())

    def is_holiday(self, d: date) -> bool:
        return d.weekday() < 5 and not self.is_trading_day(d)

    def trading_days(self, start: date, end: date) -> list[date]:
        sessions = self._cal.sessions_in_range(start.isoformat(), end.isoformat())
        return [s.date() for s in sessions]

    def next_trading_day(self, d: date) -> date:
        candidate = d + timedelta(days=1)
        while not self.is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    def prev_trading_day(self, d: date) -> date:
        candidate = d - timedelta(days=1)
        while not self.is_trading_day(candidate):
            candidate -= timedelta(days=1)
        return candidate

    # ── Intraday queries ──────────────────────────────────────────────────────

    def is_open(self, dt: datetime) -> bool:
        et = dt.astimezone(_ET)
        if not self.is_trading_day(et.date()):
            return False
        return self.session_open(et.date()) <= et < self.session_close(et.date())

    def session_phase(self, dt: datetime) -> SessionPhase:  # noqa: PLR0911
        et = dt.astimezone(_ET)
        d = et.date()

        if not self.is_trading_day(d):
            return SessionPhase.CLOSED

        pre = self.pre_market_open(d)
        reg_open = self.session_open(d)
        reg_close = self.session_close(d)
        ah_close = self.after_hours_close(d)

        if pre and pre <= et < reg_open:
            return SessionPhase.PRE_MARKET
        if ah_close and reg_close <= et < ah_close:
            return SessionPhase.AFTER_HOURS
        if et >= reg_close or et < reg_open:
            return SessionPhase.CLOSED

        # Within regular session
        orb_end = reg_open + timedelta(minutes=self._ORB_MINUTES)
        early_end = reg_open + timedelta(hours=1)       # 10:30
        mid_end = reg_open + timedelta(hours=4, minutes=30)  # 14:00
        power_start = reg_close - timedelta(hours=2)   # 14:00
        close_start = reg_close - timedelta(minutes=30)

        if et < orb_end:
            return SessionPhase.OPENING_RANGE
        if et < early_end:
            return SessionPhase.EARLY
        if et < mid_end:
            return SessionPhase.MID
        if et < close_start:
            return SessionPhase.POWER_HOUR
        return SessionPhase.CLOSING
