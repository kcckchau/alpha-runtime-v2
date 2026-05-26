"""
CME equity-index futures session calendar.

This models the broad futures trading session (18:00 ET prior day through
17:00 ET trade date) while still classifying the US cash-session sub-phases
used by the setup logic.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from alpha.calendar.base import SessionCalendar
from alpha.models.enums import SessionPhase

_ET = ZoneInfo("America/New_York")
_SESSION_START = time(18, 0)
_MAINTENANCE_START = time(17, 0)


class CMEEqIndexCalendar(SessionCalendar):
    """Session calendar for CME equity-index futures such as ES/NQ/MES/MNQ."""

    _ORB_MINUTES = 5

    @property
    def name(self) -> str:
        return "CME_EQ_INDEX"

    @property
    def timezone(self) -> str:
        return "America/New_York"

    def session_open(self, d: date) -> datetime:
        return datetime(d.year, d.month, d.day, 9, 30, tzinfo=_ET)

    def session_close(self, d: date) -> datetime:
        return datetime(d.year, d.month, d.day, 16, 0, tzinfo=_ET)

    def pre_market_open(self, d: date) -> datetime | None:
        return datetime.combine(d - timedelta(days=1), _SESSION_START, _ET)

    def after_hours_close(self, d: date) -> datetime | None:
        return datetime(d.year, d.month, d.day, 17, 0, tzinfo=_ET)

    def is_trading_day(self, d: date) -> bool:
        return d.weekday() < 5

    def is_holiday(self, d: date) -> bool:
        return False

    def trading_days(self, start: date, end: date) -> list[date]:
        days: list[date] = []
        current = start
        while current <= end:
            if self.is_trading_day(current):
                days.append(current)
            current += timedelta(days=1)
        return days

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

    def is_open(self, dt: datetime) -> bool:
        et = dt.astimezone(_ET)
        return self.session_phase(et) not in {SessionPhase.CLOSED, SessionPhase.AFTER_HOURS}

    def session_phase(self, dt: datetime) -> SessionPhase:  # noqa: PLR0911
        et = dt.astimezone(_ET)
        trade_date = self.session_date(et)

        session_start = self.pre_market_open(trade_date)
        reg_open = self.session_open(trade_date)
        reg_close = self.session_close(trade_date)
        maintenance_close = self.after_hours_close(trade_date)

        if session_start is None or maintenance_close is None:
            return SessionPhase.CLOSED
        if et < session_start or et >= maintenance_close:
            return SessionPhase.CLOSED
        if et < reg_open:
            return SessionPhase.PRE_MARKET
        if et >= reg_close:
            return SessionPhase.AFTER_HOURS

        orb_end = reg_open + timedelta(minutes=self._ORB_MINUTES)
        early_end = reg_open + timedelta(hours=1)
        mid_end = reg_open + timedelta(hours=4, minutes=30)
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

    def session_date(self, dt: datetime) -> date:
        et = dt.astimezone(_ET)
        candidate = et.date()
        if et.time() >= _SESSION_START:
            candidate += timedelta(days=1)
        if not self.is_trading_day(candidate):
            candidate = self.next_trading_day(candidate)
        return candidate
