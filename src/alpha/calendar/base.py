"""
Session calendar abstraction.

All time-of-day and holiday logic routes through this interface.
Engines never call `datetime.now()` or hardcode exchange hours directly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime

from alpha.models.enums import SessionPhase


class SessionCalendar(ABC):
    """Abstract trading session calendar."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Calendar identifier, e.g. 'NYSE', 'CME'."""

    @property
    @abstractmethod
    def timezone(self) -> str:
        """IANA timezone string, e.g. 'America/New_York'."""

    # ── Session boundaries ────────────────────────────────────────────────────

    @abstractmethod
    def session_open(self, d: date) -> datetime:
        """Regular session open for date d (timezone-aware)."""

    @abstractmethod
    def session_close(self, d: date) -> datetime:
        """Regular session close for date d (timezone-aware)."""

    @abstractmethod
    def pre_market_open(self, d: date) -> datetime | None:
        """Pre-market open, or None if not supported."""

    @abstractmethod
    def after_hours_close(self, d: date) -> datetime | None:
        """After-hours close, or None if not supported."""

    # ── Day queries ───────────────────────────────────────────────────────────

    @abstractmethod
    def is_trading_day(self, d: date) -> bool:
        """True if d is a regular trading day (not weekend/holiday)."""

    @abstractmethod
    def is_holiday(self, d: date) -> bool:
        """True if d is a market holiday."""

    @abstractmethod
    def trading_days(self, start: date, end: date) -> list[date]:
        """All trading days in [start, end] inclusive."""

    @abstractmethod
    def next_trading_day(self, d: date) -> date:
        """Next trading day after d."""

    @abstractmethod
    def prev_trading_day(self, d: date) -> date:
        """Most recent trading day before d."""

    # ── Intraday queries ──────────────────────────────────────────────────────

    @abstractmethod
    def is_open(self, dt: datetime) -> bool:
        """True if dt falls within the regular session."""

    @abstractmethod
    def session_phase(self, dt: datetime) -> SessionPhase:
        """Classify dt into a session phase."""

    def opening_range_end(self, d: date, minutes: int = 5) -> datetime:
        """End of opening range period."""
        from datetime import timedelta
        return self.session_open(d) + timedelta(minutes=minutes)
