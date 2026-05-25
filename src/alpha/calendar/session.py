"""
SessionContext: per-bar session metadata derived from the calendar.

The Feature Engine attaches a SessionContext to every BarSnapshot,
allowing downstream engines to make time-aware decisions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from alpha.models.enums import SessionPhase


@dataclass(frozen=True)
class SessionContext:
    """Snapshot of session state at a given bar timestamp."""

    date: date
    phase: SessionPhase
    bars_since_open: int
    minutes_since_open: float
    minutes_to_close: float
    is_first_bar: bool
    is_last_bar: bool
    orb_established: bool               # opening range has been set
    session_open: datetime
    session_close: datetime

    @property
    def is_regular(self) -> bool:
        return self.phase not in {
            SessionPhase.PRE_MARKET,
            SessionPhase.AFTER_HOURS,
            SessionPhase.CLOSED,
        }

    @property
    def is_early_session(self) -> bool:
        return self.phase in {SessionPhase.OPENING_RANGE, SessionPhase.EARLY}

    @property
    def is_late_session(self) -> bool:
        return self.phase in {SessionPhase.POWER_HOUR, SessionPhase.CLOSING}
