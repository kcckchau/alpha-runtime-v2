from __future__ import annotations

from functools import lru_cache

from alpha.calendar.base import SessionCalendar
from alpha.calendar.cme import CMEEqIndexCalendar
from alpha.calendar.nyse import NYSECalendar
from alpha.models.enums import AssetClass
from alpha.models.symbol import Symbol


@lru_cache(maxsize=1)
def _nyse_calendar() -> SessionCalendar:
    return NYSECalendar()


@lru_cache(maxsize=1)
def _cme_calendar() -> SessionCalendar:
    return CMEEqIndexCalendar()


def calendar_for_symbol(symbol: Symbol) -> SessionCalendar:
    if symbol.asset_class == AssetClass.FUTURE:
        return _cme_calendar()
    return _nyse_calendar()
