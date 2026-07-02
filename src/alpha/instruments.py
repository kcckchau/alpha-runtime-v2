from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from alpha.models.enums import AssetClass
from alpha.models.symbol import Symbol


@dataclass(frozen=True)
class FutureSpec:
    ticker: str
    exchange: str
    currency: str
    description: str
    tick_size: Decimal
    point_value: Decimal
    benchmark: str | None = None


_FUTURES: dict[str, FutureSpec] = {
    "MNQ": FutureSpec(
        ticker="MNQ",
        exchange="CME",
        currency="USD",
        description="Micro E-mini Nasdaq-100",
        tick_size=Decimal("0.25"),
        point_value=Decimal("2.0"),
    ),
    "MES": FutureSpec(
        ticker="MES",
        exchange="CME",
        currency="USD",
        description="Micro E-mini S&P 500",
        tick_size=Decimal("0.25"),
        point_value=Decimal("5.0"),
        benchmark="SPY",
    ),
    "M2K": FutureSpec(
        ticker="M2K",
        exchange="CME",
        currency="USD",
        description="Micro E-mini Russell 2000",
        tick_size=Decimal("0.10"),
        point_value=Decimal("5.0"),
    ),
    "MYM": FutureSpec(
        ticker="MYM",
        exchange="CBOT",
        currency="USD",
        description="Micro E-mini Dow",
        tick_size=Decimal("1"),
        point_value=Decimal("0.5"),
    ),
    "NQ": FutureSpec(
        ticker="NQ",
        exchange="CME",
        currency="USD",
        description="E-mini Nasdaq-100",
        tick_size=Decimal("0.25"),
        point_value=Decimal("20.0"),
    ),
    "ES": FutureSpec(
        ticker="ES",
        exchange="CME",
        currency="USD",
        description="E-mini S&P 500",
        tick_size=Decimal("0.25"),
        point_value=Decimal("50.0"),
        benchmark="SPY",
    ),
}

_QUARTERLY_MONTHS = (3, 6, 9, 12)


def quarterly_contract_month(as_of: date | None = None) -> str:
    current = as_of or date.today()
    month = current.month
    year = current.year
    for i, quarter_month in enumerate(_QUARTERLY_MONTHS):
        if month < quarter_month:
            return f"{year}{quarter_month:02d}"
        if month == quarter_month:
            # Roll on expiry Friday itself so we don't keep requesting a contract
            # that IBKR may no longer resolve reliably later that day.
            if current < _third_friday(year, quarter_month):
                return f"{year}{quarter_month:02d}"
            # Rolled past this expiry — return the next quarterly month.
            if i + 1 < len(_QUARTERLY_MONTHS):
                return f"{year}{_QUARTERLY_MONTHS[i + 1]:02d}"
            return f"{year + 1}03"
    return f"{year + 1}03"


def resolve_symbol(ticker: str, *, as_of: date | None = None) -> Symbol:
    normalized = ticker.upper().strip()

    # Support "ROOT-MM" style tickers (e.g. "MNQ-09" for the September MNQ contract).
    # The root is looked up in _FUTURES; root_symbol on the resulting Symbol is always
    # the bare root so that Databento can append the continuous suffix (e.g. "MNQ.c.0").
    root = normalized
    contract_month: str | None = None
    if "-" in normalized:
        parts = normalized.split("-", 1)
        root_candidate, suffix = parts[0], parts[1]
        if suffix.isdigit() and len(suffix) == 2 and root_candidate in _FUTURES:
            root = root_candidate
            # Determine year: if the month has already passed this year, it's next year.
            current = as_of or date.today()
            month = int(suffix)
            year = current.year if month >= current.month else current.year + 1
            contract_month = f"{year}{month:02d}"

    spec = _FUTURES.get(root)
    if spec is not None:
        return Symbol(
            ticker=normalized,
            exchange=spec.exchange,
            asset_class=AssetClass.FUTURE,
            root_symbol=spec.ticker,   # bare root — used by Databento for continuous suffix
            contract_month=contract_month or quarterly_contract_month(as_of),
            currency=spec.currency,
            description=spec.description,
            tick_size=spec.tick_size,
            point_value=spec.point_value,
            benchmark=spec.benchmark,
        )

    return Symbol(
        ticker=normalized,
        exchange="SMART",
        asset_class=AssetClass.EQUITY,
        benchmark="SPY" if normalized not in {"SPY", "QQQ"} else None,
    )


def _third_friday(year: int, month: int) -> date:
    first_day = date(year, month, 1)
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    return first_friday + timedelta(weeks=2)
