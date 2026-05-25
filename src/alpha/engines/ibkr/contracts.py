"""
IBKR contract factory and bar normalizer.

Shared by all three IBKR adapters so normalization logic lives in one place.
"""

from __future__ import annotations

import math
from datetime import date
from datetime import datetime, time, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from alpha.instruments import quarterly_contract_month
from alpha.models.enums import AssetClass, BarTimeframe, DataSourceId
from alpha.models.events import BarEvent, EventMetadata, QuoteEvent, TradeEvent
from alpha.models.symbol import Symbol

_ET = ZoneInfo("America/New_York")
_UTC = timezone.utc


def _safe_int(value: object) -> int:
    """Convert vendor numeric fields to int, treating None/NaN/inf as zero."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return 0
        return int(value)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(numeric):
        return 0
    return int(numeric)


def _safe_float(value: object) -> float | None:
    """Return a finite float or None for missing/NaN/inf vendor fields."""
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        numeric = float(value)
    else:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
    if not math.isfinite(numeric):
        return None
    return numeric

# Maps our BarTimeframe to IBKR barSizeSetting strings
TIMEFRAME_TO_IBKR: dict[BarTimeframe, str] = {
    BarTimeframe.S5:  "5 secs",
    BarTimeframe.S10: "10 secs",
    BarTimeframe.S15: "15 secs",
    BarTimeframe.S30: "30 secs",
    BarTimeframe.M1:  "1 min",
    BarTimeframe.M2:  "2 mins",
    BarTimeframe.M3:  "3 mins",
    BarTimeframe.M5:  "5 mins",
    BarTimeframe.M10: "10 mins",
    BarTimeframe.M15: "15 mins",
    BarTimeframe.M30: "30 mins",
    BarTimeframe.H1:  "1 hour",
    BarTimeframe.H2:  "2 hours",
    BarTimeframe.H4:  "4 hours",
    BarTimeframe.D1:  "1 day",
}

# Maximum days per chunk for each timeframe (IBKR pacing limits)
MAX_CHUNK_DAYS: dict[BarTimeframe, int] = {
    BarTimeframe.S5:  1,
    BarTimeframe.S10: 1,
    BarTimeframe.S15: 1,
    BarTimeframe.S30: 1,
    BarTimeframe.M1:  7,
    BarTimeframe.M2:  7,
    BarTimeframe.M3:  7,
    BarTimeframe.M5:  14,
    BarTimeframe.M10: 14,
    BarTimeframe.M15: 28,
    BarTimeframe.M30: 28,
    BarTimeframe.H1:  60,
    BarTimeframe.H2:  60,
    BarTimeframe.H4:  60,
    BarTimeframe.D1:  365,
}


def make_contract(sym: Symbol) -> "object":
    """Return an ib_insync Contract for the given Symbol."""
    from ib_insync import Future, Stock

    if sym.asset_class in {AssetClass.EQUITY, AssetClass.ETF}:
        exchange = sym.exchange if sym.exchange != "UNKNOWN" else "SMART"
        return Stock(sym.ticker, exchange, sym.currency)

    if sym.asset_class == AssetClass.FUTURE:
        contract_month = sym.contract_month or quarterly_contract_month(date.today())
        root_symbol = sym.root_symbol or sym.ticker
        return Future(
            symbol=root_symbol,
            lastTradeDateOrContractMonth=contract_month,
            exchange=sym.exchange,
            currency=sym.currency,
        )

    raise NotImplementedError(
        f"IBKR contract not implemented for asset class: {sym.asset_class}"
    )


def normalize_bar(
    raw: "object",
    symbol: str,
    timeframe: BarTimeframe,
    *,
    is_replay: bool,
) -> BarEvent:
    """Convert an ib_insync BarData object into a normalized BarEvent."""
    # IBKR gives datetime for intraday, date for daily+
    raw_date = getattr(raw, "date", None)
    if isinstance(raw_date, datetime):
        ts = raw_date.replace(tzinfo=_ET).astimezone(_UTC)
    else:
        # daily bar — use session open as the timestamp
        ts = datetime.combine(raw_date, time(9, 30), _ET).astimezone(_UTC)

    vwap_raw = getattr(raw, "vwap", None)
    bar_count = getattr(raw, "barCount", None)

    return BarEvent(
        symbol=symbol,
        timestamp=ts,
        metadata=EventMetadata(
            source=DataSourceId.INTERACTIVE_BROKERS,
            received_at=datetime.now(_UTC),
            is_replay=is_replay,
        ),
        timeframe=timeframe,
        open=Decimal(str(raw.open)),
        high=Decimal(str(raw.high)),
        low=Decimal(str(raw.low)),
        close=Decimal(str(raw.close)),
        volume=int(raw.volume),
        vwap=Decimal(str(round(vwap_raw, 4))) if vwap_raw and vwap_raw > 0 else None,
        trade_count=int(bar_count) if bar_count and bar_count > 0 else None,
    )


def normalize_quote(
    ticker: "object",
    symbol: str,
) -> QuoteEvent | None:
    """Convert an ib_insync Ticker into a QuoteEvent. Returns None if bid/ask not ready."""
    bid = getattr(ticker, "bid", None)
    ask = getattr(ticker, "ask", None)
    bid_size = getattr(ticker, "bidSize", None)
    ask_size = getattr(ticker, "askSize", None)

    bid_value = _safe_float(bid)
    ask_value = _safe_float(ask)

    if bid_value is None or ask_value is None or bid_value <= 0 or ask_value <= 0:
        return None

    return QuoteEvent(
        symbol=symbol,
        timestamp=datetime.now(_UTC),
        metadata=EventMetadata(
            source=DataSourceId.INTERACTIVE_BROKERS,
            received_at=datetime.now(_UTC),
            is_replay=False,
        ),
        bid_price=Decimal(str(bid_value)),
        bid_size=_safe_int(bid_size),
        ask_price=Decimal(str(ask_value)),
        ask_size=_safe_int(ask_size),
    )
