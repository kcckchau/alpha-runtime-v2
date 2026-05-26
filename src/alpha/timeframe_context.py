from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from alpha.calendar.base import SessionCalendar
from alpha.models.events import BarEvent

_ET = ZoneInfo("America/New_York")
_ZERO = Decimal("0")
_INTRADAY_PERIODS = (9, 21)
_MONTHLY_PERIODS = (10, 20, 50)
_TREND_PERIODS = (10, 20, 50, 100, 200)


def build_symbol_context(
    symbol: str,
    minute_bars: list[BarEvent],
    hourly_bars: list[BarEvent],
    daily_bars: list[BarEvent],
    calendar: SessionCalendar,
) -> dict[str, Any]:
    monthly_bars = _aggregate_monthly_bars(daily_bars)
    return {
        "symbol": symbol,
        "bar_counts": {
            "1m": len(minute_bars),
            "1h": len(hourly_bars),
            "1d": len(daily_bars),
            "1mo": len(monthly_bars),
        },
        "ema_levels": {
            "1m": _ema_levels(minute_bars, _INTRADAY_PERIODS),
            "1h": _ema_levels(hourly_bars, _TREND_PERIODS),
            "1d": _ema_levels(daily_bars, _TREND_PERIODS),
            "1mo": _ema_levels(monthly_bars, _MONTHLY_PERIODS),
        },
        "levels": _session_levels(minute_bars, daily_bars, calendar),
    }


def rows_to_history_payload(rows: list[dict[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "symbol": row["symbol"],
                "timeframe": timeframe,
                "timestamp": row["timestamp"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "vwap": row.get("vwap"),
                "trade_count": row.get("trade_count"),
                "is_partial": row.get("is_partial", False),
                "source": row.get("source"),
                "is_replay": row.get("is_replay"),
            }
        )
    return payload


def aggregate_monthly_history(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(rows, key=lambda item: item["timestamp"]):
        ts = _parse_timestamp(row["timestamp"]).astimezone(_ET)
        buckets[(ts.year, ts.month)].append(row)

    monthly_rows: list[dict[str, Any]] = []
    for key in sorted(buckets.keys()):
        bucket = buckets[key]
        first = bucket[0]
        last = bucket[-1]
        monthly_rows.append(
            {
                "symbol": first["symbol"],
                "timestamp": first["timestamp"],
                "open": first["open"],
                "high": _max_decimal_text(item["high"] for item in bucket),
                "low": _min_decimal_text(item["low"] for item in bucket),
                "close": last["close"],
                "volume": sum(int(item["volume"]) for item in bucket),
                "vwap": None,
                "trade_count": _sum_optional_int(item.get("trade_count") for item in bucket),
                "is_partial": False,
                "source": first.get("source"),
                "is_replay": first.get("is_replay"),
            }
        )
    return monthly_rows


def _aggregate_monthly_bars(daily_bars: list[BarEvent]) -> list[BarEvent]:
    buckets: dict[tuple[int, int], list[BarEvent]] = defaultdict(list)
    for bar in sorted(daily_bars, key=lambda item: item.timestamp):
        ts = bar.timestamp.astimezone(_ET)
        buckets[(ts.year, ts.month)].append(bar)

    monthly: list[BarEvent] = []
    for key in sorted(buckets.keys()):
        bucket = buckets[key]
        first = bucket[0]
        last = bucket[-1]
        volume = sum(item.volume for item in bucket)
        trade_count = _sum_optional_int(item.trade_count for item in bucket)
        monthly.append(
            BarEvent(
                symbol=first.symbol,
                timestamp=first.timestamp,
                metadata=first.metadata,
                timeframe=first.timeframe,
                open=first.open,
                high=max(item.high for item in bucket),
                low=min(item.low for item in bucket),
                close=last.close,
                volume=volume,
                vwap=None,
                trade_count=trade_count,
                is_partial=False,
            )
        )
    return monthly


def _ema_levels(bars: list[BarEvent], periods: tuple[int, ...]) -> dict[str, str | None]:
    closes = [bar.close for bar in bars]
    levels: dict[str, str | None] = {}
    for period in periods:
        ema = _ema(closes, period)
        levels[str(period)] = str(ema) if ema is not None else None
    return levels


def _ema(values: list[Decimal], period: int) -> Decimal | None:
    if not values:
        return None
    ema = values[0]
    alpha = Decimal(2) / Decimal(period + 1)
    for value in values[1:]:
        ema = (value * alpha) + (ema * (Decimal(1) - alpha))
    return ema


def _session_levels(
    minute_bars: list[BarEvent],
    daily_bars: list[BarEvent],
    calendar: SessionCalendar,
) -> dict[str, str | None]:
    levels = {
        "previous_day_high": None,
        "previous_day_low": None,
        "premarket_high": None,
        "premarket_low": None,
    }
    if daily_bars:
        today_et = datetime.now(tz=_ET).date()
        sorted_daily = sorted(daily_bars, key=lambda bar: bar.timestamp)
        previous_bar = sorted_daily[-1]
        if previous_bar.timestamp.astimezone(_ET).date() == today_et and len(sorted_daily) > 1:
            previous_bar = sorted_daily[-2]
        levels["previous_day_high"] = str(previous_bar.high)
        levels["previous_day_low"] = str(previous_bar.low)

    if minute_bars:
        current_day = datetime.now(tz=_ET).date()
        pre_open = calendar.pre_market_open(current_day)
        session_open = calendar.session_open(current_day)
        if pre_open is not None:
            premarket = [
                bar for bar in minute_bars
                if pre_open <= bar.timestamp.astimezone(_ET) < session_open
            ]
            if premarket:
                levels["premarket_high"] = str(max(bar.high for bar in premarket))
                levels["premarket_low"] = str(min(bar.low for bar in premarket))
    return levels


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _max_decimal_text(values: Any) -> str:
    return str(max(Decimal(str(value)) for value in values))


def _min_decimal_text(values: Any) -> str:
    return str(min(Decimal(str(value)) for value in values))


def _sum_optional_int(values: Any) -> int | None:
    total = 0
    seen = False
    for value in values:
        if value is None:
            continue
        total += int(value)
        seen = True
    return total if seen else None
