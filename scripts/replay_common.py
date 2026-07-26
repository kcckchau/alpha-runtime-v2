"""
replay_common.py — Shared infrastructure for backtest.py and replay_day.py.

Not a runnable script — imported by both to avoid duplicated Parquet
bar-loading logic (previously copy-pasted as backtest.py:_load_parquet_bars
and replay_day.py:_load_from_parquet).
"""
from __future__ import annotations

from datetime import date, timedelta

from alpha.config.settings import AlphaSettings
from alpha.engines.storage.engine import StorageEngine
from alpha.engines.storage.parquet import ParquetStore
from alpha.models.enums import BarTimeframe
from alpha.models.events import BarEvent


def load_m1_bars(
    symbol: str,
    start: date,
    end: date,
    settings: AlphaSettings,
    *,
    skip_read_errors: bool = True,
) -> list[BarEvent]:
    """Load M1 bars from Parquet for [start, end] inclusive, sorted by timestamp.

    skip_read_errors=True (backtest.py's original behavior) silently skips a day
    on any read failure — appropriate when scanning many days in one run, since a
    single missing/corrupt day shouldn't abort the whole range.

    skip_read_errors=False (replay_day.py's original behavior) lets a read failure
    raise — appropriate for a single-day interactive debug run, where a silent
    empty result would be more confusing than a loud error.
    """
    parquet = ParquetStore(settings.storage)
    bars: list[BarEvent] = []
    d = start
    while d <= end:
        if skip_read_errors:
            try:
                table = parquet.read_range(f"bars/{BarTimeframe.M1}", symbol, d, d)
            except Exception:
                d += timedelta(days=1)
                continue
        else:
            table = parquet.read_range(f"bars/{BarTimeframe.M1}", symbol, d, d)
        for row in table.to_pylist():
            bar = StorageEngine._row_to_bar_event(row, BarTimeframe.M1)
            if bar.symbol != symbol:
                bar = bar.model_copy(update={"symbol": symbol})
            bars.append(bar)
        d += timedelta(days=1)
    bars.sort(key=lambda b: b.timestamp)
    return bars
