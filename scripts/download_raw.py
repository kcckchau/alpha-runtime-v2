"""
download_raw.py — Stage 1 of the download-raw → backfill → backtest pipeline

Fetches raw Databento data and archives it to disk as .dbn.zst files under
DatabentoSettings.raw_archive_root (default data/dbn_raw/). Never touches
Parquet or StorageEngine — this step is purely "get the bytes onto local
disk," so a later `scripts/backfill.py` run for the same symbol/schema/day
decodes from the local archive instead of paying for another Databento call.

Iterates day-by-day using the exact same UTC midnight-to-midnight boundary
backfill.py uses, so the archive keys line up and are actually reusable —
a mismatched start/end window won't hit the cache even if it covers the
same calendar day.

Usage:
    python scripts/download_raw.py --start 2026-07-07 --end 2026-07-07 --ticks
    python scripts/download_raw.py --symbol MNQ-09 --start 2026-07-07 --end 2026-07-07 --schemas trades,mbp-1
    python scripts/download_raw.py --start 2026-07-01 --end 2026-07-10 --schemas 1m,1h,1d
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.loader import get_settings
from alpha.core.registry import SymbolRegistry
from alpha.engines.historical.sources.databento import DatabentoHistoricalDataSource
from alpha.instruments import resolve_symbol
from alpha.models.enums import BarTimeframe

_UTC = timezone.utc

_TIMEFRAME_SCHEMAS = {
    "1s": BarTimeframe.S1,
    "1m": BarTimeframe.M1,
    "5m": BarTimeframe.M5,
    "1h": BarTimeframe.H1,
    "1d": BarTimeframe.D1,
}


async def _download_day(
    source: DatabentoHistoricalDataSource,
    symbol: str,
    d: date,
    schemas: list[str],
) -> None:
    day_start = datetime(d.year, d.month, d.day, tzinfo=_UTC)
    day_end = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=_UTC)

    for schema in schemas:
        if schema in _TIMEFRAME_SCHEMAS:
            n = 0
            async for _ in source.fetch_bars(symbol, _TIMEFRAME_SCHEMAS[schema], day_start, day_end):
                n += 1
            print(f"  {d.isoformat()} {schema:6s} bars   archived {n:>10,}")
        elif schema == "trades":
            n = 0
            async for _ in source.fetch_trades(symbol, day_start, day_end):
                n += 1
            print(f"  {d.isoformat()} trades         archived {n:>10,}")
        elif schema == "mbp-1":
            n = 0
            async for _ in source.fetch_quotes(symbol, day_start, day_end):
                n += 1
            print(f"  {d.isoformat()} mbp-1          archived {n:>10,}")
        else:
            print(f"  {d.isoformat()} {schema}: unsupported — only {list(_TIMEFRAME_SCHEMAS)} + trades/mbp-1 are wired")


async def _run(symbol: str, start: date, end: date, schemas: list[str]) -> None:
    settings = get_settings()
    registry = SymbolRegistry()
    sym_obj = resolve_symbol(symbol)
    registry.register(sym_obj)
    calendar = calendar_for_symbol(sym_obj)
    source = DatabentoHistoricalDataSource(registry, settings.databento)

    if settings.databento.raw_archive_root is None:
        print("WARNING: DatabentoSettings.raw_archive_root is None — archiving is disabled, this will do nothing useful.")
        return

    trading_days = calendar.trading_days(start, end)
    print(f"Downloading raw DBN for {symbol} | {start} → {end} | schemas={schemas} | {len(trading_days)} trading day(s)")
    print(f"Archive root: {settings.databento.raw_archive_root}")
    print()

    for d in trading_days:
        await _download_day(source, symbol, d, schemas)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default="MNQ-09", help="Ticker (default: MNQ-09)")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--schemas", default=None,
        help="Comma-separated: 1s,1m,5m,1h,1d,trades,mbp-1 (default with --ticks: trades,mbp-1; otherwise 1m)",
    )
    parser.add_argument(
        "--ticks", action="store_true",
        help="Shorthand for --schemas trades,mbp-1 (ignored if --schemas is also given)",
    )
    args = parser.parse_args()

    if args.schemas:
        schemas = [s.strip() for s in args.schemas.split(",") if s.strip()]
    elif args.ticks:
        schemas = ["trades", "mbp-1"]
    else:
        schemas = ["1m"]

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    asyncio.run(_run(args.symbol, start, end, schemas))
    print("\nDone.")


if __name__ == "__main__":
    main()
