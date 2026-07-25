"""
backfill.py — Stage 2 of the download-raw → backfill → backtest pipeline

Fetches missing bar gaps (always) and, with --ticks, trades + mbp-1 quotes,
writing everything to the local Parquet cache. Reuses a local raw DBN
archive from scripts/download_raw.py instead of paying for another
Databento call, if one exists for the exact symbol/schema/day requested.

Running multiple times is safe: bars are gap-diffed against what's already
stored, and --ticks days are skipped if already on disk (use --force to
intentionally refresh a day).

Moved out of `alpha backfill` (main.py) since this is a one-shot batch job
that exits when done, not a long-running service like `alpha run`/`alpha api`
— it belongs with the other one-shot data scripts (backtest.py, this file,
download_raw.py), not the service-starting CLI.

Usage:
    python scripts/backfill.py --start 2026-07-07 --end 2026-07-07 --ticks
    python scripts/backfill.py --symbol MNQ-09 --start 2026-07-07 --end 2026-07-07 --ticks --force
    python scripts/backfill.py --dry-run

Timeframe windows (when --start is omitted):
    1m  ~3 days    (EMA9/21 + VWAP session warmup)
    5m  ~7 days    (EMA9/21 + VWAP session warmup)
    1h  ~250 days  (EMA9/21/50 + SMA100/200)
    1d  ~1500 days (EMA9/21 + SMA50/100/200)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from alpha.config.loader import get_settings
from alpha.config.settings import AlphaSettings
from alpha.models.enums import RuntimeMode

_UTC = timezone.utc


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )


def _print_dry_run(settings: AlphaSettings, ticks: bool) -> None:
    hist = settings.historical
    now = datetime.now(_UTC)
    end_dt = (
        datetime(
            settings.replay.end_date.year, settings.replay.end_date.month, settings.replay.end_date.day,
            23, 59, 59, tzinfo=_UTC,
        )
        if settings.replay.end_date
        else now
    )
    m1_days = max(3, hist.minute1_warmup_bars // 390 + 1)
    m5_days = max(7, hist.minute5_warmup_bars // 78 + 2)
    h1_days = int(hist.hourly_warmup_bars * 0.22) + 30
    d1_days = int(hist.daily_warmup_bars * 1.5)
    explicit_start = settings.replay.start_date
    print("Dry-run backfill plan (no requests will be made):")
    print(f"  Symbols : {settings.runtime.symbols}")
    print(f"  End     : {end_dt.date()}")
    print(f"  1m start: {(end_dt - timedelta(days=m1_days)).date()} ({m1_days}d window) explicit={explicit_start}")
    print(f"  5m start: {(end_dt - timedelta(days=m5_days)).date()} ({m5_days}d window) explicit={explicit_start}")
    print(f"  1h start: {(end_dt - timedelta(days=h1_days)).date()} ({h1_days}d window) explicit={explicit_start}")
    print(f"  1d start: {(end_dt - timedelta(days=d1_days)).date()} ({d1_days}d window) explicit={explicit_start}")
    if ticks:
        print(f"  ticks   : trades + quotes(mbp-1) for {explicit_start} → {end_dt.date()}")


async def _run_backfill(settings: AlphaSettings, bars: bool, ticks: bool, force: bool) -> None:
    from alpha.engines.backfill.engine import BackfillEngine

    engine = BackfillEngine(settings, fetch_bars=bars, fetch_ticks=ticks, force=force)
    try:
        await engine.initialize()
        await engine.start()
    finally:
        await engine.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default=None, help="Ticker to backfill, e.g. MNQ-09 (overrides runtime.symbols config)")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default: warmup-driven per timeframe)")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched without making requests")
    parser.add_argument(
        "--ticks", action="store_true",
        help="Also fetch and save raw trades + top-of-book quotes (MBP-1) for --start..--end. "
             "Requires --start (ticks are not warmup-driven).",
    )
    parser.add_argument(
        "--ticks-only", action="store_true",
        help="Skip the bar backfill section entirely — only fetch/save trades + quotes for "
             "--start..--end. Implies --ticks. Useful when bars were already backfilled "
             "separately in the same run (e.g. daily_update.py) and re-running the full "
             "M1/M5/H1/D1 fetch a second time would just be wasted API calls.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Refetch --ticks days even if already stored (default: skip days already on disk). "
             "Trades upsert safely by trade_id; quotes have no per-row key, so the day's file is "
             "deleted and rewritten from scratch.",
    )
    args = parser.parse_args()

    settings = get_settings()
    _configure_logging(settings.runtime.log_level)

    overrides = settings.model_dump()
    overrides["runtime"]["mode"] = RuntimeMode.HISTORICAL_BACKFILL
    if args.symbol:
        overrides["runtime"]["symbols"] = [args.symbol]
    # Always set explicitly (not just when passed) so an ambient REPLAY__START_DATE/
    # END_DATE left over from a REPLAY-mode session can't silently hijack the
    # "no --start/--end" warmup-driven default window.
    overrides["replay"]["start_date"] = args.start or None
    overrides["replay"]["end_date"] = args.end or None
    backfill_settings = AlphaSettings.model_validate(overrides)

    fetch_ticks = args.ticks or args.ticks_only
    fetch_bars = not args.ticks_only

    if fetch_ticks and not args.start:
        parser.error("--ticks/--ticks-only requires --start (tick backfill is not warmup-driven)")

    if args.dry_run:
        _print_dry_run(backfill_settings, fetch_ticks)
        return

    asyncio.run(_run_backfill(backfill_settings, fetch_bars, fetch_ticks, args.force))


if __name__ == "__main__":
    main()
