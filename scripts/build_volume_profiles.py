"""
Build and store volume profiles for all available RTH sessions.

Reads sealed 1m bars from Parquet, computes RTH and Globex profiles,
and stores them as JSON in data/volume_profiles/.

Usage:
    python scripts/build_volume_profiles.py --symbol MNQ-09
    python scripts/build_volume_profiles.py --symbol MNQ-09 --start 2026-06-01 --end 2026-07-17
    python scripts/build_volume_profiles.py --symbol MNQ-09 --date 2026-07-03

Output:
    data/volume_profiles/{symbol}/{date}_rth.json
    data/volume_profiles/{symbol}/{date}_globex.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from alpha.features.volume_profile import VolumeProfileBuilder
from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, DataSourceId

_ET = ZoneInfo("America/New_York")
_RTH_OPEN = (9, 30)    # ET
_RTH_CLOSE = (16, 0)   # ET
_GLOBEX_START = (18, 0) # ET prior day


def _load_bars(parquet_path: Path) -> list[Bar]:
    """Load bars from a single Parquet file."""
    if not parquet_path.exists():
        return []
    df = pd.read_parquet(parquet_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    bars = []
    for _, row in df.iterrows():
        bars.append(Bar(
            symbol=row["symbol"],
            timeframe=BarTimeframe.M1,
            timestamp=row["timestamp"].to_pydatetime(),
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            vwap=Decimal(str(row["vwap"])) if pd.notna(row.get("vwap")) else None,
            source=DataSourceId.DATABENTO,
        ))
    return bars


def _parquet_path(data_dir: Path, symbol: str, d: date) -> Path:
    return data_dir / "bars" / "1m" / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}" / "data.parquet"


def _filter_rth(bars: list[Bar], session_date: date) -> list[Bar]:
    """Keep only RTH bars for the given session date (09:30–16:00 ET)."""
    rth_open = datetime(session_date.year, session_date.month, session_date.day, *_RTH_OPEN, tzinfo=_ET)
    rth_close = datetime(session_date.year, session_date.month, session_date.day, *_RTH_CLOSE, tzinfo=_ET)
    return [b for b in bars if rth_open <= b.timestamp.astimezone(_ET) < rth_close]


def _filter_globex(bars_prior: list[Bar], bars_current: list[Bar], session_date: date) -> list[Bar]:
    """
    Keep Globex bars: 18:00 ET prior day → 09:30 ET current day.
    Requires bars from both the prior calendar day and current calendar day.
    """
    globex_start = datetime(
        session_date.year, session_date.month, session_date.day,
        *_GLOBEX_START, tzinfo=_ET,
    ) - timedelta(days=1)
    rth_open = datetime(session_date.year, session_date.month, session_date.day, *_RTH_OPEN, tzinfo=_ET)

    all_bars = bars_prior + bars_current
    return [b for b in all_bars if globex_start <= b.timestamp.astimezone(_ET) < rth_open]


def _profile_path(out_dir: Path, symbol: str, d: date, session_type: str) -> Path:
    return out_dir / symbol / f"{d}_{session_type}.json"


def _save_profile(profile_path: Path, profile: "VolumeProfile") -> None:  # type: ignore[name-defined]
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    data = profile.model_dump()
    # Serialize Decimal and date
    data["poc"] = str(data["poc"])
    data["vah"] = str(data["vah"])
    data["val"] = str(data["val"])
    data["hvn_levels"] = [str(x) for x in data["hvn_levels"]]
    data["lvn_levels"] = [str(x) for x in data["lvn_levels"]]
    data["session_date"] = str(data["session_date"])
    with open(profile_path, "w") as f:
        json.dump(data, f, indent=2)


def build_date(
    data_dir: Path,
    out_dir: Path,
    symbol: str,
    session_date: date,
    builder: VolumeProfileBuilder,
    force: bool = False,
) -> None:
    rth_out = _profile_path(out_dir, symbol, session_date, "rth")
    globex_out = _profile_path(out_dir, symbol, session_date, "globex")

    rth_needed = force or not rth_out.exists()
    globex_needed = force or not globex_out.exists()

    if not rth_needed and not globex_needed:
        print(f"  {session_date}: skipped (already exists)")
        return

    bars_current = _load_bars(_parquet_path(data_dir, symbol, session_date))
    if not bars_current:
        print(f"  {session_date}: no data, skipping")
        return

    if rth_needed:
        rth_bars = _filter_rth(bars_current, session_date)
        if rth_bars:
            profile = builder.build(rth_bars, symbol, session_date, "rth")
            _save_profile(rth_out, profile)
            print(f"  {session_date} RTH: {len(rth_bars)} bars, POC={profile.poc}, VA=[{profile.val}, {profile.vah}], vol={profile.total_volume:,}")
        else:
            print(f"  {session_date} RTH: no RTH bars found")

    if globex_needed:
        prior_day = session_date - timedelta(days=1)
        bars_prior = _load_bars(_parquet_path(data_dir, symbol, prior_day))
        globex_bars = _filter_globex(bars_prior, bars_current, session_date)
        if globex_bars:
            profile = builder.build(globex_bars, symbol, session_date, "globex")
            _save_profile(globex_out, profile)
            print(f"  {session_date} Globex: {len(globex_bars)} bars, POC={profile.poc}, VA=[{profile.val}, {profile.vah}], vol={profile.total_volume:,}")
        else:
            print(f"  {session_date} Globex: no overnight bars found")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build volume profiles from 1m bars")
    parser.add_argument("--symbol", default="MNQ-09")
    parser.add_argument("--date", help="Single date YYYY-MM-DD")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--bin-size", type=float, default=1.0, help="Bin size in points (default 1.0)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing profiles")
    args = parser.parse_args()

    data_dir = ROOT / "data" / "parquet"
    out_dir = ROOT / "data" / "volume_profiles"
    builder = VolumeProfileBuilder(bin_size=Decimal(str(args.bin_size)))

    if args.date:
        dates = [date.fromisoformat(args.date)]
    else:
        # Discover available dates from parquet directory
        bar_dir = data_dir / "bars" / "1m" / args.symbol
        if not bar_dir.exists():
            print(f"No 1m bar data found for {args.symbol} at {bar_dir}")
            sys.exit(1)

        all_dates = []
        for f in sorted(bar_dir.rglob("data.parquet")):
            parts = f.parts
            year = next((p for p in parts if p.startswith("year=")), None)
            month = next((p for p in parts if p.startswith("month=")), None)
            day = next((p for p in parts if p.startswith("day=")), None)
            if year and month and day:
                d = date(int(year[5:]), int(month[6:]), int(day[4:]))
                all_dates.append(d)

        start = date.fromisoformat(args.start) if args.start else all_dates[0]
        end = date.fromisoformat(args.end) if args.end else all_dates[-1]
        dates = [d for d in all_dates if start <= d <= end]

    print(f"Building volume profiles for {args.symbol}, {len(dates)} session(s), bin_size={args.bin_size}")
    for d in dates:
        build_date(data_dir, out_dir, args.symbol, d, builder, force=args.force)

    print(f"\nDone. Profiles saved to {out_dir}/{args.symbol}/")


if __name__ == "__main__":
    main()
