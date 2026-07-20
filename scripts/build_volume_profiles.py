"""
Build and store volume profiles for all available RTH sessions.

Prefers Databento trade data (event_type="trade") when available for exact
volume placement and delta. Falls back to 1m bars with uniform distribution
when trades are absent.

Usage:
    python scripts/build_volume_profiles.py --symbol MNQ-09
    python scripts/build_volume_profiles.py --symbol MNQ-09 --start 2026-06-01 --end 2026-07-17
    python scripts/build_volume_profiles.py --symbol MNQ-09 --date 2026-07-03
    python scripts/build_volume_profiles.py --symbol MNQ-09 --bars-only   # force bar-based

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
from alpha.features.volume_profile_loader import VolumeProfileLoader, _serialize_profile
from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, DataSourceId, TakerSide
from alpha.models.trade import Trade

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


def _load_trades(parquet_path: Path) -> list[Trade]:
    """Load trades from a single Parquet file."""
    if not parquet_path.exists():
        return []
    df = pd.read_parquet(parquet_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
    trades = []
    for _, row in df.iterrows():
        side_str = str(row.get("taker_side", "")).lower()
        side = TakerSide.BUY if side_str == "buy" else (TakerSide.SELL if side_str == "sell" else TakerSide.UNKNOWN)
        trades.append(Trade(
            symbol=row["symbol"],
            timestamp=row["timestamp"].to_pydatetime(),
            price=Decimal(str(row["price"])),
            size=int(row["size"]),
            taker_side=side,
            trade_id=str(row.get("trade_id", "")),
            source=DataSourceId.DATABENTO,
        ))
    return trades


def _bars_parquet_path(data_dir: Path, symbol: str, d: date) -> Path:
    return data_dir / "bars" / "1m" / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}" / "data.parquet"


def _trades_parquet_path(data_dir: Path, symbol: str, d: date) -> Path:
    return data_dir / "trades" / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}" / "data.parquet"


def _rth_window(session_date: date) -> tuple[datetime, datetime]:
    rth_open = datetime(session_date.year, session_date.month, session_date.day, *_RTH_OPEN, tzinfo=_ET)
    rth_close = datetime(session_date.year, session_date.month, session_date.day, *_RTH_CLOSE, tzinfo=_ET)
    return rth_open, rth_close


def _globex_window(session_date: date) -> tuple[datetime, datetime]:
    globex_start = datetime(
        session_date.year, session_date.month, session_date.day,
        *_GLOBEX_START, tzinfo=_ET,
    ) - timedelta(days=1)
    rth_open = datetime(session_date.year, session_date.month, session_date.day, *_RTH_OPEN, tzinfo=_ET)
    return globex_start, rth_open


def _filter_rth(bars: list[Bar], session_date: date) -> list[Bar]:
    """Keep only RTH bars for the given session date (09:30–16:00 ET)."""
    lo, hi = _rth_window(session_date)
    return [b for b in bars if lo <= b.timestamp.astimezone(_ET) < hi]


def _filter_rth_trades(trades: list[Trade], session_date: date) -> list[Trade]:
    lo, hi = _rth_window(session_date)
    return [t for t in trades if lo <= t.timestamp.astimezone(_ET) < hi]


def _filter_globex(bars_prior: list[Bar], bars_current: list[Bar], session_date: date) -> list[Bar]:
    """Keep Globex bars: 18:00 ET prior day → 09:30 ET current day."""
    lo, hi = _globex_window(session_date)
    return [b for b in bars_prior + bars_current if lo <= b.timestamp.astimezone(_ET) < hi]


def _filter_globex_trades(
    trades_prior: list[Trade], trades_current: list[Trade], session_date: date
) -> list[Trade]:
    lo, hi = _globex_window(session_date)
    return [t for t in trades_prior + trades_current if lo <= t.timestamp.astimezone(_ET) < hi]


def _globex_trades_complete(trades: list[Trade], session_date: date) -> bool:
    """
    Return True only if trades span both sides of the Globex session.

    A Globex session from prior 18:00 ET to current 09:30 ET has two halves:
      prior-day half:   18:00–23:59 ET
      current-day half: 00:00–09:29 ET

    If prior-day trade data is missing, trades will only cover the current-day
    half, producing a silently incomplete profile. We reject it rather than
    storing a partial profile that looks valid.

    Gate: require at least one trade before midnight ET of session_date.
    """
    if not trades:
        return False
    midnight = datetime(session_date.year, session_date.month, session_date.day, 0, 0, tzinfo=_ET)
    return any(t.timestamp.astimezone(_ET) < midnight for t in trades)


def _profile_path(out_dir: Path, symbol: str, d: date, session_type: str) -> Path:
    return out_dir / symbol / f"{d}_{session_type}.json"


def _save_profile(profile_path: Path, profile: "VolumeProfile") -> None:  # type: ignore[name-defined]
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    with open(profile_path, "w") as f:
        json.dump(_serialize_profile(profile), f, indent=2)


def build_date(
    data_dir: Path,
    out_dir: Path,
    symbol: str,
    session_date: date,
    builder: VolumeProfileBuilder,
    force: bool = False,
    bars_only: bool = False,
) -> None:
    rth_out = _profile_path(out_dir, symbol, session_date, "rth")
    globex_out = _profile_path(out_dir, symbol, session_date, "globex")

    rth_needed = force or not rth_out.exists()
    globex_needed = force or not globex_out.exists()

    if not rth_needed and not globex_needed:
        print(f"  {session_date}: skipped (already exists)")
        return

    # Load current-day data — prefer trades, fall back to bars
    trades_current: list[Trade] = []
    bars_current: list[Bar] = []

    use_trades = not bars_only and _trades_parquet_path(data_dir, symbol, session_date).exists()
    if use_trades:
        trades_current = _load_trades(_trades_parquet_path(data_dir, symbol, session_date))
        if not trades_current:
            use_trades = False

    if not use_trades:
        bars_current = _load_bars(_bars_parquet_path(data_dir, symbol, session_date))
        if not bars_current:
            print(f"  {session_date}: no data, skipping")
            return

    if rth_needed:
        if use_trades:
            rth_items = _filter_rth_trades(trades_current, session_date)
            if rth_items:
                profile = builder.build_from_trades(rth_items, symbol, session_date, "rth")
                _save_profile(rth_out, profile)
                print(f"  {session_date} RTH [trades]: {len(rth_items):,} ticks, POC={profile.poc}, VA=[{profile.val}, {profile.vah}], vol={profile.total_volume:,}")
            else:
                print(f"  {session_date} RTH: no RTH trades found")
        else:
            rth_bars = _filter_rth(bars_current, session_date)
            if rth_bars:
                profile = builder.build(rth_bars, symbol, session_date, "rth")
                _save_profile(rth_out, profile)
                print(f"  {session_date} RTH [bars]: {len(rth_bars)} bars, POC={profile.poc}, VA=[{profile.val}, {profile.vah}], vol={profile.total_volume:,}")
            else:
                print(f"  {session_date} RTH: no RTH bars found")

    if globex_needed:
        prior_day = session_date - timedelta(days=1)
        if use_trades:
            trades_prior = _load_trades(_trades_parquet_path(data_dir, symbol, prior_day))
            globex_items = _filter_globex_trades(trades_prior, trades_current, session_date)
            if not globex_items:
                print(f"  {session_date} Globex: no overnight trades found")
            elif not _globex_trades_complete(globex_items, session_date):
                # Prior-day trades missing — profile would only cover 00:00–09:30,
                # silently omitting the 18:00–23:59 session. Fall back to bars.
                print(f"  {session_date} Globex [trades]: incomplete (prior-day missing), falling back to bars")
                bars_prior = _load_bars(_bars_parquet_path(data_dir, symbol, prior_day))
                bars_curr = _load_bars(_bars_parquet_path(data_dir, symbol, session_date))
                globex_bars = _filter_globex(bars_prior, bars_curr, session_date)
                if globex_bars:
                    profile = builder.build(globex_bars, symbol, session_date, "globex")
                    _save_profile(globex_out, profile)
                    print(f"  {session_date} Globex [bars fallback]: {len(globex_bars)} bars, POC={profile.poc}, VA=[{profile.val}, {profile.vah}], vol={profile.total_volume:,}")
                else:
                    print(f"  {session_date} Globex: no bar fallback data either, skipping")
            else:
                profile = builder.build_from_trades(globex_items, symbol, session_date, "globex")
                _save_profile(globex_out, profile)
                print(f"  {session_date} Globex [trades]: {len(globex_items):,} ticks, POC={profile.poc}, VA=[{profile.val}, {profile.vah}], vol={profile.total_volume:,}")
        else:
            bars_prior = _load_bars(_bars_parquet_path(data_dir, symbol, prior_day))
            globex_bars = _filter_globex(bars_prior, bars_current, session_date)
            if globex_bars:
                profile = builder.build(globex_bars, symbol, session_date, "globex")
                _save_profile(globex_out, profile)
                print(f"  {session_date} Globex [bars]: {len(globex_bars)} bars, POC={profile.poc}, VA=[{profile.val}, {profile.vah}], vol={profile.total_volume:,}")
            else:
                print(f"  {session_date} Globex: no overnight bars found")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build volume profiles (trades preferred, bars fallback)")
    parser.add_argument("--symbol", default="MNQ-09")
    parser.add_argument("--date", help="Single date YYYY-MM-DD")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--bin-size", type=float, default=1.0, help="Bin size in points (default 1.0)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing profiles")
    parser.add_argument("--bars-only", action="store_true", help="Force bar-based profiles even if trades exist")
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

    mode = "bars-only" if args.bars_only else "trades-preferred"
    print(f"Building volume profiles for {args.symbol}, {len(dates)} session(s), bin_size={args.bin_size}, mode={mode}")
    for d in dates:
        build_date(data_dir, out_dir, args.symbol, d, builder, force=args.force, bars_only=args.bars_only)

    print(f"\nDone. Profiles saved to {out_dir}/{args.symbol}/")


if __name__ == "__main__":
    main()
