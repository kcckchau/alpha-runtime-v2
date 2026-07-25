"""
validate_data.py — Data integrity checks for a given symbol/date.

Checks DBN archives and Parquet files for corruption, gaps, and
cross-schema inconsistencies. Exits non-zero if any hard failures
are detected.

Usage:
    python scripts/validate_data.py --date 2026-07-20
    python scripts/validate_data.py --symbol MNQ-09 --date 2026-07-17 --verbose
    python scripts/validate_data.py --date 2026-07-20 --schemas trades,mbp-1,1m,mbo
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from alpha.config.loader import get_settings

_UTC = timezone.utc

# CME maintenance window: 17:00–18:05 ET daily (21:00–22:05 UTC)
# Include a 5-minute grace at the Globex reopen (18:00 ET) — the book can be
# crossed/thin in the first few ticks after the session restarts.
_MAINTENANCE_START_UTC = (21, 0)   # (hour, minute) — 17:00 ET
_MAINTENANCE_END_UTC = (22, 5)     # (hour, minute) — 18:05 ET

# RTH: 09:30–16:00 ET = 13:30–20:00 UTC
_RTH_START_UTC_H = 13
_RTH_START_UTC_M = 30
_RTH_END_UTC_H = 20

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
WARN = "\033[33mWARN\033[0m"
SKIP = "\033[90mSKIP\033[0m"


def _fmt(label: str, status: str, detail: str = "") -> str:
    line = f"  [{status}] {label}"
    if detail:
        line += f"  — {detail}"
    return line


# ── DBN archive checks ────────────────────────────────────────────────────────

def _dbn_archive_path(raw_root: Path, schema: str, db_symbol: str, d: date) -> Path:
    start_str = datetime(d.year, d.month, d.day, tzinfo=_UTC).strftime("%Y%m%dT%H%M%S")
    end_str = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=_UTC).strftime("%Y%m%dT%H%M%S")
    return raw_root / schema / db_symbol / f"{start_str}_{end_str}.dbn.zst"


def check_dbn_archive(raw_root: Path, schema: str, db_symbol: str, d: date, verbose: bool) -> tuple[bool, list[str]]:
    """Returns (ok, list_of_issues)."""
    import databento as db

    path = _dbn_archive_path(raw_root, schema, db_symbol, d)
    issues: list[str] = []

    if not path.exists():
        return False, [f"MISSING:{path}"]

    try:
        store = db.DBNStore.from_file(str(path))
    except Exception as e:
        return False, [f"failed to open archive: {e}"]

    meta = store.metadata
    nbytes = path.stat().st_size
    if nbytes == 0:
        return False, ["archive is 0 bytes"]

    if verbose:
        print(f"      file: {path}")
        print(f"      size: {nbytes / 1e6:.2f} MB")
        print(f"      schema: {meta.schema}, dataset: {meta.dataset}")

    # Count records and run schema-specific checks
    n_records = 0
    bad_prices = 0
    crossed = 0
    crossed_outside_maintenance = 0
    prev_ts: int | None = None
    out_of_order = 0
    bid_stuck = None

    for rec in store:
        rtype = getattr(rec, 'rtype', None)
        if rtype in (21, 22, 23):  # error/mapping/system
            continue
        n_records += 1

        ts = getattr(rec, 'ts_event', None)
        if ts is not None and prev_ts is not None and ts < prev_ts:
            out_of_order += 1
        if ts is not None:
            prev_ts = ts

        if schema == "mbp-1":
            bid = getattr(rec, 'bid_px_00', None)
            ask = getattr(rec, 'ask_px_00', None)
            if bid is not None and ask is not None:
                bid_f = bid / 1e9
                ask_f = ask / 1e9
                if bid_f <= 0 or ask_f <= 0:
                    bad_prices += 1
                elif bid_f >= ask_f:
                    crossed += 1
                    if ts is not None:
                        ts_dt = datetime.fromtimestamp(ts / 1e9, tz=_UTC)
                        h, m = ts_dt.hour, ts_dt.minute
                        hm = (h, m)
                        in_maint = _MAINTENANCE_START_UTC <= hm < _MAINTENANCE_END_UTC
                        if not in_maint:
                            crossed_outside_maintenance += 1

        elif schema in ("trades", "mbo"):
            px = getattr(rec, 'price', None)
            if px is not None and px / 1e9 <= 0:
                bad_prices += 1

    if n_records == 0:
        issues.append("0 records after filtering metadata records")

    if out_of_order > 0:
        issues.append(f"{out_of_order} out-of-order timestamps")

    if bad_prices > 0:
        issues.append(f"{bad_prices} zero/negative prices")

    if schema == "mbp-1":
        if crossed_outside_maintenance > 0:
            issues.append(f"{crossed_outside_maintenance} crossed markets outside maintenance window")
        elif crossed > 0 and verbose:
            print(f"      {crossed} crossed markets during maintenance window (expected)")

    ok = len(issues) == 0
    if verbose or not ok:
        print(f"      records: {n_records:,}")

    return ok, issues


# ── Parquet checks ────────────────────────────────────────────────────────────

def _parquet_path(parquet_root: Path, schema_dir: str, symbol: str, d: date) -> Path:
    return parquet_root / schema_dir / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}" / "data.parquet"


def check_bars_parquet(parquet_root: Path, symbol: str, d: date, verbose: bool) -> tuple[bool, list[str]]:
    import pandas as pd

    path = _parquet_path(parquet_root, "bars/1m", symbol, d)
    issues: list[str] = []

    if not path.exists():
        return False, [f"missing: {path}"]

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        return False, [f"failed to read: {e}"]

    if len(df) == 0:
        return False, ["0 rows"]

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)

    # OHLC sanity
    bad_ohlc = (
        (df["high"] < df["low"]) |
        (df["high"] < df["open"]) |
        (df["high"] < df["close"]) |
        (df["low"] > df["open"]) |
        (df["low"] > df["close"])
    ).sum()
    if bad_ohlc > 0:
        issues.append(f"{bad_ohlc} bars with invalid OHLC (high < low or similar)")

    # Zero prices
    zero_prices = (df["close"] <= 0).sum()
    if zero_prices > 0:
        issues.append(f"{zero_prices} bars with zero/negative close")

    # RTH gap detection: flag any gap > 5 min during 13:30–20:00 UTC
    gaps = df["timestamp"].diff()
    rth_start = datetime(d.year, d.month, d.day, _RTH_START_UTC_H, _RTH_START_UTC_M, tzinfo=_UTC)
    rth_end = datetime(d.year, d.month, d.day, _RTH_END_UTC_H, tzinfo=_UTC)
    rth_mask = (df["timestamp"] >= rth_start) & (df["timestamp"] <= rth_end)
    rth_gaps = gaps[rth_mask & (gaps > pd.Timedelta("5min"))]
    if len(rth_gaps) > 0:
        for idx in rth_gaps.index:
            prev_ts = df["timestamp"].iloc[idx - 1] if idx > 0 else None
            curr_ts = df["timestamp"].iloc[idx]
            gap_min = rth_gaps[idx].total_seconds() / 60
            issues.append(f"RTH bar gap {gap_min:.0f}min: {prev_ts} → {curr_ts}")

    if verbose:
        print(f"      file: {path}")
        print(f"      rows: {len(df):,}  range: {df['close'].min():.2f}–{df['close'].max():.2f}")

    return len(issues) == 0, issues


def check_trades_parquet(parquet_root: Path, symbol: str, d: date, verbose: bool) -> tuple[bool, list[str]]:
    import pandas as pd

    path = _parquet_path(parquet_root, "trades", symbol, d)
    issues: list[str] = []

    if not path.exists():
        return False, [f"missing: {path}"]

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        return False, [f"failed to read: {e}"]

    if len(df) == 0:
        return False, ["0 rows"]

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)

    bad = (df["price"] <= 0).sum()
    if bad > 0:
        issues.append(f"{bad} trades with zero/negative price")

    df_sorted = df.sort_values("timestamp")
    out_of_order = (df_sorted["timestamp"].diff() < pd.Timedelta(0)).sum()
    if out_of_order > 0:
        issues.append(f"{out_of_order} out-of-order trade timestamps")

    if verbose:
        print(f"      file: {path}")
        print(f"      rows: {len(df):,}  range: {df['price'].min():.2f}–{df['price'].max():.2f}")

    return len(issues) == 0, issues


def cross_validate_bars_trades(
    parquet_root: Path, symbol: str, d: date, verbose: bool
) -> tuple[list[str], list[str]]:
    """
    Returns (hard_failures, warnings).

    Hard failure: bars RTH range is significantly narrower than trades range — bars missing data.
    Warning: bars RTH range extends beyond trades range — trades might be incomplete.
    """
    import pandas as pd

    bars_path = _parquet_path(parquet_root, "bars/1m", symbol, d)
    trades_path = _parquet_path(parquet_root, "trades", symbol, d)

    if not bars_path.exists() or not trades_path.exists():
        return [], []  # can't check

    bars = pd.read_parquet(bars_path)
    trades = pd.read_parquet(trades_path)

    for col in ("high", "low"):
        bars[col] = pd.to_numeric(bars[col], errors="coerce")
    trades["price"] = pd.to_numeric(trades["price"], errors="coerce")
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], format="ISO8601", utc=True)
    trades["timestamp"] = pd.to_datetime(trades["timestamp"], format="ISO8601", utc=True)

    rth_start = datetime(d.year, d.month, d.day, _RTH_START_UTC_H, _RTH_START_UTC_M, tzinfo=_UTC)
    rth_end = datetime(d.year, d.month, d.day, _RTH_END_UTC_H, tzinfo=_UTC)
    rth_bars = bars[(bars["timestamp"] >= rth_start) & (bars["timestamp"] <= rth_end)]
    rth_trades = trades[(trades["timestamp"] >= rth_start) & (trades["timestamp"] <= rth_end)]

    if rth_bars.empty or rth_trades.empty:
        return [], []

    bars_high = rth_bars["high"].max()
    bars_low = rth_bars["low"].min()
    trades_high = rth_trades["price"].max()
    trades_low = rth_trades["price"].min()

    if verbose:
        print(f"      bars RTH range: {bars_low:.2f}–{bars_high:.2f}")
        print(f"      trades RTH range: {trades_low:.2f}–{trades_high:.2f}")

    failures: list[str] = []
    warnings: list[str] = []
    GAP_THRESHOLD = 50.0  # points

    # FAIL: bars miss price action that trades captured (bars have a gap)
    if trades_high - bars_high > GAP_THRESHOLD:
        failures.append(
            f"bars RTH high ({bars_high:.2f}) is {trades_high - bars_high:.2f} pts below trades high ({trades_high:.2f}) — likely missing bars"
        )
    if bars_low - trades_low > GAP_THRESHOLD:
        failures.append(
            f"bars RTH low ({bars_low:.2f}) is {bars_low - trades_low:.2f} pts above trades low ({trades_low:.2f}) — likely missing bars"
        )

    # WARN: bars capture price action trades don't (trades may be incomplete)
    WARN_THRESHOLD = 20.0
    if bars_high - trades_high > WARN_THRESHOLD:
        warnings.append(
            f"bars RTH high ({bars_high:.2f}) exceeds trades high ({trades_high:.2f}) by {bars_high - trades_high:.2f} pts — trades may be incomplete"
        )
    if trades_low - bars_low > WARN_THRESHOLD:
        warnings.append(
            f"bars RTH low ({bars_low:.2f}) is {trades_low - bars_low:.2f} pts below trades low ({trades_low:.2f}) — trades may be incomplete"
        )

    return failures, warnings


# ── DBN vs Parquet cross-check ────────────────────────────────────────────────

_TIMEFRAME_TO_DBN_SCHEMA = {
    "1m": "ohlcv-1m",
    "1h": "ohlcv-1h",
    "1d": "ohlcv-1d",
}


def _find_overlapping_archives(archive_dir: Path, d: date) -> list[Path]:
    """Return OHLCV archives whose timestamp range overlaps the target calendar date."""
    day_start = datetime(d.year, d.month, d.day, tzinfo=_UTC)
    day_end = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=_UTC)
    result = []
    for f in sorted(archive_dir.glob("*.dbn.zst")):
        name = f.name
        stem = name[:-8] if name.endswith(".dbn.zst") else (name[:-4] if name.endswith(".dbn") else None)
        if stem is None:
            continue
        parts = stem.split("_")
        if len(parts) != 2:
            continue
        try:
            arch_start = datetime.strptime(parts[0], "%Y%m%dT%H%M%S").replace(tzinfo=_UTC)
            arch_end = datetime.strptime(parts[1], "%Y%m%dT%H%M%S").replace(tzinfo=_UTC)
        except ValueError:
            continue
        if arch_start <= day_end and arch_end >= day_start:
            result.append(f)
    return result


def check_bars_vs_dbn(
    raw_root: Path, parquet_root: Path, symbol: str, db_symbol: str, d: date, verbose: bool,
    timeframe: str = "1m",
) -> tuple[bool, list[str]]:
    """Verify every bar in local DBN OHLCV archives exists in Parquet.

    Scans all archives overlapping the target calendar date, extracts bar
    timestamps for that date, and checks that Parquet contains all of them.
    Extra bars in Parquet (from the live feed) are not flagged — only bars
    present in DBN but absent from Parquet are hard failures.

    timeframe: "1m", "1h", or "1d"

    Returns (ok, issues).
    """
    import databento as db
    import pandas as pd

    dbn_schema = _TIMEFRAME_TO_DBN_SCHEMA.get(timeframe, f"ohlcv-{timeframe}")
    archive_dir = raw_root / dbn_schema / db_symbol
    if not archive_dir.exists():
        return True, []

    day_start = datetime(d.year, d.month, d.day, tzinfo=_UTC)
    day_end = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=_UTC)

    matching = _find_overlapping_archives(archive_dir, d)
    if not matching:
        return True, []  # no local archives for this date — skip

    # Collect DBN bar timestamps as nanosecond integers (avoids datetime comparison issues)
    day_start_ns = int(day_start.timestamp() * 1e9)
    day_end_ns = int(day_end.timestamp() * 1e9)

    dbn_ts_ns: set[int] = set()
    for f in matching:
        try:
            store = db.DBNStore.from_file(str(f))
        except Exception as e:
            return False, [f"failed to open archive {f.name}: {e}"]
        for rec in store:
            rtype = getattr(rec, "rtype", None)
            if rtype in (21, 22, 23):  # error/mapping/system records
                continue
            ts_ns = getattr(rec, "ts_event", None)
            if ts_ns is None:
                continue
            if day_start_ns <= ts_ns <= day_end_ns:
                dbn_ts_ns.add(ts_ns)

    if not dbn_ts_ns:
        return True, []

    if verbose:
        print(f"      archives checked: {len(matching)}")
        print(f"      DBN bars for {d}: {len(dbn_ts_ns)}")

    parquet_path = _parquet_path(parquet_root, f"bars/{timeframe}", symbol, d)
    if not parquet_path.exists():
        return False, [f"Parquet missing — DBN has {len(dbn_ts_ns)} bars for this date"]

    try:
        df = pd.read_parquet(parquet_path, columns=["timestamp"])
    except Exception as e:
        return False, [f"failed to read Parquet: {e}"]

    parquet_ts_ns = set(pd.to_datetime(df["timestamp"], utc=True).astype("int64").tolist())

    missing = dbn_ts_ns - parquet_ts_ns
    if not missing:
        if verbose:
            print(f"      Parquet bars: {len(parquet_ts_ns)} — all {len(dbn_ts_ns)} DBN bars present")
        return True, []

    sample = [datetime.fromtimestamp(t / 1e9, tz=_UTC).isoformat() for t in sorted(missing)[:3]]
    detail = f"first 3: {sample}" if len(missing) > 3 else str(sample)
    return False, [f"{len(missing)} bar(s) in DBN archive missing from Parquet — {detail}"]


# ── Trades DBN vs Parquet cross-check ────────────────────────────────────────

def check_trades_vs_dbn(
    raw_root: Path, parquet_root: Path, symbol: str, db_symbol: str, d: date, verbose: bool
) -> tuple[bool, list[str]]:
    """Verify every trade in the local DBN trades archive exists in Parquet.

    Matches on sequence number (stored as trade_id in Parquet). Trades in Parquet
    that have no DBN record (e.g. live-feed trades outside the archive window)
    are not flagged — only DBN trades missing from Parquet are hard failures.

    Note: one CME order can produce multiple fills, each with a distinct sequence
    number — these are expected and each should appear once in Parquet.
    """
    import databento as db
    import pandas as pd

    archive_dir = raw_root / "trades" / db_symbol
    if not archive_dir.exists():
        return True, []

    day_start = datetime(d.year, d.month, d.day, tzinfo=_UTC)
    day_end = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=_UTC)

    matching = _find_overlapping_archives(archive_dir, d)
    if not matching:
        return True, []

    dbn_sequences: set[str] = set()
    for f in matching:
        try:
            store = db.DBNStore.from_file(str(f))
        except Exception as e:
            return False, [f"failed to open archive {f.name}: {e}"]
        for rec in store:
            rtype = getattr(rec, "rtype", None)
            if rtype in (21, 22, 23):
                continue
            ts_ns = getattr(rec, "ts_event", None)
            if ts_ns is None:
                continue
            ts = datetime.fromtimestamp(ts_ns / 1e9, tz=_UTC)
            if day_start <= ts <= day_end:
                seq = getattr(rec, "sequence", None)
                if seq is not None:
                    dbn_sequences.add(str(seq))

    if not dbn_sequences:
        return True, []

    if verbose:
        print(f"      archives checked: {len(matching)}")
        print(f"      DBN trades for {d}: {len(dbn_sequences):,}")

    parquet_path = _parquet_path(parquet_root, "trades", symbol, d)
    if not parquet_path.exists():
        return False, [f"Parquet missing — DBN has {len(dbn_sequences):,} trades for this date"]

    try:
        df = pd.read_parquet(parquet_path, columns=["trade_id"])
    except Exception as e:
        return False, [f"failed to read Parquet: {e}"]

    parquet_sequences = set(df["trade_id"].astype(str))

    missing = dbn_sequences - parquet_sequences
    if not missing:
        if verbose:
            print(f"      Parquet trades: {len(parquet_sequences):,} — all {len(dbn_sequences):,} DBN trades present")
        return True, []

    sample = sorted(missing)[:3]
    detail = f"first 3 sequences: {sample}" if len(missing) > 3 else f"sequences: {sample}"
    return False, [f"{len(missing):,} trade(s) in DBN archive missing from Parquet — {detail}"]


# ── M5 bars check ────────────────────────────────────────────────────────────

def check_m5_bars_parquet(parquet_root: Path, symbol: str, d: date, verbose: bool) -> tuple[bool, list[str]]:
    import pandas as pd

    path = _parquet_path(parquet_root, "bars/5m", symbol, d)
    issues: list[str] = []

    if not path.exists():
        return False, [f"missing: {path}"]

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        return False, [f"failed to read: {e}"]

    if len(df) == 0:
        return False, ["0 rows"]

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)

    bad_ohlc = (
        (df["high"] < df["low"]) |
        (df["high"] < df["open"]) |
        (df["high"] < df["close"]) |
        (df["low"] > df["open"]) |
        (df["low"] > df["close"])
    ).sum()
    if bad_ohlc > 0:
        issues.append(f"{bad_ohlc} bars with invalid OHLC (high < low or similar)")

    zero_prices = (df["close"] <= 0).sum()
    if zero_prices > 0:
        issues.append(f"{zero_prices} bars with zero/negative close")

    # Verify bar timestamps are on 5-minute boundaries (resampling bug detection)
    bad_ts = (df["timestamp"].dt.minute % 5 != 0).sum()
    if bad_ts > 0:
        issues.append(f"{bad_ts} bar(s) with non-5-minute timestamps — likely resampled from wrong schema")

    if verbose:
        print(f"      file: {path}")
        print(f"      rows: {len(df):,}  range: {df['close'].min():.2f}–{df['close'].max():.2f}")

    return len(issues) == 0, issues


# ── Quotes existence check ────────────────────────────────────────────────────

def check_quotes_parquet(parquet_root: Path, symbol: str, d: date, verbose: bool) -> tuple[bool, list[str]]:
    import pandas as pd

    path = _parquet_path(parquet_root, "quotes", symbol, d)

    if not path.exists():
        return False, [f"missing: {path}"]

    try:
        pf = __import__("pyarrow.parquet", fromlist=["ParquetFile"]).ParquetFile(path)
        n_rows = pf.metadata.num_rows
    except Exception as e:
        return False, [f"failed to read: {e}"]

    if n_rows == 0:
        return False, ["0 rows"]

    if verbose:
        print(f"      file: {path}")
        print(f"      rows: {n_rows:,}")

    return True, []


# ── Volume profile check ──────────────────────────────────────────────────────

def check_volume_profiles(vp_root: Path, symbol: str, d: date, verbose: bool) -> tuple[bool, list[str]]:
    import json

    issues: list[str] = []
    symbol_dir = vp_root / symbol

    for session in ("rth", "globex"):
        path = symbol_dir / f"{d.isoformat()}_{session}.json"
        if not path.exists():
            issues.append(f"{session}: JSON missing")
            continue
        try:
            with path.open() as f:
                data = json.load(f)
        except Exception as e:
            issues.append(f"{session}: failed to parse JSON — {e}")
            continue

        try:
            poc = float(data["poc"])
            vah = float(data["vah"])
            val = float(data["val"])
            total_vol = int(data["total_volume"])
        except (KeyError, ValueError) as e:
            issues.append(f"{session}: missing or non-numeric field — {e}")
            continue

        if poc <= 0 or vah <= 0 or val <= 0:
            issues.append(f"{session}: zero/negative POC/VAH/VAL (poc={poc}, vah={vah}, val={val})")
        elif not (val <= poc <= vah):
            issues.append(f"{session}: VAL/POC/VAH order invalid (val={val}, poc={poc}, vah={vah})")

        if total_vol <= 0:
            issues.append(f"{session}: total_volume is zero")

        if verbose:
            print(f"      {session}: poc={poc} val={val} vah={vah} vol={total_vol:,} source={data.get('source', '?')}")

    return len(issues) == 0, issues


# ── Main ──────────────────────────────────────────────────────────────────────

_SCHEMA_TO_DB_SYMBOL = {
    "MNQ-09": "MNQ.c.0",
}


def run_checks(symbol: str, d: date, schemas: list[str], verbose: bool) -> int:
    """Returns number of hard failures."""
    settings = get_settings()
    raw_root = settings.databento.raw_archive_root
    parquet_root = settings.databento.parquet_root if hasattr(settings.databento, "parquet_root") else Path("data/parquet")
    db_symbol = _SCHEMA_TO_DB_SYMBOL.get(symbol, symbol.replace("-", ".c.") if "-" in symbol else symbol)

    failures = 0
    warnings = 0

    print(f"\nValidating {symbol} / {d.isoformat()}")
    print("─" * 60)

    # ── DBN archives ──────────────────────────────────────────────
    dbn_schemas = [s for s in schemas if s in ("trades", "mbp-1", "mbo")]
    if dbn_schemas:
        print("\nDBN Archives:")
        if raw_root is None:
            print(f"  [{SKIP}] raw_archive_root not configured")
        else:
            for schema in dbn_schemas:
                ok, issues = check_dbn_archive(raw_root, schema, db_symbol, d, verbose)
                if ok:
                    path = _dbn_archive_path(raw_root, schema, db_symbol, d)
                    size_mb = path.stat().st_size / 1e6
                    print(_fmt(f"dbn/{schema}", PASS, f"{size_mb:.1f} MB"))
                else:
                    is_missing = any(i.startswith("MISSING:") for i in issues)
                    for issue in issues:
                        msg = issue.removeprefix("MISSING:")
                        if is_missing:
                            print(_fmt(f"dbn/{schema}", WARN, f"archive not downloaded: {msg}"))
                            warnings += 1
                        else:
                            print(_fmt(f"dbn/{schema}", FAIL, msg))
                            failures += 1

    # ── Parquet ───────────────────────────────────────────────────
    parquet_schemas = [s for s in schemas if s in ("1m", "5m", "1h", "1d", "trades", "quotes")]
    if parquet_schemas:
        print("\nParquet:")

        if "1m" in parquet_schemas:
            ok, issues = check_bars_parquet(parquet_root, symbol, d, verbose)
            if ok:
                print(_fmt("bars/1m", PASS))
            else:
                for issue in issues:
                    # RTH gaps are hard failures; missing file is also hard
                    severity = FAIL if ("gap" in issue or "missing" in issue or "OHLC" in issue) else WARN
                    print(_fmt("bars/1m", severity, issue))
                    if severity == FAIL:
                        failures += 1
                    else:
                        warnings += 1

        for tf in ("1h", "1d"):
            if tf in parquet_schemas:
                path = _parquet_path(parquet_root, f"bars/{tf}", symbol, d)
                if not path.exists():
                    # H1/D1 may be absent on non-trading days — treat as warning not failure
                    print(_fmt(f"bars/{tf}", WARN, f"missing: {path}"))
                    warnings += 1
                else:
                    try:
                        import pandas as pd
                        df = pd.read_parquet(path)
                        if len(df) == 0:
                            print(_fmt(f"bars/{tf}", FAIL, "0 rows"))
                            failures += 1
                        else:
                            print(_fmt(f"bars/{tf}", PASS, f"{len(df)} bar(s)"))
                    except Exception as e:
                        print(_fmt(f"bars/{tf}", FAIL, f"failed to read: {e}"))
                        failures += 1

        if "5m" in parquet_schemas:
            ok, issues = check_m5_bars_parquet(parquet_root, symbol, d, verbose)
            if ok:
                print(_fmt("bars/5m", PASS))
            else:
                for issue in issues:
                    severity = FAIL if ("missing" in issue or "OHLC" in issue or "timestamps" in issue) else WARN
                    print(_fmt("bars/5m", severity, issue))
                    if severity == FAIL:
                        failures += 1
                    else:
                        warnings += 1

        if "trades" in parquet_schemas:
            ok, issues = check_trades_parquet(parquet_root, symbol, d, verbose)
            if ok:
                print(_fmt("trades parquet", PASS))
            else:
                for issue in issues:
                    print(_fmt("trades parquet", FAIL, issue))
                failures += 1

        if "quotes" in parquet_schemas:
            ok, issues = check_quotes_parquet(parquet_root, symbol, d, verbose)
            if ok:
                print(_fmt("quotes parquet", PASS))
            else:
                for issue in issues:
                    # Missing quotes = warning (not all days have quotes downloaded)
                    severity = WARN if "missing" in issue else FAIL
                    print(_fmt("quotes parquet", severity, issue))
                    if severity == FAIL:
                        failures += 1
                    else:
                        warnings += 1

    # ── DBN vs Parquet ────────────────────────────────────────────
    bar_tfs = [s for s in ("1m", "1h", "1d") if s in schemas]
    has_trades_xcheck = "trades" in schemas and raw_root is not None
    if (bar_tfs or has_trades_xcheck) and raw_root is not None:
        print("\nDBN vs Parquet:")
        for tf in bar_tfs:
            dbn_schema = _TIMEFRAME_TO_DBN_SCHEMA.get(tf, f"ohlcv-{tf}")
            ok, issues = check_bars_vs_dbn(raw_root, parquet_root, symbol, db_symbol, d, verbose, timeframe=tf)
            if ok:
                print(_fmt(f"bars/{tf} vs {dbn_schema} archive", PASS))
            else:
                for issue in issues:
                    print(_fmt(f"bars/{tf} vs {dbn_schema} archive", FAIL, issue))
                    failures += 1
        if has_trades_xcheck:
            ok, issues = check_trades_vs_dbn(raw_root, parquet_root, symbol, db_symbol, d, verbose)
            if ok:
                print(_fmt("trades vs dbn/trades archive", PASS))
            else:
                for issue in issues:
                    print(_fmt("trades vs dbn/trades archive", FAIL, issue))
                    failures += 1

    # ── Cross-validation ──────────────────────────────────────────
    if "1m" in schemas and "trades" in schemas:
        print("\nCross-validation:")
        xv_failures, xv_warnings = cross_validate_bars_trades(parquet_root, symbol, d, verbose)
        if not xv_failures and not xv_warnings:
            print(_fmt("bars vs trades price range", PASS))
        for issue in xv_failures:
            print(_fmt("bars vs trades price range", FAIL, issue))
        for issue in xv_warnings:
            print(_fmt("bars vs trades price range", WARN, issue))
            warnings += 1
        if xv_failures:
            failures += 1

    # ── Volume profiles ───────────────────────────────────────────
    if "vp" in schemas:
        print("\nVolume Profiles:")
        vp_root_path = _REPO / "data" / "volume_profiles"
        ok, issues = check_volume_profiles(vp_root_path, symbol, d, verbose)
        if ok:
            print(_fmt("volume profiles", PASS))
        else:
            for issue in issues:
                # Missing = warning (non-trading days have no profile)
                severity = WARN if "missing" in issue else FAIL
                print(_fmt("volume profiles", severity, issue))
                if severity == FAIL:
                    failures += 1
                else:
                    warnings += 1

    print()
    if failures == 0 and warnings == 0:
        print(f"Result: {PASS} — all checks passed")
    elif failures == 0:
        print(f"Result: {WARN} — {warnings} warning(s), no hard failures")
    else:
        print(f"Result: {FAIL} — {failures} failure(s), {warnings} warning(s)")

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default="MNQ-09")
    parser.add_argument("--date", required=True, help="Date to validate YYYY-MM-DD")
    parser.add_argument(
        "--schemas", default="1m,5m,1h,1d,trades,quotes,vp,mbp-1,mbo",
        help="Comma-separated schemas to check (default: 1m,5m,1h,1d,trades,quotes,vp,mbp-1,mbo)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    d = date.fromisoformat(args.date)
    schemas = [s.strip() for s in args.schemas.split(",") if s.strip()]

    failures = run_checks(args.symbol, d, schemas, args.verbose)
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
