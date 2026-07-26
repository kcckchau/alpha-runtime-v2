"""
validate_history.py — Run data integrity checks across all stored trading dates.

Discovers dates from the bars/1m Parquet partition and runs validate_data.py
checks for each one. Prints a summary table and exits non-zero if any hard
failures are detected.

Usage:
    python scripts/validate_history.py
    python scripts/validate_history.py --symbol MNQ-09 --start 2026-01-01
    python scripts/validate_history.py --full          # include DBN streaming cross-checks (slow)
    python scripts/validate_history.py --fail-only     # only print dates with failures
"""

from __future__ import annotations

import argparse
import contextlib
import io
import sys
from datetime import date
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from alpha.config.loader import get_settings

# Re-use run_checks from validate_data without subprocess overhead
sys.path.insert(0, str(_REPO / "scripts"))
from validate_data import run_checks  # noqa: E402

_FAST_SCHEMAS = ["1m", "5m", "1h", "1d", "trades", "vp"]
_FULL_SCHEMAS = ["1m", "5m", "1h", "1d", "trades", "vp", "mbp-1", "mbo"]

# ANSI colours (reuse from validate_data)
_PASS = "\033[32mPASS\033[0m"
_FAIL = "\033[31mFAIL\033[0m"
_WARN = "\033[33mWARN\033[0m"


def _discover_dates(symbol: str, parquet_root: Path, start: date | None, end: date | None) -> list[date]:
    base = parquet_root / "bars" / "1m" / symbol
    if not base.exists():
        return []
    dates: list[date] = []
    for y in sorted(base.glob("year=*")):
        for m in sorted(y.glob("month=*")):
            for d in sorted(m.glob("day=*")):
                try:
                    dt = date(int(y.name[5:]), int(m.name[6:]), int(d.name[4:]))
                except (ValueError, IndexError):
                    continue
                if start and dt < start:
                    continue
                if end and dt > end:
                    continue
                dates.append(dt)
    return dates


def _extract_failures(output: str) -> list[str]:
    """Pull FAIL lines out of captured run_checks() output."""
    lines = []
    for line in output.splitlines():
        stripped = line.strip()
        if "FAIL" in stripped and stripped.startswith("["):
            # Strip ANSI escape codes for clean storage
            import re
            clean = re.sub(r"\033\[[0-9;]*m", "", stripped)
            lines.append(clean)
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--symbol", default="MNQ-09")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Include DBN streaming cross-checks (slow — ~30s/date for trades)",
    )
    parser.add_argument(
        "--fail-only",
        action="store_true",
        help="Only print dates with hard failures (suppress passing dates)",
    )
    args = parser.parse_args()

    settings = get_settings()
    parquet_root = settings.storage.parquet_root

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None

    dates = _discover_dates(args.symbol, parquet_root, start, end)
    if not dates:
        print(f"No dates found for {args.symbol} in {parquet_root}/bars/1m")
        sys.exit(0)

    schemas = _FULL_SCHEMAS if args.full else _FAST_SCHEMAS
    mode = "full (DBN streaming)" if args.full else "fast (Parquet only)"

    print(f"\nHistorical validation — {args.symbol}")
    print(f"Dates: {dates[0]} → {dates[-1]}  ({len(dates)} trading days)")
    print(f"Mode:  {mode}")
    print(f"{'─' * 60}")

    if args.full:
        print("WARNING: full mode streams DBN archives — this will take a long time.")
        print(f"{'─' * 60}")

    passed = 0
    warned = 0
    failed_dates: list[tuple[date, list[str]]] = []

    for d in dates:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            n_failures = run_checks(args.symbol, d, schemas, verbose=False)
        output = buf.getvalue()

        has_warn = "WARN" in output and n_failures == 0
        fail_lines = _extract_failures(output)

        if n_failures > 0:
            failed_dates.append((d, fail_lines))
            if not args.fail_only:
                print(f"  [{_FAIL}] {d.isoformat()}  ({n_failures} failure(s))")
                for line in fail_lines:
                    print(f"          {line}")
        elif has_warn:
            warned += 1
            if not args.fail_only:
                print(f"  [{_WARN}] {d.isoformat()}")
        else:
            passed += 1
            if not args.fail_only:
                print(f"  [{_PASS}] {d.isoformat()}")

    print(f"\n{'═' * 60}")
    print(f"Results: {len(dates)} dates checked")
    print(f"  PASS : {passed}")
    print(f"  WARN : {warned}  (warnings only, no hard failures)")
    print(f"  FAIL : {len(failed_dates)}")

    if failed_dates:
        print(f"\nFailed dates:")
        for d, lines in failed_dates:
            print(f"  {d.isoformat()}")
            for line in lines:
                print(f"    {line}")

    print(f"{'═' * 60}\n")
    sys.exit(1 if failed_dates else 0)


if __name__ == "__main__":
    main()
