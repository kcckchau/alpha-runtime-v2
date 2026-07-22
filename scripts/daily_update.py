"""
daily_update.py — Daily data maintenance: backfill bars + ticks, then validate.

Run once per trading day after CME settlement (~18:00 ET / 22:00 UTC):
    python scripts/daily_update.py

Or for a specific date:
    python scripts/daily_update.py --date 2026-07-22

Steps:
  1. Backfill bars (M1/M5/H1/D1) for the warmup window — idempotent, fills gaps only
  2. Backfill ticks (trades + quotes) for the target date — skipped if already stored
  3. Validate target date: 1m Parquet integrity, DBN cross-check, bars vs trades range
  4. Exit non-zero if any validation hard-failure is detected

Intended for use as a cron job or post-session maintenance script. All steps
are idempotent — safe to run multiple times.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_PYTHON = sys.executable


def _run(label: str, cmd: list[str]) -> int:
    print(f"\n{'─' * 60}")
    print(f"[daily_update] {label}")
    print(f"{'─' * 60}")
    result = subprocess.run(cmd, cwd=_REPO)
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target date YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--symbol",
        default="MNQ-09",
        help="Symbol to update (default: MNQ-09)",
    )
    parser.add_argument(
        "--skip-ticks",
        action="store_true",
        help="Skip tick backfill (trades + quotes) — bars and validation still run",
    )
    parser.add_argument(
        "--force-ticks",
        action="store_true",
        help="Pass --force to tick backfill (re-fetch even if already stored)",
    )
    args = parser.parse_args()

    target: date = (
        date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)
    )

    print(f"\n[daily_update] Starting update for {args.symbol} / {target.isoformat()}")

    failures = 0

    # ── Step 1: Bar backfill (warmup-driven window, idempotent) ───────────────
    rc = _run(
        "Bar backfill (warmup window)",
        [_PYTHON, "scripts/backfill.py", "--symbol", args.symbol],
    )
    if rc != 0:
        print(f"\n[daily_update] FAIL: bar backfill exited {rc}")
        failures += 1

    # ── Step 2: Tick backfill for target date ─────────────────────────────────
    if not args.skip_ticks:
        tick_cmd = [
            _PYTHON, "scripts/backfill.py",
            "--symbol", args.symbol,
            "--start", target.isoformat(),
            "--end", target.isoformat(),
            "--ticks",
        ]
        if args.force_ticks:
            tick_cmd.append("--force")
        rc = _run(f"Tick backfill ({target.isoformat()})", tick_cmd)
        if rc != 0:
            print(f"\n[daily_update] FAIL: tick backfill exited {rc}")
            failures += 1

    # ── Step 3: Validate ──────────────────────────────────────────────────────
    rc = _run(
        f"Validate {target.isoformat()}",
        [
            _PYTHON, "scripts/validate_data.py",
            "--symbol", args.symbol,
            "--date", target.isoformat(),
            "--schemas", "1m,trades",
        ],
    )
    if rc != 0:
        print(f"\n[daily_update] FAIL: validation exited {rc}")
        failures += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    if failures == 0:
        print(f"[daily_update] OK — {args.symbol} {target.isoformat()} updated and validated")
    else:
        print(f"[daily_update] FAILED — {failures} step(s) failed for {args.symbol} {target.isoformat()}")
    print(f"{'═' * 60}\n")

    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
