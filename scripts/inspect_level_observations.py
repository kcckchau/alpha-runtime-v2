"""
Inspect LevelBarObservation Parquet files written by the Phase 0 shadow pipeline.

Usage:
    python scripts/inspect_level_observations.py
    python scripts/inspect_level_observations.py --symbol MNQ
    python scripts/inspect_level_observations.py --symbol MNQ --run-id replay-20260715-aabbccdd
    python scripts/inspect_level_observations.py --root data/research

No engine startup required — reads Parquet files directly.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def _find_parquet_files(root: Path, symbol: str | None) -> list[Path]:
    base = root / "level_observations"
    if not base.exists():
        return []
    if symbol:
        base = base / symbol
        if not base.exists():
            return []
    return sorted(base.rglob("*.parquet"))


def _load(files: list[Path], run_id: str | None) -> pa.Table | None:
    tables = []
    for f in files:
        try:
            # Use ParquetFile.read() to avoid Hive partition discovery conflicting
            # with the session_date column stored in the file itself.
            t = pq.ParquetFile(str(f)).read()
            if run_id:
                mask = pc.equal(t.column("run_id"), run_id)
                t = t.filter(mask)
            if t.num_rows > 0:
                tables.append(t)
        except Exception as exc:
            print(f"  [warn] Could not read {f}: {exc}", file=sys.stderr)

    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="permissive")


def _print_summary(table: pa.Table) -> None:
    total = table.num_rows
    print(f"\nTotal rows: {total:,}")

    # Date range
    ts_col = table.column("bar_timestamp")
    print(f"bar_timestamp range: {pc.min(ts_col).as_py()} → {pc.max(ts_col).as_py()}")

    # Session dates
    dates = pc.unique(table.column("session_date")).to_pylist()
    dates.sort()
    print(f"Session dates covered ({len(dates)}): {', '.join(dates[:10])}")
    if len(dates) > 10:
        print(f"  ... and {len(dates) - 10} more")

    # Level types
    level_counts = {}
    for lt in ["vwap", "orh", "orl"]:
        mask = pc.equal(table.column("level_type"), lt)
        level_counts[lt] = table.filter(mask).num_rows
    print(f"\nRows by level_type:")
    for lt, count in level_counts.items():
        pct = count / total * 100 if total else 0
        print(f"  {lt:8s}: {count:>8,}  ({pct:.1f}%)")

    # Schema versions
    sv = pc.value_counts(table.column("schema_version"))
    print(f"\nSchema versions: {sv.to_pylist()}")

    # Run IDs
    run_ids = pc.unique(table.column("run_id")).to_pylist()
    print(f"Run IDs ({len(run_ids)}): {', '.join(run_ids[:5])}")

    # Data quality distribution
    dq = pc.value_counts(table.column("data_quality"))
    print(f"\nData quality distribution: {dq.to_pylist()}")


def _print_samples(table: pa.Table, n: int = 5) -> None:
    for lt in ["vwap", "orh", "orl"]:
        mask = pc.equal(table.column("level_type"), lt)
        sub = table.filter(mask)
        if sub.num_rows == 0:
            continue
        sample = sub.slice(0, min(n, sub.num_rows)).to_pylist()
        print(f"\n── Sample rows: {lt.upper()} (first {len(sample)} of {sub.num_rows:,}) ──")
        for row in sample:
            ts = row["bar_timestamp"]
            print(
                f"  {ts}  level={row['level_value']:>10s}"
                f"  o={row['open_distance_ticks']:+4d}"
                f"  h={row['high_distance_ticks']:+4d}"
                f"  l={row['low_distance_ticks']:+4d}"
                f"  c={row['close_distance_ticks']:+4d}"
                f"  overlap={str(row['bar_overlaps_level']):5s}"
                f"  open_side={row['open_side']:5s}"
                f"  close_side={row['close_side']:5s}"
                f"  dq={row['data_quality']}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect LevelBarObservation Parquet files")
    parser.add_argument("--symbol", help="Filter by symbol (e.g. MNQ)")
    parser.add_argument("--run-id", help="Filter by run_id")
    parser.add_argument("--root", default="data/research", help="Research data root (default: data/research)")
    parser.add_argument("--samples", type=int, default=5, help="Sample rows per level type (default: 5)")
    args = parser.parse_args()

    root = Path(args.root)
    files = _find_parquet_files(root, args.symbol)

    if not files:
        sym_msg = f" for symbol={args.symbol}" if args.symbol else ""
        print(f"No Parquet files found under {root / 'level_observations'}{sym_msg}")
        print("Run the engine in replay or live mode to generate data.")
        sys.exit(0)

    print(f"Found {len(files)} Parquet file(s) under {root / 'level_observations'}")

    table = _load(files, args.run_id)
    if table is None or table.num_rows == 0:
        print("No rows matched the given filters.")
        sys.exit(0)

    _print_summary(table)
    _print_samples(table, args.samples)
    print()


if __name__ == "__main__":
    main()
