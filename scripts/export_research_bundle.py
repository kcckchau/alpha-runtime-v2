"""
Export a self-contained research bundle for one symbol/date for external review.

Produces three Parquet files under data/exports/{symbol}_{date}/:

  episodes.parquet      — one row per completed episode
  episode_bars.parquet  — one row per bar per episode (ordered by episode_id, bar_seq)
  m1_bars.parquet       — one row per M1 bar across the full session:
                            timestamp, OHLC (reconstructed), vwap, orb_high, orb_low,
                            session_phase, orb_state, proximity_band_ticks

OHLC reconstruction:
  M1 OHLC is not separately stored on disk; it is reconstructed from
  level_observations (level_type='vwap') using:
      price = level_value + distance_ticks * tick_size
  MNQ prices are integer multiples of 0.25 (= tick_size), so reconstruction
  is exact.  Other symbols may differ if tick_size != minimum price increment.

Usage:
    python scripts/export_research_bundle.py --symbol MNQ --date 2026-07-15
    python scripts/export_research_bundle.py --symbol MNQ --date 2026-07-15 --out my_dir
"""
from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


# ── Loaders ───────────────────────────────────────────────────────────────────

def _read_parquet_dir(base: Path, symbol: str, date: str, date_key: str = "session_date") -> pa.Table | None:
    """Read all parquet files under base/{symbol}/session_date={date}/."""
    part_dir = base / symbol / f"session_date={date}"
    if not part_dir.exists():
        return None
    files = sorted(part_dir.rglob("*.parquet"))
    if not files:
        return None
    tables = [pq.ParquetFile(str(f)).read() for f in files]
    return pa.concat_tables(tables, promote_options="permissive")


def _load_level_obs(root: Path, symbol: str, date: str) -> pa.Table | None:
    return _read_parquet_dir(root / "level_observations", symbol, date)


def _load_episodes(root: Path, symbol: str, date: str) -> pa.Table | None:
    return _read_parquet_dir(root / "interaction" / "episodes", symbol, date)


def _load_episode_bars(root: Path, symbol: str, date: str) -> pa.Table | None:
    return _read_parquet_dir(root / "interaction" / "episode_bars", symbol, date)


# ── M1 reconstruction from level_observations ─────────────────────────────────

def _build_m1_table(obs: pa.Table) -> pa.Table:
    """
    Reconstruct M1 bar series from level_observations.

    Uses vwap rows as the primary per-bar record (one per bar).
    OHLC is reconstructed from distance columns:
        price = Decimal(level_value) + distance_ticks * Decimal(tick_size)

    orb_high and orb_low are filled from orh/orl rows at each timestamp.
    When orb_state == 'not_set' (OR still accumulating), those columns are null.
    """
    rows = obs.to_pylist()

    # Index by (bar_timestamp, level_type)
    by_ts: dict[str, dict] = {}  # ts_str → {vwap: row, orh: row, orl: row}
    for r in rows:
        ts = str(r["bar_timestamp"])
        lt = r["level_type"]
        if ts not in by_ts:
            by_ts[ts] = {}
        by_ts[ts][lt] = r

    m1_rows = []
    for ts_str in sorted(by_ts.keys()):
        bucket = by_ts[ts_str]
        vwap_row = bucket.get("vwap")
        if vwap_row is None:
            continue  # no VWAP observation for this bar — skip

        tick_size = Decimal(vwap_row["tick_size"])
        lv = Decimal(vwap_row["level_value"])

        def _price(distance_ticks: int) -> str:
            # Reconstruct tick-aligned price: round to nearest tick to eliminate
            # floating-point tail from the VWAP level value.
            raw = lv + Decimal(distance_ticks) * tick_size
            snapped = round(raw / tick_size) * tick_size
            return str(snapped)

        orh_row = bucket.get("orh")
        orl_row = bucket.get("orl")

        # ORB levels are only valid once the opening range window has closed.
        # During 'not_set', the values in level_observations reflect prior-session
        # or accumulating state — not yet frozen. Null them out to avoid confusion.
        orb_frozen = vwap_row["orb_state"] != "not_set"

        m1_rows.append({
            "bar_timestamp":        vwap_row["bar_timestamp"],
            "open":                 _price(vwap_row["open_distance_ticks"]),
            "high":                 _price(vwap_row["high_distance_ticks"]),
            "low":                  _price(vwap_row["low_distance_ticks"]),
            "close":                _price(vwap_row["close_distance_ticks"]),
            "vwap":                 vwap_row["level_value"],
            "orb_high":             orh_row["level_value"] if (orh_row and orb_frozen) else None,
            "orb_low":              orl_row["level_value"] if (orl_row and orb_frozen) else None,
            "session_phase":        vwap_row["session_phase"],
            "orb_state":            vwap_row["orb_state"],
            "proximity_band_ticks": vwap_row["proximity_band_ticks"],
            "session_date":         vwap_row["session_date"],
        })

    schema = pa.schema([
        pa.field("bar_timestamp",        pa.timestamp("us", tz="UTC")),
        pa.field("open",                 pa.string()),
        pa.field("high",                 pa.string()),
        pa.field("low",                  pa.string()),
        pa.field("close",                pa.string()),
        pa.field("vwap",                 pa.string()),
        pa.field("orb_high",             pa.string()),   # nullable — null before OR closes
        pa.field("orb_low",              pa.string()),   # nullable
        pa.field("session_phase",        pa.string()),
        pa.field("orb_state",            pa.string()),
        pa.field("proximity_band_ticks", pa.int32()),
        pa.field("session_date",         pa.string()),
    ])
    return pa.Table.from_pylist(m1_rows, schema=schema)


# ── Writer ────────────────────────────────────────────────────────────────────

def _write(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(path), compression="snappy")
    print(f"  wrote {path}  ({table.num_rows} rows)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Export research bundle for external review")
    parser.add_argument("--symbol", default="MNQ")
    parser.add_argument("--date",   required=True, help="Session date YYYY-MM-DD")
    parser.add_argument("--root",   default="data/research", help="Research root (default: data/research)")
    parser.add_argument("--out",    help="Output directory (default: data/exports/{symbol}_{date})")
    args = parser.parse_args()

    root = Path(args.root)
    out_dir = Path(args.out) if args.out else Path("data/exports") / f"{args.symbol}_{args.date}"

    print(f"\nExporting research bundle: {args.symbol}  {args.date}")
    print(f"  source: {root}")
    print(f"  output: {out_dir}\n")

    # ── Episodes ──────────────────────────────────────────────────────────────
    ep = _load_episodes(root, args.symbol, args.date)
    if ep is None or ep.num_rows == 0:
        print(f"No episode data for {args.symbol} {args.date}. Run research_replay.py first.")
        sys.exit(1)
    _write(ep, out_dir / "episodes.parquet")

    # ── Episode bars ──────────────────────────────────────────────────────────
    bars = _load_episode_bars(root, args.symbol, args.date)
    if bars is not None and bars.num_rows > 0:
        # Sort for readability: episode_id, bar_seq
        import pyarrow.compute as pc
        idx = pc.sort_indices(bars, sort_keys=[("episode_id", "ascending"), ("bar_seq", "ascending")])
        bars = bars.take(idx)
        _write(bars, out_dir / "episode_bars.parquet")
    else:
        print("  [warn] no episode_bars found")

    # ── M1 series ─────────────────────────────────────────────────────────────
    obs = _load_level_obs(root, args.symbol, args.date)
    if obs is not None and obs.num_rows > 0:
        m1 = _build_m1_table(obs)
        _write(m1, out_dir / "m1_bars.parquet")
    else:
        print("  [warn] no level_observations found — m1_bars not written")

    print(f"\nBundle ready: {out_dir}/")
    print("  episodes.parquet    — episode summaries")
    print("  episode_bars.parquet — bar-by-bar sequences")
    print("  m1_bars.parquet     — full M1 OHLCV + VWAP + ORB context")


if __name__ == "__main__":
    main()
