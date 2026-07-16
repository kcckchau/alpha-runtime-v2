"""
Inspect LevelInteractionEngine episode output (Phase 1 research).

Usage:
    python scripts/inspect_episodes.py
    python scripts/inspect_episodes.py --symbol MNQ --date 2026-07-10
    python scripts/inspect_episodes.py --symbol MNQ --date 2026-07-10 --level vwap
    python scripts/inspect_episodes.py --symbol MNQ --date 2026-07-10 --episode <episode_id_prefix>
    python scripts/inspect_episodes.py --root data/research

No engine startup required — reads Parquet files directly.

Output modes:
  (default)       Summary table of all episodes for the date
  --episode <id>  Bar-by-bar sequence for one specific episode
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def _load_episodes(root: Path, symbol: str, date: str | None) -> pa.Table | None:
    base = root / "interaction" / "episodes" / symbol
    if not base.exists():
        return None
    files = sorted(base.rglob("*.parquet"))
    if not files:
        return None
    tables = []
    for f in files:
        try:
            t = pq.ParquetFile(str(f)).read()
            if date:
                t = t.filter(pc.equal(t.column("session_date"), date))
            if t.num_rows > 0:
                tables.append(t)
        except Exception as exc:
            print(f"  [warn] {f}: {exc}", file=sys.stderr)
    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="permissive")


def _load_bars(root: Path, symbol: str, date: str | None) -> pa.Table | None:
    base = root / "interaction" / "episode_bars" / symbol
    if not base.exists():
        return None
    files = sorted(base.rglob("*.parquet"))
    if not files:
        return None
    tables = []
    for f in files:
        try:
            t = pq.ParquetFile(str(f)).read()
            if date:
                t = t.filter(pc.equal(t.column("session_date"), date))
            if t.num_rows > 0:
                tables.append(t)
        except Exception as exc:
            print(f"  [warn] {f}: {exc}", file=sys.stderr)
    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="permissive")


def _print_summary(ep: pa.Table, level_filter: str | None) -> None:
    rows = ep.to_pylist()
    if level_filter:
        rows = [r for r in rows if r["level_id"].startswith(level_filter)]

    if not rows:
        print("No episodes matched filters.")
        return

    dates = sorted(set(r["session_date"] for r in rows))
    print(f"\nEpisodes: {len(rows)}  |  Session dates: {', '.join(dates)}")

    from collections import Counter
    by_level = Counter(r["level_id"].split(":")[0] for r in rows)
    by_end = Counter(r["end_reason"] for r in rows)
    valid = sum(1 for r in rows if r["is_valid_for_research"])
    cross_counts = [r["cross_count"] for r in rows]
    bar_counts = [r["bar_count"] for r in rows]
    print(f"By level type:  {dict(by_level)}")
    print(f"End reasons:    {dict(by_end)}")
    print(f"Valid:          {valid}/{len(rows)}")
    print(f"cross_count:    mean={sum(cross_counts)/len(cross_counts):.1f}  max={max(cross_counts)}")
    print(f"bar_count:      mean={sum(bar_counts)/len(bar_counts):.1f}  max={max(bar_counts)}")

    # Policy hash
    hashes = set(r["policy_config_hash"] for r in rows)
    print(f"policy_hash:    {', '.join(sorted(hashes))}")

    # Per-date breakdown
    for date in dates:
        day_rows = [r for r in rows if r["session_date"] == date]
        by_lt = Counter(r["level_id"].split(":")[0] for r in day_rows)
        invalid = sum(1 for r in day_rows if not r["is_valid_for_research"])
        print(f"\n  {date}  ({len(day_rows)} episodes  invalid={invalid})")
        for lt in ["vwap", "orh", "orl"]:
            ep_lt = [r for r in day_rows if r["level_id"].startswith(lt)]
            if not ep_lt:
                continue
            cc = [r["cross_count"] for r in ep_lt]
            bc = [r["bar_count"] for r in ep_lt]
            ends = Counter(r["end_reason"] for r in ep_lt)
            print(
                f"    {lt:4s}  episodes={len(ep_lt):3d}"
                f"  cross_count avg={sum(cc)/len(cc):.1f} max={max(cc)}"
                f"  bars avg={sum(bc)/len(bc):.1f}"
                f"  ends={dict(ends)}"
            )

    # Episode list
    print(f"\n{'idx':>4}  {'level':5}  {'cc':>3}  {'bars':>4}  {'flips':>5}  {'end_reason':12}  {'valid':5}  {'episode_id (prefix)'}")
    print("─" * 80)
    rows_sorted = sorted(rows, key=lambda r: (r["session_date"], r["level_id"].split(":")[0], r["interaction_index"]))
    prev_date = None
    for r in rows_sorted:
        if r["session_date"] != prev_date:
            if prev_date is not None:
                print()
            print(f"  ── {r['session_date']} ──")
            prev_date = r["session_date"]
        lt = r["level_id"].split(":")[0]
        eid = r["episode_id"][:16]
        valid_str = "✓" if r["is_valid_for_research"] else "✗"
        print(
            f"  {r['interaction_index']:>4}  {lt:5}  {r['cross_count']:>3}  {r['bar_count']:>4}"
            f"  {r['close_side_flip_count']:>5}  {r['end_reason']:12}  {valid_str:5}  {eid}…"
        )


def _print_episode_bars(ep: pa.Table, bars: pa.Table, episode_prefix: str) -> None:
    ep_rows = [r for r in ep.to_pylist() if r["episode_id"].startswith(episode_prefix)]
    if not ep_rows:
        print(f"No episode found with id prefix '{episode_prefix}'")
        return
    if len(ep_rows) > 1:
        print(f"Ambiguous prefix — matched {len(ep_rows)} episodes:")
        for r in ep_rows:
            print(f"  {r['episode_id']}  date={r['session_date']}  level={r['level_id']}")
        return

    e = ep_rows[0]
    print(f"\nEpisode: {e['episode_id']}")
    print(f"  level_id={e['level_id']}")
    print(f"  session_date={e['session_date']}  interaction_index={e['interaction_index']}")
    print(f"  cross_count={e['cross_count']}  bar_count={e['bar_count']}  end_reason={e['end_reason']}")
    print(f"  is_valid={e['is_valid_for_research']}  policy_hash={e['policy_config_hash']}")
    print(f"  close_side_flips={e['close_side_flip_count']}")
    print(f"  max_above_ticks={e['max_above_ticks']}  max_below_ticks={e['max_below_ticks']}")
    print(f"  pre_entry_close_side={e['pre_entry_close_side']}  pre_entry_close_distance_ticks={e['pre_entry_close_distance_ticks']}")
    print(f"  prior_episode_id={str(e['prior_episode_id'])[:16] + '…' if e['prior_episode_id'] else None}")

    bar_rows = [r for r in bars.to_pylist() if r["episode_id"] == e["episode_id"]]
    bar_rows.sort(key=lambda r: r["sequence_num"] or 0)

    if not bar_rows:
        print("\n  (no bar records found)")
        return

    print(f"\n  {'seq':>3}  {'timestamp (UTC)':24}  {'o_side':6}  {'c_side':6}  {'c_dist':>7}  {'range_spans':11}  {'level_value':12}")
    print("  " + "─" * 82)
    for b in bar_rows:
        ts = str(b["bar_timestamp"])[:23]
        spans = "YES" if b["range_spans_level"] else "no"
        print(
            f"  {str(b['sequence_num']) if b['sequence_num'] is not None else '-':>3}  {ts:24}  {str(b['open_side']):6}  {str(b['close_side']):6}"
            f"  {b['close_distance_ticks']:>+7}  {spans:11}  {b['level_value_at_timestamp']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect LevelInteractionEngine episode output")
    parser.add_argument("--symbol", default="MNQ", help="Symbol (default: MNQ)")
    parser.add_argument("--date", help="Filter by session date YYYY-MM-DD")
    parser.add_argument("--level", choices=["vwap", "orh", "orl"], help="Filter by level type")
    parser.add_argument("--episode", help="Show bar sequence for one episode (id prefix)")
    parser.add_argument("--root", default="data/research", help="Research root (default: data/research)")
    args = parser.parse_args()

    root = Path(args.root)

    ep = _load_episodes(root, args.symbol, args.date)
    if ep is None or ep.num_rows == 0:
        sym_msg = f"symbol={args.symbol}"
        date_msg = f" date={args.date}" if args.date else ""
        print(f"No episode data found for {sym_msg}{date_msg} under {root}/interaction/episodes/")
        print("Run: python scripts/research_replay.py --symbol MNQ --date <YYYY-MM-DD> --fetch")
        sys.exit(0)

    if args.episode:
        bars = _load_bars(root, args.symbol, args.date)
        if bars is None:
            print("No episode_bars data found.")
            sys.exit(1)
        _print_episode_bars(ep, bars, args.episode)
    else:
        _print_summary(ep, args.level)

    print()


if __name__ == "__main__":
    main()
