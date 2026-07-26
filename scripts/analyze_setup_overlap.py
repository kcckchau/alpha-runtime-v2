"""
analyze_setup_overlap.py — Opportunity clustering + level-interaction linking
+ MFE/MAE/fixed-horizon returns for persisted setups.

Phase 1 baseline instrumentation for the setup-ontology question from
earlier research: is fake_breakdown's SSS grade earning it, or is it a
repackaging of vwap_undercut_reclaim? Answering that needs three things the
setups table alone doesn't give you:

  1. opportunity_id — SetupEngine's 11 detectors are not mutually exclusive;
     several can fire off the same underlying price move. Without a grouping
     key, "how many trend_pullback signals fired in 2024" and "how many
     independent trading opportunities occurred" are different numbers, and
     the setups table only gives you the first one.
  2. interaction linking — which VWAP/OR level-interaction episodes
     (LevelInteractionEngine, data/research/interaction/) were concurrently
     active for each opportunity, so a setup label can be related back to
     the raw geometric evidence it supposedly represents.
  3. MFE/MAE + fixed-horizon returns — so setup labels can be compared on
     the same footing regardless of --min-grade filtering (unlike
     backtest.py's signals.jsonl, which only tracks setups that were
     promoted to a full entry/exit simulation).

Does NOT compute marginal-vs-incremental expectancy stats — that's the
natural next step once this enriched table exists, deliberately left for a
follow-up rather than building an unverified stats layer in the same pass.

Reads from data/parquet/setups/ (works on both live-captured and --persist
reconstructed history — both are the same table, is_replay tells them apart)
and, if present, data/research/interaction/episodes/.

Usage:
    python scripts/analyze_setup_overlap.py --start 2026-07-05 --end 2026-07-24
    python scripts/analyze_setup_overlap.py --symbol MNQ-09 --start 2024-01-01 --end 2024-12-31 \
        --horizons 5,15,30,60 --out data/research/overlap
"""
from __future__ import annotations

import argparse
import glob
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts"))

import pandas as pd

from alpha.config.settings import AlphaSettings
from alpha.engines.storage.parquet import ParquetStore
from replay_common import load_m1_bars

# Same 4 short-setup-type names as backtest.py's _SELL_TYPES (kept as plain
# strings here since the persisted setups table stores setup_type as a
# string, not the SetupType enum — this is intentionally a second copy of
# that classification, not imported from backtest.py, since backtest.py is a
# runnable script, not a library other scripts should import symbols from).
_SHORT_SETUP_TYPES = frozenset({
    "vwap_rejection", "vwap_failed_reclaim_short", "trend_pullback_short", "orb_breakdown",
})

_RESOLVED_STATES = {"triggered", "invalidated", "failed", "expired"}


def load_setup_lifecycles(symbol: str, start: date, end: date, settings: AlphaSettings) -> pd.DataFrame:
    """One row per setup_id: detected_at (first event), resolved_at (last event),
    setup_type, final_state, and the last non-null entry/stop/target/grade/score
    seen across its lifecycle (these only populate from CONFIRMED onward)."""
    parquet = ParquetStore(settings.storage)
    frames = []
    d = start
    while d <= end:
        table = parquet.read("setups", symbol, d)
        if table is not None and table.num_rows > 0:
            frames.append(table.to_pandas())
        d += timedelta(days=1)
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp")

    # entry_trigger/stop_reference/target_reference/grade/score were only
    # added to the persisted schema recently (StorageEngine._serialize_event
    # used to drop them) — data written before that fix doesn't have these
    # columns in its Parquet files at all, not just null values. Backfill
    # them as all-null so old and new data can be concatenated/aggregated
    # the same way; downstream code already treats these as optional.
    for col in ("entry_trigger", "stop_reference", "target_reference", "grade", "score"):
        if col not in df.columns:
            df[col] = None

    def _last_non_null(s: pd.Series):
        s = s.dropna()
        return s.iloc[-1] if len(s) else None

    lifecycles = df.groupby("setup_id").agg(
        setup_type=("setup_type", "first"),
        detected_at=("timestamp", "min"),
        resolved_at=("timestamp", "max"),
        final_state=("setup_state", "last"),
        entry_trigger=("entry_trigger", _last_non_null),
        stop_reference=("stop_reference", _last_non_null),
        target_reference=("target_reference", _last_non_null),
        grade=("grade", _last_non_null),
        score=("score", _last_non_null),
        is_replay=("is_replay", "first"),
    ).reset_index()
    lifecycles["session_date"] = lifecycles["detected_at"].dt.date.astype(str)
    return lifecycles


def load_interaction_episodes(symbol: str, start: date, end: date, research_root: Path) -> tuple[pd.DataFrame, dict[str, set[str]]]:
    """Returns (episodes_df, {session_date: {run_id_prefixes_found}}) — the
    second value is purely informational, printed by main() as a heads-up if
    a date has episodes from more than one run (e.g. both a manual
    research_replay.py pass and a --record-interactions backtest both
    touched the same day) — not a hard guard like --persist's, since
    duplicate episode counts don't corrupt this analysis the way duplicate
    setup rows would corrupt backtest.py's own table, but worth knowing
    about since it could double-count interaction evidence."""
    root = research_root / "interaction" / "episodes" / symbol
    frames = []
    run_ids_by_date: dict[str, set[str]] = {}
    d = start
    while d <= end:
        session_dir = root / f"session_date={d.isoformat()}"
        files = sorted(glob.glob(str(session_dir / "*.parquet")))
        if files:
            run_ids = set()
            for f in files:
                frames.append(pd.read_parquet(f))
                # filename: part-{run_id}-{seq:06d}.parquet
                stem = Path(f).stem
                run_id = stem[len("part-"):stem.rfind("-")]
                run_ids.add(run_id)
            run_ids_by_date[d.isoformat()] = run_ids
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame(), run_ids_by_date

    df = pd.concat(frames, ignore_index=True)
    df["started_at"] = pd.to_datetime(df["started_at"], utc=True)
    df["ended_at"] = pd.to_datetime(df["ended_at"], utc=True)
    return df, run_ids_by_date


def cluster_opportunities(lifecycles: pd.DataFrame) -> pd.DataFrame:
    """
    Greedy interval-merge on [detected_at, resolved_at] — two setups belong
    to the same opportunity if their active windows overlap at all. No fuzz
    tolerance beyond exact overlap for this first pass: merging "nearby but
    not overlapping" setups is a real design choice (how near is near?) that
    deserves its own decision once this data shows whether exact-overlap
    clustering already produces sensible groups, not a guessed default.
    """
    if lifecycles.empty:
        return lifecycles.assign(opportunity_id=pd.Series(dtype="object"))

    df = lifecycles.sort_values("detected_at").reset_index(drop=True)
    opp_ids: list[str] = []
    counter = 0
    current_end = None
    for _, row in df.iterrows():
        if current_end is None or row["detected_at"] > current_end:
            counter += 1
            current_end = row["resolved_at"]
        else:
            current_end = max(current_end, row["resolved_at"])
        opp_ids.append(f"opp-{counter:06d}")
    df["opportunity_id"] = opp_ids
    return df


def _direction_for(setup_type: str) -> str:
    return "short" if setup_type in _SHORT_SETUP_TYPES else "long"


def build_opportunities(lifecycles: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    """One row per opportunity_id: setup_labels, final_states, entry/direction
    (from the first setup in the cluster with a non-null entry_trigger, else
    the first setup's type with no price reference), and interaction_labels
    (level_type/approach_side/end_side of any episode overlapping the
    opportunity's [detected_at, resolved_at] window)."""
    clustered = cluster_opportunities(lifecycles)
    if clustered.empty:
        return clustered

    rows = []
    for opp_id, grp in clustered.groupby("opportunity_id"):
        grp = grp.sort_values("detected_at")
        win_start, win_end = grp["detected_at"].min(), grp["resolved_at"].max()

        priced = grp[grp["entry_trigger"].notna()]
        if len(priced):
            entry_row = priced.iloc[0]
            entry = Decimal(str(entry_row["entry_trigger"]))
            direction = _direction_for(entry_row["setup_type"])
        else:
            entry = None
            direction = _direction_for(grp.iloc[0]["setup_type"])

        interaction_labels: list[str] = []
        if not episodes.empty:
            overlap = episodes[
                (episodes["started_at"] <= win_end) & (episodes["ended_at"] >= win_start)
            ]
            interaction_labels = [
                f"{r.level_type}:{r.approach_side}->{r.end_side}" for r in overlap.itertuples()
            ]

        rows.append({
            "opportunity_id": opp_id,
            "session_date": grp.iloc[0]["session_date"],
            "detected_at": win_start,
            "resolved_at": win_end,
            "n_setups": len(grp),
            "setup_labels": list(grp["setup_type"]),
            "final_states": list(grp["final_state"]),
            "grades": [g for g in grp["grade"] if g is not None],
            "direction": direction,
            "entry": entry,
            "interaction_labels": interaction_labels,
        })
    return pd.DataFrame(rows)


def attach_mfe_mae_and_returns(
    opportunities: pd.DataFrame,
    symbol: str,
    settings: AlphaSettings,
    horizons_min: list[int],
) -> pd.DataFrame:
    """MFE/MAE and fixed-horizon returns from the opportunity's entry price,
    over the largest requested horizon. Opportunities with no priced setup
    (entry is None — the cluster never reached CONFIRMED) get null MFE/MAE/
    returns rather than being dropped, so they still count toward the
    setup-label frequency/overlap picture even though they can't contribute
    to the expectancy picture.
    """
    if opportunities.empty:
        return opportunities

    max_horizon = max(horizons_min)
    start = opportunities["detected_at"].min().date() - timedelta(days=1)
    end = opportunities["resolved_at"].max().date() + timedelta(days=1)
    bars = load_m1_bars(symbol, start, end, settings)
    if not bars:
        for h in horizons_min:
            opportunities[f"ret_{h}m"] = None
        opportunities["mfe"] = None
        opportunities["mae"] = None
        return opportunities

    bars_df = pd.DataFrame([{
        "timestamp": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close,
    } for b in bars]).sort_values("timestamp").reset_index(drop=True)
    bars_df["timestamp"] = pd.to_datetime(bars_df["timestamp"], utc=True)
    bars_df = bars_df.set_index("timestamp")

    def _compute(row) -> dict:
        if row["entry"] is None:
            out = {f"ret_{h}m": None for h in horizons_min}
            out["mfe"] = None
            out["mae"] = None
            return out
        entry = float(row["entry"])
        window_end = row["detected_at"] + timedelta(minutes=max_horizon)
        window = bars_df.loc[(bars_df.index >= row["detected_at"]) & (bars_df.index <= window_end)]
        out: dict = {}
        if window.empty:
            for h in horizons_min:
                out[f"ret_{h}m"] = None
            out["mfe"] = None
            out["mae"] = None
            return out
        if row["direction"] == "long":
            out["mfe"] = float(window["high"].max()) - entry
            out["mae"] = entry - float(window["low"].min())
        else:
            out["mfe"] = entry - float(window["low"].min())
            out["mae"] = float(window["high"].max()) - entry
        for h in horizons_min:
            h_end = row["detected_at"] + timedelta(minutes=h)
            h_window = window.loc[window.index <= h_end]
            if h_window.empty:
                out[f"ret_{h}m"] = None
                continue
            close_at_h = float(h_window["close"].iloc[-1])
            out[f"ret_{h}m"] = (close_at_h - entry) if row["direction"] == "long" else (entry - close_at_h)
        return out

    computed = opportunities.apply(_compute, axis=1, result_type="expand")
    return pd.concat([opportunities.reset_index(drop=True), computed.reset_index(drop=True)], axis=1)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--symbol", default="MNQ-09")
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    p.add_argument("--horizons", default="5,15,30,60", help="Comma-separated minute horizons (default: 5,15,30,60)")
    p.add_argument("--out", default=None, help="Write enriched opportunities to this Parquet path")
    args = p.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    horizons = [int(h) for h in args.horizons.split(",")]

    settings = AlphaSettings()
    lifecycles = load_setup_lifecycles(args.symbol, start, end, settings)
    if lifecycles.empty:
        print(f"No setups found for {args.symbol} {start}..{end}. Check data/parquet/setups/.")
        return

    episodes, run_ids_by_date = load_interaction_episodes(args.symbol, start, end, _REPO / "data" / "research")
    multi_run_dates = {d: ids for d, ids in run_ids_by_date.items() if len(ids) > 1}
    if multi_run_dates:
        print(f"Note: {len(multi_run_dates)} date(s) have level-interaction episodes from more than one "
              f"run_id — may double-count interaction evidence for those dates:")
        for d, ids in sorted(multi_run_dates.items())[:5]:
            print(f"  {d}: {sorted(ids)}")

    opportunities = build_opportunities(lifecycles, episodes)
    opportunities = attach_mfe_mae_and_returns(opportunities, args.symbol, settings, horizons)

    print(f"\n{args.symbol}  {start} → {end}")
    print(f"  {len(lifecycles)} setups  →  {len(opportunities)} opportunities "
          f"({len(lifecycles) - len(opportunities)} merged as overlapping)")
    priced = opportunities[opportunities["entry"].notna()]
    print(f"  {len(priced)} opportunities reached a priced (CONFIRMED+) state")
    if not episodes.empty:
        linked = opportunities[opportunities["interaction_labels"].apply(len) > 0]
        print(f"  {len(linked)} opportunities have at least one overlapping level-interaction episode")

    multi_label = opportunities[opportunities["n_setups"] > 1]
    if len(multi_label):
        print(f"\n  {len(multi_label)} opportunities have >1 setup label — most common combinations:")
        combos = multi_label["setup_labels"].apply(lambda labels: " + ".join(sorted(set(labels)))).value_counts()
        for combo, count in combos.head(10).items():
            print(f"    {count:>4}  {combo}")

    if args.out:
        out_path = Path(args.out) / args.symbol / f"{start}_{end}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        write_df = opportunities.copy()
        for col in ("setup_labels", "final_states", "grades", "interaction_labels"):
            write_df[col] = write_df[col].apply(list)
        write_df["entry"] = write_df["entry"].apply(lambda v: str(v) if v is not None else None)
        write_df.to_parquet(out_path)
        print(f"\nWrote {len(write_df)} opportunities → {out_path}")


if __name__ == "__main__":
    main()
