"""
vwap_touch_analysis.py — Standalone VWAP hold / VWAP reject pattern backtest

Answers: "how well do VWAP hold and VWAP reject perform as strategies?"
using every 1m bar available in data/parquet/bars/1m (loaded from the same
Parquet store the live system writes to — originally sourced from the raw
Databento .dbn.zst archive under data/dbn_raw/).

This is deliberately independent of SetupEngine/ScoringEngine's SSS/A/B/C
confluence gating (see scripts/backtest.py for that) — it fires on every
raw VWAP touch matching the pattern definitions below, so it trades off
sample size for signal purity. Read both to get the full picture: this
script tells you "does the raw pattern have edge at all", backtest.py
tells you "does it survive the extra confluence filters this system
actually trades on".

Session-anchored VWAP: cumulative typical-price-volume / cumulative volume,
resetting at 18:00 ET (the CME Globex session boundary — same anchor
FeatureEngine uses in src/alpha/engines/feature/engine.py).

Pattern definitions (bar i, vwap = cumulative VWAP through bar i):
  HOLD_LONG   — price trending above VWAP (prev bar close > vwap), bar's low
                dips to/through VWAP intrabar but *closes back above* it
                (support held, no close-through). Go long at bar close.
  HOLD_SHORT  — mirror: price below VWAP, bar's high pokes up to/through VWAP
                but closes back below (resistance held). Go short at close.
  REJECT_SHORT— price *crosses* above VWAP (close > vwap) after being below,
                then within REJECT_WINDOW bars crosses back below and closes
                there (failed reclaim / breakout fade). Go short at the
                cross-back-down bar's close.
  REJECT_LONG — mirror: crosses below VWAP after being above, then within
                REJECT_WINDOW bars reclaims back above and closes there
                (failed breakdown). Go long at the reclaim-back-up bar's close.

Exit: fixed forward-looking holding periods (in minutes), measured in index
points and converted to $ using MNQ's $2/point multiplier. No stop-loss is
simulated here (that's a position-sizing/risk decision, not a question about
whether the pattern itself has directional edge) — see the "max adverse
excursion" columns for how painful the ride typically is.

Usage:
    python scripts/vwap_touch_analysis.py
    python scripts/vwap_touch_analysis.py --symbol MNQ-09 --horizons 5,15,30,60
    python scripts/vwap_touch_analysis.py --rth-only
"""

from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
_ET = ZoneInfo("America/New_York")
_POINT_VALUE = 2.0  # MNQ multiplier: $ per index point
_REJECT_WINDOW = 3  # bars — matches FeatureEngine's "rejected" classification


# ── Data loading ──────────────────────────────────────────────────────────────

def load_bars(symbol_root: str) -> pd.DataFrame:
    pattern = str(_REPO / "data" / "parquet" / "bars" / "1m" / "*" / "**" / "data.parquet")
    files = glob.glob(pattern, recursive=True)
    if not files:
        raise SystemExit(f"No parquet bar files found under {pattern}")

    frames = []
    for f in files:
        if f"/1m/{symbol_root}/" not in f and f"/1m/{symbol_root}-" not in f:
            continue
        df = pd.read_parquet(f, columns=["timestamp", "symbol", "open", "high", "low", "close", "volume", "is_partial"])
        frames.append(df)
    if not frames:
        raise SystemExit(f"No parquet files matched symbol root '{symbol_root}'")

    df = pd.concat(frames, ignore_index=True)
    df = df[~df["is_partial"]].copy()
    for c in ("open", "high", "low", "close"):
        df[c] = df[c].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
    return df


# ── Session-anchored VWAP ─────────────────────────────────────────────────────

def add_session_vwap(df: pd.DataFrame) -> pd.DataFrame:
    et = df["timestamp"].dt.tz_convert(_ET)
    session_date = et.dt.date.where(et.dt.time < pd.Timestamp("18:00").time(), et.dt.date + pd.Timedelta(days=1))
    df["session_date"] = session_date
    df["is_rth"] = (et.dt.time >= pd.Timestamp("09:30").time()) & (et.dt.time < pd.Timestamp("16:00").time())

    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    df["_tpv"] = typical * df["volume"]
    grp = df.groupby("session_date", sort=False)
    df["cum_vol"] = grp["volume"].cumsum()
    df["cum_tpv"] = grp["_tpv"].cumsum()
    df["vwap"] = df["cum_tpv"] / df["cum_vol"].replace(0, np.nan)
    df["bars_since_open"] = grp.cumcount()
    df.drop(columns=["_tpv", "cum_vol", "cum_tpv"], inplace=True)
    return df


# ── Signal detection ──────────────────────────────────────────────────────────

def detect_signals(df: pd.DataFrame, min_bars_warmup: int = 10) -> pd.DataFrame:
    df = df.copy()
    vwap = df["vwap"]
    above = df["close"] > vwap
    prev_above = above.groupby(df["session_date"]).shift(1)

    cross_up = (prev_above == False) & above  # noqa: E712
    cross_down = (prev_above == True) & (~above)  # noqa: E712

    warm = df["bars_since_open"] >= min_bars_warmup

    hold_long = warm & (prev_above == True) & above & (df["low"] <= vwap)  # noqa: E712
    hold_short = warm & (prev_above == False) & (~above) & (df["high"] >= vwap)  # noqa: E712

    # Rolling "was there a cross_{up,down} in the last REJECT_WINDOW bars?" per session
    def _recent_cross(mask: pd.Series) -> pd.Series:
        return (
            mask.groupby(df["session_date"])
            .transform(lambda s: s.shift(1).rolling(_REJECT_WINDOW, min_periods=1).max())
            .fillna(0)
            .astype(bool)
        )

    recent_cross_up = _recent_cross(cross_up)
    recent_cross_down = _recent_cross(cross_down)

    reject_short = warm & cross_down & recent_cross_up
    reject_long = warm & cross_up & recent_cross_down

    df["signal"] = np.select(
        [hold_long, hold_short, reject_long, reject_short],
        ["HOLD_LONG", "HOLD_SHORT", "REJECT_LONG", "REJECT_SHORT"],
        default="",
    )
    df["direction"] = np.select(
        [df["signal"].isin(["HOLD_LONG", "REJECT_LONG"]), df["signal"].isin(["HOLD_SHORT", "REJECT_SHORT"])],
        [1, -1],
        default=0,
    )
    return df


# ── Forward returns ───────────────────────────────────────────────────────────

def compute_forward_returns(df: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    df = df.copy()
    for h in horizons:
        fwd_close = df.groupby("session_date")["close"].shift(-h)
        # Cap MAE/MFE window to same-session bars only (approx: use next h bars' high/low)
        fut_high = df["high"].groupby(df["session_date"]).transform(
            lambda s: s.shift(-1).rolling(h, min_periods=1).max().shift(-(h - 1)) if h > 1 else s.shift(-1)
        )
        fut_low = df["low"].groupby(df["session_date"]).transform(
            lambda s: s.shift(-1).rolling(h, min_periods=1).min().shift(-(h - 1)) if h > 1 else s.shift(-1)
        )
        df[f"fwd_ret_{h}"] = (fwd_close - df["close"]) * df["direction"]
        df[f"mfe_{h}"] = np.where(
            df["direction"] > 0, fut_high - df["close"], df["close"] - fut_low
        )
        df[f"mae_{h}"] = np.where(
            df["direction"] > 0, df["close"] - fut_low, fut_high - df["close"]
        )
    return df


# ── Stats ─────────────────────────────────────────────────────────────────────

def summarize(df: pd.DataFrame, horizons: list[int], label: str) -> None:
    sub = df[df["signal"] != ""]
    print(f"\n{'='*100}\n{label}\n{'='*100}")
    print(f"  Total bars analyzed : {len(df):,}   Sessions: {df['session_date'].nunique()}")
    counts = sub["signal"].value_counts()
    print(f"  Signals fired       : {len(sub):,}")
    for sig, n in counts.items():
        print(f"    {sig:<14} {n:>6}")

    for sig in ["HOLD_LONG", "HOLD_SHORT", "REJECT_LONG", "REJECT_SHORT"]:
        rows = sub[sub["signal"] == sig]
        if rows.empty:
            continue
        print(f"\n  --- {sig} (n={len(rows)}) ---")
        header = f"    {'horizon':>8}  {'n':>6}  {'win%':>6}  {'avg_pts':>9}  {'avg_$':>9}  {'avg_mae_pts':>12}  {'total_pts':>10}  {'PF':>6}"
        print(header)
        for h in horizons:
            col = f"fwd_ret_{h}"
            r = rows[col].dropna()
            if r.empty:
                continue
            wins = r[r > 0]
            losses = r[r < 0]
            win_rate = len(wins) / len(r) * 100 if len(r) else 0.0
            pf = (wins.sum() / abs(losses.sum())) if losses.sum() != 0 else float("inf")
            mae = rows[f"mae_{h}"].dropna().mean()
            print(
                f"    {h:>6}m  {len(r):>6}  {win_rate:>5.1f}%  {r.mean():>9.2f}  "
                f"{r.mean()*_POINT_VALUE:>9.2f}  {mae:>12.2f}  {r.sum():>10.1f}  "
                f"{(pf if pf != float('inf') else float('nan')):>6.2f}"
            )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--symbol", default="MNQ", help="Root symbol to filter parquet files on (default: MNQ)")
    p.add_argument("--horizons", default="5,15,30,60", help="Comma-separated forward-look minutes")
    p.add_argument("--rth-only", action="store_true", help="Restrict signals to 9:30-16:00 ET")
    p.add_argument("--min-warmup-bars", type=int, default=10, help="Bars into session before signals are allowed")
    args = p.parse_args()

    horizons = [int(x) for x in args.horizons.split(",")]

    df = load_bars(args.symbol)
    df = add_session_vwap(df)
    df = detect_signals(df, min_bars_warmup=args.min_warmup_bars)
    df = compute_forward_returns(df, horizons)

    summarize(df, horizons, f"ALL SESSIONS  ({args.symbol}, {df['session_date'].min()} \u2192 {df['session_date'].max()})")

    if args.rth_only:
        summarize(df[df["is_rth"]], horizons, "RTH ONLY (09:30-16:00 ET)")
    else:
        summarize(df[df["is_rth"]], horizons, "RTH ONLY (09:30-16:00 ET)")
        summarize(df[~df["is_rth"]], horizons, "OVERNIGHT ONLY")


if __name__ == "__main__":
    main()
