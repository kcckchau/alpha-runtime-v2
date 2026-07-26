"""
replay_common.py — Shared infrastructure for backtest.py and replay_day.py.

Not a runnable script — imported by both to avoid duplicated Parquet
bar-loading logic (previously copy-pasted as backtest.py:_load_parquet_bars
and replay_day.py:_load_from_parquet).
"""
from __future__ import annotations

import subprocess
from datetime import date, timedelta
from pathlib import Path

from alpha.config.settings import AlphaSettings
from alpha.engines.storage.engine import StorageEngine
from alpha.engines.storage.parquet import ParquetStore
from alpha.models.enums import BarTimeframe
from alpha.models.events import BarEvent

_REPO = Path(__file__).resolve().parent.parent

# Paths where a code change would silently change replay/backtest output
# without touching AlphaSettings — most SetupEngine/ScoringEngine/ThesisEngine/
# MarketStateEngine thresholds are hardcoded Python literals with no version
# string of their own (unlike FeatureEngine's norm3/ribbon policies below), so
# the git commit + dirty flag is the only reliable "did the logic change"
# signal for those engines.
_TRADING_LOGIC_PATHS = ("src/alpha/engines/", "src/alpha/features/", "scripts/")


def config_fingerprint_lines() -> list[str]:
    """
    Human-readable lines identifying exactly what setup/context code produced
    a replay/backtest run, so a later run with different code (even
    uncommitted) doesn't get silently compared as if it used the same logic.
    """
    from alpha.features.slope import (
        EMA_1H_RIBBON_POLICY_VERSION,
        EMA_1H_SLOPE_POLICY_VERSION,
        SLOPE_POLICY_VERSION,
    )

    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_REPO, capture_output=True, text=True, timeout=5,
        ).stdout.strip() or "unknown"
    except Exception:
        commit = "unknown"

    dirty_files: list[str] = []
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=_REPO, capture_output=True, text=True, timeout=5,
        ).stdout
        for line in status.splitlines():
            path = line[3:].strip()
            if path.startswith(_TRADING_LOGIC_PATHS):
                dirty_files.append(path)
    except Exception:
        pass

    dirty_label = (
        f"DIRTY — {len(dirty_files)} uncommitted trading-logic file(s)"
        if dirty_files else "clean"
    )
    lines = [f"Config fingerprint: commit={commit} ({dirty_label})"]
    for f in dirty_files:
        lines.append(f"  uncommitted: {f}")
    lines.append(
        f"  policy versions: slope={SLOPE_POLICY_VERSION} "
        f"1h_ribbon={EMA_1H_RIBBON_POLICY_VERSION} 1h_slope={EMA_1H_SLOPE_POLICY_VERSION}"
    )
    return lines


def default_m1_warmup_days(settings: AlphaSettings) -> int:
    """
    Same formula backfill.py uses for its own default (warmup-driven) M1 fetch
    window, so backtest.py/replay_day.py's --warmup default tracks
    settings.historical.minute1_warmup_bars instead of an unrelated flat
    constant that silently drifts if that config changes.
    """
    return max(3, settings.historical.minute1_warmup_bars // 390 + 1)


def load_m1_bars(
    symbol: str,
    start: date,
    end: date,
    settings: AlphaSettings,
    *,
    skip_read_errors: bool = True,
) -> list[BarEvent]:
    """Load M1 bars from Parquet for [start, end] inclusive, sorted by timestamp.

    skip_read_errors=True (backtest.py's original behavior) silently skips a day
    on any read failure — appropriate when scanning many days in one run, since a
    single missing/corrupt day shouldn't abort the whole range.

    skip_read_errors=False (replay_day.py's original behavior) lets a read failure
    raise — appropriate for a single-day interactive debug run, where a silent
    empty result would be more confusing than a loud error.
    """
    parquet = ParquetStore(settings.storage)
    bars: list[BarEvent] = []
    d = start
    while d <= end:
        if skip_read_errors:
            try:
                table = parquet.read_range(f"bars/{BarTimeframe.M1}", symbol, d, d)
            except Exception:
                d += timedelta(days=1)
                continue
        else:
            table = parquet.read_range(f"bars/{BarTimeframe.M1}", symbol, d, d)
        for row in table.to_pylist():
            bar = StorageEngine._row_to_bar_event(row, BarTimeframe.M1)
            if bar.symbol != symbol:
                bar = bar.model_copy(update={"symbol": symbol})
            bars.append(bar)
        d += timedelta(days=1)
    bars.sort(key=lambda b: b.timestamp)
    return bars
