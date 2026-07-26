"""
replay_common.py — Shared infrastructure for backtest.py and replay_day.py.

Not a runnable script — imported by both to avoid duplicated Parquet
bar-loading logic (previously copy-pasted as backtest.py:_load_parquet_bars
and replay_day.py:_load_from_parquet).
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.clock import WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.context.engine import ContextEngine
from alpha.engines.feature.engine import FeatureEngine
from alpha.engines.flow.aggregator import BarFlowAggregator
from alpha.engines.flow.pipeline import BarPipeline
from alpha.engines.market_state.engine import MarketStateEngine
from alpha.engines.scoring.engine import ScoringEngine
from alpha.engines.setup.engine import SetupEngine
from alpha.engines.storage.engine import StorageEngine
from alpha.engines.storage.parquet import ParquetStore
from alpha.engines.thesis.engine import ThesisEngine
from alpha.models.enums import BarTimeframe
from alpha.models.events import BarEvent
from alpha.models.symbol import Symbol

_REPO = Path(__file__).resolve().parent.parent


@dataclass
class EngineBundle:
    """Everything backtest.py/replay_day.py need to feed bars and read state back."""
    bus: EventBus
    registry: SymbolRegistry
    calendar: SessionCalendar
    feature: FeatureEngine
    context: ContextEngine
    market_state: MarketStateEngine
    setup: SetupEngine
    thesis: ThesisEngine
    scoring: ScoringEngine | None


async def build_replay_pipeline(
    settings: AlphaSettings,
    symbol: str,
    sym_obj: Symbol,
    *,
    include_scoring: bool = True,
) -> EngineBundle:
    """
    Construct and wire FeatureEngine/ContextEngine/MarketStateEngine/SetupEngine/
    ThesisEngine[/ScoringEngine] onto BarFlowAggregator + BarPipeline (same
    sequential-stage pattern as live), started and ready for bar events.

    include_scoring=False (replay_day.py) intentionally omits ScoringEngine —
    that script shows SetupEngine's raw score, not a final letter grade.

    Caller owns feeding bars via bus.publish()/bus.flush() and must call
    stop_replay_pipeline() when done.
    """
    registry = SymbolRegistry()
    registry.register(sym_obj)
    calendar = calendar_for_symbol(sym_obj)
    bus = EventBus(queue_size=5000)
    await bus.start()
    clock = WallClock()

    feature_engine      = FeatureEngine(settings, bus, registry, calendar, clock)
    context_engine      = ContextEngine(settings, bus, registry, calendar, clock)
    market_state_engine = MarketStateEngine(settings, bus, registry)
    setup_engine        = SetupEngine(settings, bus, registry)
    thesis_engine       = ThesisEngine(settings, bus, registry)
    scoring_engine      = ScoringEngine(settings, bus) if include_scoring else None

    market_state_engine.set_feature_engine(feature_engine)
    context_engine.set_feature_engine(feature_engine)
    market_state_engine.set_context_engine(context_engine)
    setup_engine.set_feature_engine(feature_engine)
    setup_engine.set_market_state_engine(market_state_engine)
    setup_engine.set_context_engine(context_engine)
    thesis_engine.set_feature_engine(feature_engine)
    thesis_engine.set_context_engine(context_engine)
    if scoring_engine is not None:
        scoring_engine.set_setup_engine(setup_engine)
        scoring_engine.set_feature_engine(feature_engine)

    # Wire BarFlowAggregator + BarPipeline (same as live)
    agg = BarFlowAggregator(
        symbol=symbol,
        event_bus=bus,
        large_trade_threshold=getattr(settings.runtime, "large_trade_threshold", 10),
    )
    agg.attach()

    pipeline = BarPipeline(bus)
    pipeline.set_feature_engine(feature_engine)
    pipeline.set_market_state_engine(market_state_engine)
    pipeline.set_thesis_engine(thesis_engine)
    pipeline.set_setup_engine(setup_engine)
    if scoring_engine is not None:
        pipeline.set_scoring_engine(scoring_engine)
    pipeline.attach()

    await feature_engine.initialize()
    await context_engine.initialize()
    await market_state_engine.initialize()
    await setup_engine.initialize()
    await thesis_engine.initialize()
    if scoring_engine is not None:
        await scoring_engine.initialize()

    # ContextEngine must start AFTER FeatureEngine so its BAR subscription
    # fires second — guaranteeing FeatureEngine.get_snapshot() is current.
    # (ContextEngine is not migrated onto BarPipeline; BarPipeline has no
    # set_context_engine — it still relies on this start-order.)
    await feature_engine.start()
    await context_engine.start()
    await market_state_engine.start()
    await setup_engine.start()
    await thesis_engine.start()
    if scoring_engine is not None:
        await scoring_engine.start()

    return EngineBundle(
        bus=bus, registry=registry, calendar=calendar,
        feature=feature_engine, context=context_engine,
        market_state=market_state_engine, setup=setup_engine,
        thesis=thesis_engine, scoring=scoring_engine,
    )


async def stop_replay_pipeline(bundle: EngineBundle) -> None:
    if bundle.scoring is not None:
        await bundle.scoring.stop()
    await bundle.thesis.stop()
    await bundle.setup.stop()
    await bundle.market_state.stop()
    await bundle.context.stop()
    await bundle.feature.stop()
    await bundle.bus.stop()

# Paths where a code change would silently change replay/backtest output
# without touching AlphaSettings — most SetupEngine/ScoringEngine/ThesisEngine/
# MarketStateEngine thresholds are hardcoded Python literals with no version
# string of their own (unlike FeatureEngine's norm3/ribbon policies below), so
# the git commit + dirty flag is the only reliable "did the logic change"
# signal for those engines.
_TRADING_LOGIC_PATHS = ("src/alpha/engines/", "src/alpha/features/", "scripts/")


def build_config_fingerprint() -> dict:
    """
    Structured provenance for exactly what code produced a replay/backtest run.

    NOTE on completeness: SetupEngine/ScoringEngine/ThesisEngine/MarketStateEngine
    thresholds are hardcoded Python literals inline in each engine's detector/
    scoring methods — there is no settings object or version string for them
    (unlike FeatureEngine's norm3/ribbon policies below, which are real). That
    means git_commit + dirty_files is not one signal among several for those
    engines — it is the *only* signal. There is deliberately no
    "setup_engine_version"/"scoring_policy_version" field here: fabricating one
    would claim a completeness this data doesn't have.

    On git command failure, returns available=False with null commit/dirty
    fields rather than raising — a broken git call must never crash a backtest.
    """
    from alpha.features.slope import (
        EMA_1H_RIBBON_POLICY_VERSION,
        EMA_1H_SLOPE_POLICY_VERSION,
        SLOPE_POLICY_VERSION,
    )

    commit: str | None = None
    dirty_files: list[str] | None = None
    available = True
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_REPO, capture_output=True, text=True, timeout=5,
        )
        commit = result.stdout.strip() or None
        if commit is None:
            available = False
    except Exception:
        available = False

    if available:
        try:
            status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=_REPO, capture_output=True, text=True, timeout=5,
            ).stdout
            dirty_files = [
                line[3:].strip() for line in status.splitlines()
                if line[3:].strip().startswith(_TRADING_LOGIC_PATHS)
            ]
        except Exception:
            dirty_files = None
            available = False

    return {
        "available": available,
        "git_commit": commit,
        "git_dirty": (len(dirty_files) > 0) if dirty_files is not None else None,
        "dirty_trading_logic_files": dirty_files,
        "policy_versions": {
            "slope": SLOPE_POLICY_VERSION,
            "ema_1h_ribbon": EMA_1H_RIBBON_POLICY_VERSION,
            "ema_1h_slope": EMA_1H_SLOPE_POLICY_VERSION,
        },
        "fingerprint_generated_at": datetime.now(timezone.utc).isoformat(),
    }


def config_fingerprint_lines() -> list[str]:
    """Human-readable rendering of build_config_fingerprint() for console output."""
    fp = build_config_fingerprint()
    if not fp["available"]:
        return ["Config fingerprint: unavailable (git command failed)"]

    dirty_files = fp["dirty_trading_logic_files"] or []
    dirty_label = (
        f"DIRTY — {len(dirty_files)} uncommitted trading-logic file(s)"
        if fp["git_dirty"] else "clean"
    )
    lines = [f"Config fingerprint: commit={fp['git_commit']} ({dirty_label})"]
    for f in dirty_files:
        lines.append(f"  uncommitted: {f}")
    pv = fp["policy_versions"]
    lines.append(
        f"  policy versions: slope={pv['slope']} "
        f"1h_ribbon={pv['ema_1h_ribbon']} 1h_slope={pv['ema_1h_slope']}"
    )
    return lines


# Key names (case-insensitive substring match) redacted from build_resolved_config()'s
# settings dump. Defense in depth on top of pydantic SecretStr's own masking
# (AlphaSettings.model_dump(mode="json") already renders SecretStr as "**********")
# — this also catches any field holding a secret that isn't typed SecretStr.
_SENSITIVE_KEY_MARKERS = ("key", "token", "secret", "password", "database_url", "dsn")


def _redact(obj):
    if isinstance(obj, dict):
        return {
            k: ("***REDACTED***" if any(m in k.lower() for m in _SENSITIVE_KEY_MARKERS) else _redact(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def build_resolved_config(settings: AlphaSettings, cli_args: dict) -> dict:
    """
    The actual resolved parameters a run used — not a config file path, since
    env vars / CLI overrides / code-default changes can all make the file on
    disk diverge from what actually executed. Two layers:

    - "settings": AlphaSettings.model_dump(), redacted — the pydantic-resolved
      config (runtime/storage/historical/etc). Does NOT include SetupEngine/
      ScoringEngine thresholds; see build_config_fingerprint()'s docstring for
      why those aren't representable as config at all in this codebase.
    - "cli_args": the exact resolved arguments this run was invoked with
      (after defaults like --warmup's backfill.py-formula resolution are
      applied) — e.g. symbol, date range, min_grade, warmup_days.
    """
    return {
        "settings": _redact(settings.model_dump(mode="json")),
        "cli_args": cli_args,
    }


def config_hash(resolved_config: dict) -> str:
    """
    sha256 over the resolved config's stable content (canonical/sorted JSON).
    Two runs with the same config_hash used identical settings+CLI args;
    combined with git_commit from build_config_fingerprint(), this lets you
    tell apart a parameter experiment (same commit, different hash) from a
    pure code change (different commit, same hash).
    """
    import hashlib
    import json as _json

    canonical = _json.dumps(resolved_config, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def dataset_manifest(
    bars: list[BarEvent],
    calendar: SessionCalendar,
    start: date,
    end: date,
    symbol: str,
    source: str = "parquet",
) -> dict:
    """
    Coverage audit for bars actually loaded vs. what should exist for
    [start, end] per the exchange calendar.

    load_m1_bars(skip_read_errors=True) silently continues past a missing or
    corrupt day — necessary so one bad day doesn't abort a whole multi-day
    range, but that means a real data gap and an ordinary non-trading day are
    otherwise indistinguishable from the output alone. This is how that
    silence gets surfaced in saved provenance instead of just producing fewer
    bars with no trace: "why did this run produce fewer signals?" should be
    answerable from code changed (fingerprint) + config changed (config) +
    data missing (this), without having to re-derive any of the three.
    """
    expected_days = set(calendar.trading_days(start, end))
    actual_days = {calendar.session_date(b.timestamp) for b in bars}
    missing_days = sorted(d.isoformat() for d in (expected_days - actual_days))
    return {
        "source": source,
        "symbol": symbol,
        "coverage": {"start": start.isoformat(), "end": end.isoformat()},
        "expected_trading_days": len(expected_days),
        "trading_days_with_bars": len(expected_days) - len(missing_days),
        "missing_days": missing_days,
    }


def default_m1_warmup_days(settings: AlphaSettings) -> int:
    """
    backtest.py/replay_day.py's --warmup default: the M1 entry of
    BackfillEngine's default_warmup_days() — the actual single source of
    truth (also used by BackfillEngine._on_start() and backfill.py --dry-run)
    — instead of an unrelated flat constant that would silently drift if
    settings.historical.minute1_warmup_bars changes.
    """
    from alpha.engines.backfill.engine import default_warmup_days
    from alpha.models.enums import BarTimeframe

    return default_warmup_days(settings.historical)[BarTimeframe.M1]


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
