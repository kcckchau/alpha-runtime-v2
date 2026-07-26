"""
replay_common.py — Shared infrastructure for backtest.py and replay_day.py.

Not a runnable script — imported by both to avoid duplicated Parquet
bar-loading logic (previously copy-pasted as backtest.py:_load_parquet_bars
and replay_day.py:_load_from_parquet).
"""
from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings, RuntimeSettings, StorageSettings
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
from alpha.models.enums import AssetClass, BarTimeframe, RuntimeMode
from alpha.models.events import BarEvent
from alpha.models.symbol import Symbol
from alpha.research.interaction.engine import LevelInteractionEngine

# ANSI codes for guard/warning messages printed from replay_common itself —
# duplicated here rather than imported from either script (that would be a
# backwards dependency: replay_common is imported BY both scripts).
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_DIM    = "\033[2m"
_R      = "\033[0m"

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
    storage: StorageEngine | None


async def build_replay_pipeline(
    settings: AlphaSettings,
    symbol: str,
    sym_obj: Symbol,
    *,
    include_scoring: bool = True,
    persist: bool = False,
) -> EngineBundle:
    """
    Construct and wire FeatureEngine/ContextEngine/MarketStateEngine/SetupEngine/
    ThesisEngine[/ScoringEngine] onto BarFlowAggregator + BarPipeline (same
    sequential-stage pattern as live), started and ready for bar events.

    include_scoring=False (replay_day.py) intentionally omits ScoringEngine —
    that script shows SetupEngine's raw score, not a final letter grade.

    persist=True additionally wires a StorageEngine, subscribed to the same
    bus like live's does — SetupEngine/MarketStateEngine already publish
    SetupEvent/MarketStateEvent (this was never the gap), and those events
    already cascade is_replay from the triggering bar's metadata
    (`metadata=trigger.metadata` in both engines' _emit()) — the only thing
    that was missing is something subscribed to receive and persist them.
    Writes to the *same* data/parquet/setups|market_states/ tables live uses,
    tagged is_replay=True (load_m1_bars() forces this on every bar it loads,
    regardless of what was originally stored), so a downstream reader can
    tell reconstructed history apart from live-captured signals in the same
    table. BarEvent itself is NOT re-persisted — StorageEngine already skips
    is_replay bars (they're written directly by CatchupService/backfill, and
    re-writing them here would just be duplicate I/O of the same rows).
    Off by default: this is an explicit choice, not automatic, since it
    writes into the same canonical tables live uses.

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
    storage_engine      = StorageEngine(settings, bus) if persist else None

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
    if storage_engine is not None:
        await storage_engine.initialize()

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
    if storage_engine is not None:
        # Subscribes to the bus last — doesn't matter for correctness (pub/sub,
        # not a pipeline stage), but keeps it clearly last in the startup story.
        await storage_engine.start()

    return EngineBundle(
        bus=bus, registry=registry, calendar=calendar,
        feature=feature_engine, context=context_engine,
        market_state=market_state_engine, setup=setup_engine,
        thesis=thesis_engine, scoring=scoring_engine, storage=storage_engine,
    )


async def stop_replay_pipeline(bundle: EngineBundle) -> None:
    if bundle.storage is not None:
        # Stop first so its final flush (in _on_stop) captures everything the
        # other engines published this run, before they're torn down.
        await bundle.storage.stop()
    if bundle.scoring is not None:
        await bundle.scoring.stop()
    await bundle.thesis.stop()
    await bundle.setup.stop()
    await bundle.market_state.stop()
    await bundle.context.stop()
    await bundle.feature.stop()
    await bundle.bus.stop()


def build_replay_settings_and_symbol(symbol: str) -> tuple[AlphaSettings, Symbol]:
    """
    AlphaSettings + Symbol construction — identical in backtest.py and
    replay_day.py before this was extracted (same RuntimeMode.REPLAY settings,
    same asset-class-from-ticker-format heuristic, same MNQ tick/point-value
    special case). Not engine wiring — that's build_replay_pipeline().
    """
    settings = AlphaSettings(
        runtime=RuntimeSettings(mode=RuntimeMode.REPLAY, symbols=[symbol], orb_minutes=5),
        storage=StorageSettings(parquet_root=_REPO / "data" / "parquet"),
    )
    asset_class = AssetClass.FUTURE if "-" in symbol else AssetClass.EQUITY
    root = symbol.split("-")[0] if "-" in symbol else symbol
    sym_obj = Symbol(
        ticker=symbol,
        exchange="CME" if asset_class == AssetClass.FUTURE else "NYSE",
        asset_class=asset_class,
        root_symbol=root,
        lot_size=1,
        tick_size=Decimal("0.25") if "MNQ" in symbol else Decimal("0.01"),
        point_value=Decimal("2.0") if "MNQ" in symbol else Decimal("1.0"),
    )
    return settings, sym_obj


@dataclass
class ReplayContext:
    """
    Everything backtest.py/replay_day.py build once per run beyond the raw
    engine bundle: the interaction engine (if requested) and its run_id.
    Per-bar feeding stays in each script — backtest.py drives SignalTracker's
    PnL simulation, replay_day.py drives a live console table; those are
    genuinely different, not duplication, so this does not try to unify them
    behind a generic feed() method.
    """
    settings: AlphaSettings
    sym_obj: Symbol
    bundle: EngineBundle
    interaction_engine: LevelInteractionEngine | None
    interaction_run_id: str | None

    @property
    def symbol(self) -> str:
        return self.sym_obj.ticker

    def attach_interactions(self) -> None:
        """Call once, after the initial warmup bars are fed — matches both
        scripts' existing rule (LevelInteractionEngine has its own session-
        rollover handling, but warmup days should never become episodes)."""
        if self.interaction_engine is not None:
            self.interaction_engine.attach()

    def flush_interactions(self) -> int:
        """
        Flush interactions (if any) — does NOT stop the pipeline. Returns
        episodes_written. Separate from stop() because the two scripts
        genuinely differ here: backtest.py has nothing left to do with the
        engines afterward, so it calls both together; replay_day.py still
        needs feature_engine/thesis_engine/setup_engine alive afterward (for
        its "Final indicators" printout and --save-results), so it flushes
        interactions early but stops the pipeline later, at the true end.

        flush() also force-closes any still-open episode
        (end_reason="replay_completed") — must only be called once, at the
        true end of a run, not per-day under continuous execution.
        """
        if self.interaction_engine is None:
            return 0
        self.interaction_engine.flush()
        return self.interaction_engine.episodes_written

    async def stop(self) -> None:
        """Tear down the engine pipeline. Call flush_interactions() first if
        record_interactions was used — this does not flush for you."""
        await stop_replay_pipeline(self.bundle)

    async def finish(self) -> dict:
        """flush_interactions() + stop(), for callers with nothing left to do
        with the engines afterward (backtest.py). Returns {"episodes_written": int}
        — callers format/print however fits their own output style; this
        deliberately doesn't print anything itself."""
        episodes_written = self.flush_interactions()
        await self.stop()
        return {"episodes_written": episodes_written}


def enforce_persist_guard(
    symbol: str,
    dates: list[date],
    settings: AlphaSettings,
    *,
    persist: bool,
    persist_force: bool,
) -> None:
    """
    sys.exit(1) if --persist would duplicate existing reconstructed data and
    --persist-force wasn't passed; clears the prior reconstruction first
    (never touching live-captured is_replay=False rows) if it was. No-op if
    persist=False. Identical logic previously copy-pasted in both scripts.
    """
    if not persist:
        return
    existing = find_existing_persisted_replay(symbol, dates, settings)
    if not existing:
        return
    if not persist_force:
        print(f"\n{_RED}--persist would duplicate existing reconstructed data:{_R}")
        for data_type, hit_dates in existing.items():
            print(f"  {data_type}: {', '.join(str(d) for d in hit_dates)}")
        print(f"{_YELLOW}setup_id is a fresh UUID per run (not content-derived) and "
              f"market_states has no dedup key — rerunning would append a full duplicate "
              f"set on top of the last one, not replace it.{_R}")
        print(f"{_DIM}Pass --persist-force to clear the prior reconstruction for these "
              f"dates first (live-captured is_replay=False rows are never touched).{_R}")
        sys.exit(1)
    clear_dates = sorted({d for hit_dates in existing.values() for d in hit_dates})
    print(f"\n{_YELLOW}--persist-force: clearing prior reconstruction for "
          f"{len(clear_dates)} date(s) before rerunning{_R}")
    clear_persisted_replay(symbol, clear_dates, settings)


async def build_replay_context(
    symbol: str,
    sym_obj: Symbol,
    settings: AlphaSettings,
    *,
    include_scoring: bool,
    persist: bool,
    record_interactions: bool,
    interaction_run_id: str,
    research_root: Path | None = None,
) -> ReplayContext:
    """
    build_replay_pipeline() + (optional) LevelInteractionEngine construction,
    as one call. Does NOT attach interactions or feed any bars — call
    ctx.attach_interactions() after your own warmup-feeding loop, matching
    both scripts' existing rule.
    """
    bundle = await build_replay_pipeline(settings, symbol, sym_obj, include_scoring=include_scoring, persist=persist)
    interaction_engine: LevelInteractionEngine | None = None
    if record_interactions:
        interaction_engine = LevelInteractionEngine(
            event_bus=bundle.bus,
            registry=bundle.registry,
            research_root=research_root or (_REPO / "data" / "research"),
            run_id=interaction_run_id,
        )
    return ReplayContext(
        settings=settings, sym_obj=sym_obj, bundle=bundle,
        interaction_engine=interaction_engine,
        interaction_run_id=interaction_run_id if record_interactions else None,
    )


_PERSISTED_DATA_TYPES = ("setups", "market_states")


def _read_partition_file(root: Path, data_type: str, symbol: str, d: date, columns: list[str] | None = None):
    """
    Read one partition's data.parquet directly via pq.ParquetFile(...).read(),
    not the higher-level pq.read_table()/ParquetStore.read() — those go
    through pyarrow's dataset API, which does hive-partition schema inference
    from the path itself (this partition layout is literally .../year=YYYY/
    month=MM/day=DD/...) and can throw a spurious ArrowTypeError merging the
    inferred partition-column type against the file's actual schema. Mirrors
    exactly what ParquetStore.write() already does internally to read the
    existing file before a dedup merge — proven safe, since that's the path
    every write in this codebase already goes through.
    """
    import pyarrow.parquet as pq
    from alpha.engines.storage.parquet import _partition_path

    file_path = _partition_path(root, data_type, symbol, d) / "data.parquet"
    if not file_path.exists():
        return None
    try:
        table = pq.ParquetFile(file_path).read()
    except Exception:
        return None
    return table.select(columns) if columns is not None else table


def find_existing_persisted_replay(symbol: str, dates: list[date], settings: AlphaSettings) -> dict[str, list[date]]:
    """
    Which of `dates` already have reconstructed (is_replay=True) rows in
    data/parquet/setups|market_states/ — i.e. a previous --persist run already
    wrote here.

    Necessary because --persist is not idempotent on rerun: setup_id is a
    fresh uuid4() per SetupEngine detection (not derived from
    symbol/timestamp/setup_type), so the dedup-by-setup_id upsert in
    ParquetStore.write() never matches a prior run's rows — and
    market_states has no dedup key at all (see _DEDUP_KEYS in
    storage/engine.py). Every rerun over the same range would silently
    append a full duplicate set on top of the last one.
    """
    import pyarrow.compute as pc

    found: dict[str, list[date]] = {}
    for data_type in _PERSISTED_DATA_TYPES:
        hits = []
        for d in dates:
            table = _read_partition_file(settings.storage.parquet_root, data_type, symbol, d, columns=["is_replay"])
            if table is not None and table.num_rows > 0:
                if pc.any(table.column("is_replay")).as_py():
                    hits.append(d)
        if hits:
            found[data_type] = hits
    return found


def clear_persisted_replay(symbol: str, dates: list[date], settings: AlphaSettings) -> None:
    """
    Strip is_replay=True rows from setups/market_states for `dates`, keeping
    any is_replay=False (live-captured) rows untouched — e.g. 2026-07-24
    already mixes both from live bootstrap's own catchup-replay at startup.
    Called before a --persist-force rerun so it replaces rather than
    duplicates a prior reconstruction.
    """
    import os
    import tempfile

    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    from alpha.engines.storage.parquet import _partition_path

    parquet = ParquetStore(settings.storage)
    for data_type in _PERSISTED_DATA_TYPES:
        for d in dates:
            table = _read_partition_file(settings.storage.parquet_root, data_type, symbol, d)
            if table is None:
                continue
            kept = table.filter(pc.invert(table.column("is_replay")))
            if kept.num_rows == table.num_rows:
                continue  # nothing was_replay=True here — leave file untouched
            if kept.num_rows == 0:
                parquet.delete(data_type, symbol, d)
                continue
            part_dir = _partition_path(parquet._root, data_type, symbol, d)
            file_path = part_dir / "data.parquet"
            tmp_fd, tmp_path = tempfile.mkstemp(dir=part_dir, suffix=".parquet.tmp")
            try:
                os.close(tmp_fd)
                pq.write_table(kept, tmp_path, compression=parquet._compress, row_group_size=parquet._row_group_size)
                os.replace(tmp_path, file_path)
            except Exception:
                os.unlink(tmp_path)
                raise


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
            updates: dict = {}
            if bar.symbol != symbol:
                updates["symbol"] = symbol
            # Force is_replay=True regardless of what was stored: _row_to_bar_event
            # reconstructs whatever flag the bar had at its *original* ingestion
            # (live vs backfill), but every load_m1_bars() caller is running a
            # replay/backtest right now — SetupEvent/MarketStateEvent cascade this
            # flag from the triggering bar's metadata (see setup/engine.py's
            # `metadata=trigger.metadata`), so this is what lets any persisted
            # setup/market-state output be told apart from live-captured ones.
            if not bar.metadata.is_replay:
                updates["metadata"] = bar.metadata.model_copy(update={"is_replay": True})
            if updates:
                bar = bar.model_copy(update=updates)
            bars.append(bar)
        d += timedelta(days=1)
    bars.sort(key=lambda b: b.timestamp)
    return bars
