"""
backtest.py — Multi-day signal backtester

Replays historical Parquet bars through the full pipeline
(FeatureEngine → SetupEngine → ScoringEngine) and simulates entry/exit
for every WOULD_ENTER signal, recording outcomes.

What this answers:
  - How many Grade-A setups fired per day?
  - What was the win rate and average R:R?
  - Which setup types have edge?
  - Does the signal quality justify a prop firm challenge?

Entry simulation:
  When a scored setup appears in PIPELINE_OUTPUT with grade >= min_grade,
  entry_trigger set, stop and target set, and no position already open:
  → Entry at entry_trigger price (stop-order fill assumption).
  → This fires on the bar where the trigger is first crossed.

Exit simulation (in priority order per bar):
  1. Gap through stop   — bar open on wrong side of stop → fill at open
  2. Stop hit          — bar low/high crosses stop → fill at stop
  3. Target hit        — bar high/low crosses target → fill at target
  4. Timeout           — held for max_hold_bars without resolution → exit at close
  5. Session end       — last bar of session → exit at close

Usage:
    python scripts/backtest.py --start 2026-06-01 --end 2026-07-08
    python scripts/backtest.py --start 2026-06-01 --end 2026-07-08 --symbol MNQ-09
    python scripts/backtest.py --start 2026-06-01 --end 2026-07-08 --min-grade B
    python scripts/backtest.py --start 2026-06-01 --end 2026-07-08 --timeout 120 --save

Output (when --save):
    data/backtest_results/{symbol}/{start}_{end}/
        signals.jsonl   — one JSON object per signal
        summary.json    — aggregate statistics
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

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
from alpha.engines.thesis.engine import ThesisEngine
from alpha.research.interaction.engine import LevelInteractionEngine
from replay_common import config_fingerprint_lines, default_m1_warmup_days, load_m1_bars
from alpha.models.enums import (
    AssetClass, BarTimeframe, EventType, OrderSide, RuntimeMode, SetupGrade, SetupType,
)
from alpha.models.events import BarEvent, PipelineOutputEvent
from alpha.models.symbol import Symbol

_UTC = timezone.utc
_ET  = timezone(timedelta(hours=-4))   # EDT display

logging.basicConfig(level=logging.WARNING, format="%(levelname)-8s %(name)s: %(message)s")

# ── ANSI ──────────────────────────────────────────────────────────────────────
_R = "\033[0m"
_BOLD = "\033[1m"
_DIM  = "\033[2m"
_GREEN  = "\033[32m"
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_CYAN   = "\033[36m"

# ── Short setups ──────────────────────────────────────────────────────────────
_SELL_TYPES: frozenset[SetupType] = frozenset({
    SetupType.VWAP_REJECTION,
    SetupType.VWAP_FAILED_RECLAIM_SHORT,
    SetupType.TREND_PULLBACK_SHORT,
    SetupType.ORB_BREAKDOWN,
})

_GRADE_ORDER = [SetupGrade.SSS, SetupGrade.A_PLUS, SetupGrade.A, SetupGrade.B, SetupGrade.C]


def _grade_index(g: SetupGrade | None) -> int:
    try:
        return _GRADE_ORDER.index(g)
    except (ValueError, TypeError):
        return len(_GRADE_ORDER)


def _direction(setup_type: SetupType) -> OrderSide:
    return OrderSide.SELL if setup_type in _SELL_TYPES else OrderSide.BUY


def _ts(dt: datetime) -> str:
    return dt.astimezone(_ET).strftime("%H:%M")


# ── Signal record ─────────────────────────────────────────────────────────────

@dataclass
class OpenPosition:
    signal_id: str
    symbol: str
    setup_id: str
    setup_type: str
    grade: str
    direction: OrderSide
    entry_bar_ts: datetime
    entry_price: Decimal
    stop: Decimal
    target: Decimal
    risk_pts: Decimal
    reward_pts: Decimal
    rr_setup: float
    bars_held: int = 0


@dataclass
class SignalRecord:
    signal_id: str
    date: str
    symbol: str
    entry_bar_ts: str
    setup_type: str
    grade: str
    direction: str
    entry_price: float
    stop: float
    target: float
    risk_pts: float
    reward_pts: float
    rr_setup: float
    exit_bar_ts: str
    exit_price: float
    exit_reason: str        # target_hit | stop_hit | stop_hit_gap | timeout | session_end
    pnl_pts: float
    hold_bars: int
    outcome: str            # win | loss | breakeven | incomplete


# ── Signal tracker ────────────────────────────────────────────────────────────

class SignalTracker:
    """
    Tracks open simulated positions and records outcomes.

    Two-phase per-bar loop:
      1. pre_bar(bar)  — runs BEFORE publish:
           a. If in position: check exits.
           b. If flat + pending setups from the previous bar's pipeline run: check entry.
      2. on_pipeline_output(event) — runs AFTER publish (via EventBus):
           Store the best qualifying setups for entry checking on the NEXT bar.

    This matches live PositionMonitor: setups are detected at bar close,
    entry fills happen on subsequent bars.
    """

    def __init__(
        self,
        symbol: str,
        session_date: date,
        min_grade: SetupGrade,
        max_hold_bars: int,
        point_value: Decimal,
    ) -> None:
        self._symbol = symbol
        self._session_date = str(session_date)
        self._min_grade = min_grade
        self._max_hold_bars = max_hold_bars
        self._point_value = point_value
        self._open: OpenPosition | None = None
        # Setups stored from the most recent PIPELINE_OUTPUT.
        # Checked for entry on the following bar in pre_bar().
        self._pending_setups: list[Any] = []
        self.signals: list[SignalRecord] = []

    def pre_bar(self, bar: BarEvent) -> None:
        """
        Called BEFORE publishing each bar to the EventBus.

        1. Check exits if a position is open.
        2. Check entries from pending setups if flat.
        """
        if self._open is not None:
            pos = self._open
            pos.bars_held += 1
            exit_price, exit_reason = self._simulate_exit(pos, bar)
            if exit_reason:
                self._close(pos, bar.timestamp, exit_price, exit_reason)
        elif self._pending_setups:
            # Try to enter on this bar using setups detected on the previous bar.
            for setup in self._pending_setups:
                direction = _direction(setup.setup_type)
                # Entry check: did price touch the trigger this bar?
                # bar high/low = stop-limit order fill assumption.
                triggered = (
                    (direction == OrderSide.BUY  and bar.high >= setup.entry_trigger) or
                    (direction == OrderSide.SELL and bar.low  <= setup.entry_trigger)
                )
                if not triggered:
                    continue

                entry_price = setup.entry_trigger
                stop        = setup.stop_reference
                target      = setup.target_reference
                risk_pts    = abs(entry_price - stop)
                reward_pts  = abs(target - entry_price)
                rr_setup    = float(reward_pts / risk_pts) if risk_pts > 0 else 0.0

                self._open = OpenPosition(
                    signal_id=str(uuid4()),
                    symbol=self._symbol,
                    setup_id=str(setup.setup_id),
                    setup_type=str(setup.setup_type),
                    grade=str(setup.grade),
                    direction=direction,
                    entry_bar_ts=bar.timestamp,
                    entry_price=entry_price,
                    stop=stop,
                    target=target,
                    risk_pts=risk_pts,
                    reward_pts=reward_pts,
                    rr_setup=rr_setup,
                )
                break  # one signal per bar

    async def on_pipeline_output(self, event: Any) -> None:
        """Store qualifying setups from PIPELINE_OUTPUT for entry on the next bar."""
        if not isinstance(event, PipelineOutputEvent):
            return

        candidates = sorted(
            event.scored_setups,
            key=lambda s: (_grade_index(s.grade), str(s.setup_id)),
        )
        qualifying = []
        for setup in candidates:
            if setup.grade is None:
                continue
            if _grade_index(setup.grade) > _grade_index(self._min_grade):
                continue
            if setup.entry_trigger is None or setup.stop_reference is None or setup.target_reference is None:
                continue
            qualifying.append(setup)

        # Replace pending setups (stale setups from a prior bar don't carry over
        # unless the pipeline keeps emitting them — which it does for confirmed setups).
        self._pending_setups = qualifying

    def close_remaining(self, last_bar: BarEvent) -> None:
        """Close any open position at session end."""
        if self._open is not None:
            self._close(self._open, last_bar.timestamp, last_bar.close, "session_end")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _simulate_exit(
        self, pos: OpenPosition, bar: BarEvent
    ) -> tuple[Decimal | None, str | None]:
        if pos.direction == OrderSide.BUY:
            # Gap down through stop
            if bar.open <= pos.stop:
                return bar.open, "stop_hit_gap"
            # Stop hit intrabar
            if bar.low <= pos.stop:
                return pos.stop, "stop_hit"
            # Target hit
            if bar.high >= pos.target:
                return pos.target, "target_hit"
        else:  # SHORT
            # Gap up through stop
            if bar.open >= pos.stop:
                return bar.open, "stop_hit_gap"
            # Stop hit intrabar
            if bar.high >= pos.stop:
                return pos.stop, "stop_hit"
            # Target hit
            if bar.low <= pos.target:
                return pos.target, "target_hit"

        # Timeout
        if pos.bars_held >= self._max_hold_bars:
            return bar.close, "timeout"

        return None, None

    def _close(
        self,
        pos: OpenPosition,
        bar_ts: datetime,
        exit_price: Decimal,
        exit_reason: str,
    ) -> None:
        if pos.direction == OrderSide.BUY:
            pnl_pts = float(exit_price - pos.entry_price)
        else:
            pnl_pts = float(pos.entry_price - exit_price)

        if pnl_pts > 0.01:
            outcome = "win"
        elif pnl_pts < -0.01:
            outcome = "loss"
        else:
            outcome = "breakeven"

        if exit_reason == "session_end":
            outcome = "incomplete"

        self.signals.append(SignalRecord(
            signal_id=pos.signal_id,
            date=self._session_date,
            symbol=pos.symbol,
            entry_bar_ts=pos.entry_bar_ts.isoformat(),
            setup_type=pos.setup_type,
            grade=pos.grade,
            direction="long" if pos.direction == OrderSide.BUY else "short",
            entry_price=float(pos.entry_price),
            stop=float(pos.stop),
            target=float(pos.target),
            risk_pts=float(pos.risk_pts),
            reward_pts=float(pos.reward_pts),
            rr_setup=pos.rr_setup,
            exit_bar_ts=bar_ts.isoformat(),
            exit_price=float(exit_price),
            exit_reason=exit_reason,
            pnl_pts=pnl_pts,
            hold_bars=pos.bars_held,
            outcome=outcome,
        ))
        self._open = None


# ── Bar loading ───────────────────────────────────────────────────────────────

# ── Per-day replay ────────────────────────────────────────────────────────────

async def _replay_day(
    symbol: str,
    sym_obj: Symbol,
    session_date: date,
    warmup_days: int,
    settings: AlphaSettings,
    tracker: SignalTracker,
    verbose: bool,
    record_interactions: bool = False,
    research_root: Path | None = None,
    interaction_run_id: str | None = None,
) -> tuple[int, int]:
    """
    Replay one session through the full pipeline.
    Returns (session bars processed, level-interaction episodes written).
    """
    # Load bars: warmup window + session
    warmup_start = session_date - timedelta(days=warmup_days + 2)
    all_bars = load_m1_bars(symbol, warmup_start, session_date, settings)
    if not all_bars:
        return 0, 0

    # Split warmup vs session
    # CME futures session: 18:00 ET prior day → 18:00 ET session_date. Computed via
    # CMEEqIndexCalendar.pre_market_open() (ZoneInfo-based) rather than a hardcoded
    # UTC offset — a fixed 22:00 UTC is EDT-only and drifts an hour off during EST.
    calendar = calendar_for_symbol(sym_obj)
    session_utc_start = calendar.pre_market_open(session_date)
    session_utc_end = calendar.pre_market_open(session_date + timedelta(days=1))

    warmup_bars  = [b for b in all_bars if b.timestamp < session_utc_start]
    session_bars = [b for b in all_bars if session_utc_start <= b.timestamp < session_utc_end]

    if not session_bars:
        return 0, 0

    # ── Build engines ──────────────────────────────────────────────────────────
    registry = SymbolRegistry()
    registry.register(sym_obj)
    bus = EventBus(queue_size=5000)
    await bus.start()
    clock = WallClock()

    feature_engine      = FeatureEngine(settings, bus, registry, calendar, clock)
    context_engine      = ContextEngine(settings, bus, registry, calendar, clock)
    market_state_engine = MarketStateEngine(settings, bus, registry)
    setup_engine        = SetupEngine(settings, bus, registry)
    thesis_engine       = ThesisEngine(settings, bus, registry)
    scoring_engine      = ScoringEngine(settings, bus)

    market_state_engine.set_feature_engine(feature_engine)
    context_engine.set_feature_engine(feature_engine)
    market_state_engine.set_context_engine(context_engine)
    setup_engine.set_feature_engine(feature_engine)
    setup_engine.set_market_state_engine(market_state_engine)
    setup_engine.set_context_engine(context_engine)
    thesis_engine.set_feature_engine(feature_engine)
    thesis_engine.set_context_engine(context_engine)
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
    pipeline.set_scoring_engine(scoring_engine)
    pipeline.attach()

    # Subscribe tracker to PIPELINE_OUTPUT
    bus.subscribe(EventType.PIPELINE_OUTPUT, tracker.on_pipeline_output)

    # Start engines
    await feature_engine.initialize()
    await context_engine.initialize()
    await market_state_engine.initialize()
    await setup_engine.initialize()
    await thesis_engine.initialize()
    await scoring_engine.initialize()

    await feature_engine.start()
    await context_engine.start()
    await market_state_engine.start()
    await setup_engine.start()
    await thesis_engine.start()
    await scoring_engine.start()

    # ── Feed warmup bars ───────────────────────────────────────────────────────
    for bar in warmup_bars:
        await bus.publish(bar)
        await bus.flush()

    if verbose:
        print(f"  {_DIM}Warmup: {len(warmup_bars)} bars fed{_R}")

    # Level interactions: shadow research subscriber, never influences trades.
    # Attached only now (target date only) — matching research_replay.py's pattern —
    # so warmup days aren't re-recorded as episodes on every subsequent day's run.
    interaction_engine: LevelInteractionEngine | None = None
    if record_interactions:
        interaction_engine = LevelInteractionEngine(
            event_bus=bus,
            registry=registry,
            research_root=research_root or (_REPO / "data" / "research"),
            run_id=interaction_run_id,
        )
        interaction_engine.attach()

    # ── Feed session bars ──────────────────────────────────────────────────────
    for bar in session_bars:
        # Exit check runs before pipeline (pre_bar is synchronous)
        tracker.pre_bar(bar)
        await bus.publish(bar)
        await bus.flush()  # pipeline runs → PIPELINE_OUTPUT → tracker.on_pipeline_output

        if verbose and tracker._open:
            pos = tracker._open
            print(f"  {_ts(bar.timestamp)}  {_CYAN}IN POSITION{_R}  "
                  f"{pos.setup_type} {pos.grade}  "
                  f"entry={float(pos.entry_price):.2f}  "
                  f"stop={float(pos.stop):.2f}  "
                  f"target={float(pos.target):.2f}  "
                  f"bars_held={pos.bars_held}")

    # Close any position still open at session end
    if session_bars:
        tracker.close_remaining(session_bars[-1])

    episodes_written = 0
    if interaction_engine is not None:
        interaction_engine.flush()
        episodes_written = interaction_engine.episodes_written

    # ── Stop engines ───────────────────────────────────────────────────────────
    await scoring_engine.stop()
    await thesis_engine.stop()
    await setup_engine.stop()
    await market_state_engine.stop()
    await context_engine.stop()
    await feature_engine.stop()
    await bus.stop()

    return len(session_bars), episodes_written


# ── Aggregate statistics ──────────────────────────────────────────────────────

def _compute_stats(signals: list[SignalRecord]) -> dict[str, Any]:
    completed = [s for s in signals if s.outcome != "incomplete"]
    wins   = [s for s in completed if s.outcome == "win"]
    losses = [s for s in completed if s.outcome == "loss"]

    total = len(signals)
    n_completed = len(completed)
    n_wins = len(wins)
    n_losses = len(losses)
    n_incomplete = len([s for s in signals if s.outcome == "incomplete"])
    n_timeout = len([s for s in completed if s.exit_reason == "timeout"])

    win_rate = n_wins / n_completed if n_completed > 0 else 0.0
    avg_rr_setup = (sum(s.rr_setup for s in signals) / total) if total > 0 else 0.0

    total_win_pts  = sum(s.pnl_pts for s in wins)
    total_loss_pts = abs(sum(s.pnl_pts for s in losses))
    profit_factor  = (total_win_pts / total_loss_pts) if total_loss_pts > 0 else float("inf")
    total_pnl_pts  = sum(s.pnl_pts for s in completed)

    avg_win_pts  = (total_win_pts / n_wins) if n_wins > 0 else 0.0
    avg_loss_pts = (total_loss_pts / n_losses) if n_losses > 0 else 0.0
    avg_rr_actual = (avg_win_pts / avg_loss_pts) if avg_loss_pts > 0 else 0.0

    # By setup type
    by_type: dict[str, Any] = {}
    for s in completed:
        t = s.setup_type
        if t not in by_type:
            by_type[t] = {"total": 0, "wins": 0, "losses": 0, "pnl_pts": 0.0}
        by_type[t]["total"] += 1
        by_type[t]["pnl_pts"] += s.pnl_pts
        if s.outcome == "win":
            by_type[t]["wins"] += 1
        elif s.outcome == "loss":
            by_type[t]["losses"] += 1
    for t, d in by_type.items():
        d["win_rate"] = round(d["wins"] / d["total"], 3) if d["total"] > 0 else 0.0

    # By grade
    by_grade: dict[str, Any] = {}
    for s in completed:
        g = s.grade
        if g not in by_grade:
            by_grade[g] = {"total": 0, "wins": 0, "pnl_pts": 0.0}
        by_grade[g]["total"] += 1
        by_grade[g]["pnl_pts"] += s.pnl_pts
        if s.outcome == "win":
            by_grade[g]["wins"] += 1
    for g, d in by_grade.items():
        d["win_rate"] = round(d["wins"] / d["total"], 3) if d["total"] > 0 else 0.0

    # Exit reason breakdown
    exit_reasons: dict[str, int] = {}
    for s in signals:
        exit_reasons[s.exit_reason] = exit_reasons.get(s.exit_reason, 0) + 1

    return {
        "total_signals": total,
        "completed": n_completed,
        "wins": n_wins,
        "losses": n_losses,
        "timeouts": n_timeout,
        "incomplete": n_incomplete,
        "win_rate": round(win_rate, 4),
        "avg_rr_setup": round(avg_rr_setup, 3),
        "avg_rr_actual": round(avg_rr_actual, 3),
        "avg_win_pts": round(avg_win_pts, 2),
        "avg_loss_pts": round(avg_loss_pts, 2),
        "profit_factor": round(profit_factor, 3) if profit_factor != float("inf") else None,
        "total_pnl_pts": round(total_pnl_pts, 2),
        "by_setup_type": by_type,
        "by_grade": by_grade,
        "exit_reasons": exit_reasons,
    }


# ── Terminal output ───────────────────────────────────────────────────────────

def _print_signal(s: SignalRecord) -> None:
    col = _GREEN if s.outcome == "win" else _RED if s.outcome == "loss" else _DIM
    entry_time = datetime.fromisoformat(s.entry_bar_ts).astimezone(_ET).strftime("%H:%M")
    exit_time  = datetime.fromisoformat(s.exit_bar_ts).astimezone(_ET).strftime("%H:%M")
    pnl_str = f"{s.pnl_pts:+.2f}pts"
    print(
        f"  {s.date}  {entry_time}→{exit_time}  "
        f"{s.grade:<4} {s.setup_type:<30}  "
        f"{s.direction:<5}  "
        f"R:R={s.rr_setup:.1f}  "
        f"{col}{pnl_str:>10}{_R}  "
        f"{_DIM}{s.exit_reason}{_R}"
    )


def _print_summary(stats: dict, symbol: str, start: str, end: str) -> None:
    print(f"\n{'='*80}")
    print(f"{_BOLD}Backtest summary  {symbol}  {start} → {end}{_R}")
    print(f"{'='*80}")
    print(f"  Signals    : {stats['total_signals']}  "
          f"(completed={stats['completed']}, "
          f"incomplete={stats['incomplete']})")
    print(f"  Wins/Losses: {stats['wins']}/{stats['losses']}  "
          f"win rate = {_BOLD}{stats['win_rate']*100:.1f}%{_R}")
    print(f"  Avg R:R    : setup={stats['avg_rr_setup']:.2f}  "
          f"actual={stats['avg_rr_actual']:.2f}")
    print(f"  Avg win    : {stats['avg_win_pts']:.2f} pts  "
          f"Avg loss: {stats['avg_loss_pts']:.2f} pts")
    pf = stats['profit_factor']
    pf_str = f"{pf:.2f}" if pf is not None else "∞"
    print(f"  Profit fac.: {pf_str}")
    print(f"  Total P&L  : {_BOLD}{stats['total_pnl_pts']:+.2f} pts{_R}")
    print(f"  Timeouts   : {stats['timeouts']}")

    if stats["by_setup_type"]:
        print(f"\n  {_DIM}By setup type:{_R}")
        for stype, d in sorted(stats["by_setup_type"].items(), key=lambda x: -x[1]["pnl_pts"]):
            wr = d["win_rate"] * 100
            col = _GREEN if d["pnl_pts"] > 0 else _RED
            print(f"    {stype:<35}  {d['total']:>3} signals  "
                  f"win={wr:.0f}%  "
                  f"{col}{d['pnl_pts']:+.1f}pts{_R}")

    if stats["by_grade"]:
        print(f"\n  {_DIM}By grade:{_R}")
        for grade, d in sorted(stats["by_grade"].items(), key=lambda x: _grade_index(x[0])):
            wr = d["win_rate"] * 100
            col = _GREEN if d["pnl_pts"] > 0 else _RED
            print(f"    {grade:<6}  {d['total']:>3} signals  "
                  f"win={wr:.0f}%  "
                  f"{col}{d['pnl_pts']:+.1f}pts{_R}")

    if stats["exit_reasons"]:
        print(f"\n  {_DIM}Exit reasons:{_R}")
        for reason, count in sorted(stats["exit_reasons"].items(), key=lambda x: -x[1]):
            print(f"    {reason:<20}  {count}")
    print()


# ── Save results ──────────────────────────────────────────────────────────────

def _save(
    signals: list[SignalRecord],
    stats: dict,
    symbol: str,
    start: str,
    end: str,
    meta: dict,
) -> Path:
    out_dir = _REPO / "data" / "backtest_results" / symbol / f"{start}_{end}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # signals.jsonl
    jsonl_path = out_dir / "signals.jsonl"
    with open(jsonl_path, "w") as f:
        for s in signals:
            f.write(json.dumps(s.__dict__) + "\n")

    # summary.json
    summary = {"meta": meta, "stats": stats}
    summary_path = out_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"{_GREEN}Results saved:{_R}")
    print(f"  signals  → {jsonl_path}")
    print(f"  summary  → {summary_path}")
    return out_dir


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(
    symbol: str,
    start_date: date,
    end_date: date,
    min_grade: SetupGrade,
    warmup_days: int,
    max_hold_bars: int,
    save: bool,
    verbose: bool,
    record_interactions: bool = False,
) -> None:
    settings = AlphaSettings(
        runtime=RuntimeSettings(
            mode=RuntimeMode.REPLAY,
            symbols=[symbol],
            orb_minutes=5,
        ),
        storage=StorageSettings(
            parquet_root=_REPO / "data" / "parquet",
        ),
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
    point_value = sym_obj.point_value or Decimal("1.0")

    # Enumerate candidate session dates
    candidate_dates: list[date] = []
    d = start_date
    while d <= end_date:
        candidate_dates.append(d)
        d += timedelta(days=1)

    all_signals: list[SignalRecord] = []

    print(f"\n{_BOLD}Backtest{_R}  {symbol}  "
          f"{start_date} → {end_date}  "
          f"min_grade={min_grade}  "
          f"warmup={warmup_days}d  "
          f"timeout={max_hold_bars}bars")
    for line in config_fingerprint_lines():
        print(f"{_DIM}{line}{_R}")
    print()

    print(f"{'Date':<12} {'Bars':>5}  {'Signals':>8}  {'Win':>4} {'Loss':>4}  {'P&L':>10}"
          + ("  Episodes" if record_interactions else ""))
    print("-" * (56 + (10 if record_interactions else 0)))

    interaction_run_id = f"btest-{start_date}_{end_date}"
    total_episodes = 0
    session_count = 0
    for session_date in candidate_dates:
        tracker = SignalTracker(
            symbol=symbol,
            session_date=session_date,
            min_grade=min_grade,
            max_hold_bars=max_hold_bars,
            point_value=point_value,
        )

        bars_processed, episodes_written = await _replay_day(
            symbol=symbol,
            sym_obj=sym_obj,
            session_date=session_date,
            warmup_days=warmup_days,
            settings=settings,
            tracker=tracker,
            verbose=verbose,
            record_interactions=record_interactions,
            interaction_run_id=interaction_run_id,
        )
        total_episodes += episodes_written

        if bars_processed == 0:
            continue  # not a trading day / no data

        session_count += 1
        day_signals = tracker.signals
        all_signals.extend(day_signals)

        day_wins   = sum(1 for s in day_signals if s.outcome == "win")
        day_losses = sum(1 for s in day_signals if s.outcome == "loss")
        day_pnl    = sum(s.pnl_pts for s in day_signals if s.outcome != "incomplete")
        pnl_col    = _GREEN if day_pnl >= 0 else _RED

        print(
            f"{str(session_date):<12} {bars_processed:>5}  "
            f"{len(day_signals):>8}  "
            f"{day_wins:>4} {day_losses:>4}  "
            f"{pnl_col}{day_pnl:>+9.1f}pts{_R}"
            + (f"  {episodes_written:>8}" if record_interactions else "")
        )

        if verbose and day_signals:
            for s in day_signals:
                _print_signal(s)

    print("-" * 56)
    print(f"  {session_count} trading sessions")
    if record_interactions:
        print(f"  {total_episodes} level-interaction episodes written "
              f"(data/research/interaction/, run_id={interaction_run_id})")

    if not all_signals:
        print(f"\n{_YELLOW}No signals found. Check date range, min_grade, and parquet data.{_R}")
        return

    stats = _compute_stats(all_signals)
    _print_summary(stats, symbol, str(start_date), str(end_date))

    if save:
        meta = {
            "symbol": symbol,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "min_grade": str(min_grade),
            "warmup_days": warmup_days,
            "max_hold_bars": max_hold_bars,
            "sessions": session_count,
            "generated_at": datetime.now(_UTC).isoformat(),
        }
        _save(all_signals, stats, symbol, str(start_date), str(end_date), meta)


def main() -> None:
    p = argparse.ArgumentParser(description="Multi-day signal backtester")
    p.add_argument("--symbol",    default="MNQ-09",  help="Ticker (default: MNQ-09)")
    p.add_argument("--start",     required=True,      help="Start date YYYY-MM-DD")
    p.add_argument("--end",       required=True,      help="End date YYYY-MM-DD")
    p.add_argument("--min-grade", default="A",
                   choices=["SSS", "A+", "A", "B", "C"],
                   help="Minimum setup grade to track (default: A)")
    p.add_argument("--warmup",    type=int, default=None,
                   help="Warmup days per session (default: derived from "
                        "settings.historical.minute1_warmup_bars, same formula backfill.py uses)")
    p.add_argument("--timeout",   type=int, default=60,
                   help="Max bars to hold before timeout exit (default: 60 = 1 hour)")
    p.add_argument("--save",      action="store_true",
                   help="Save signals.jsonl + summary.json to data/backtest_results/")
    p.add_argument("--verbose",   action="store_true",
                   help="Print each signal and intraday position details")
    p.add_argument("--record-interactions", action="store_true",
                   help="Also run LevelInteractionEngine (shadow, VWAP/OR episode geometry) "
                        "and write to data/research/interaction/")
    args = p.parse_args()

    grade_map = {
        "SSS": SetupGrade.SSS,
        "A+":  SetupGrade.A_PLUS,
        "A":   SetupGrade.A,
        "B":   SetupGrade.B,
        "C":   SetupGrade.C,
    }
    min_grade = grade_map[args.min_grade]

    warmup_days = args.warmup
    if warmup_days is None:
        warmup_days = default_m1_warmup_days(AlphaSettings())

    asyncio.run(run(
        symbol=args.symbol,
        start_date=date.fromisoformat(args.start),
        end_date=date.fromisoformat(args.end),
        min_grade=min_grade,
        warmup_days=warmup_days,
        max_hold_bars=args.timeout,
        save=args.save,
        verbose=args.verbose,
        record_interactions=args.record_interactions,
    ))


if __name__ == "__main__":
    main()
