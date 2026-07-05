"""
replay_day.py — Single-day replay through FeatureEngine → SetupEngine → ThesisEngine.

Supports two data sources:
  --source parquet    Load bars from local Parquet cache (default)
  --source databento  Download bars live from Databento historical API

Usage:
    python scripts/replay_day.py [--symbol MNQ-09] [--date 2026-07-03] [--warmup 5]
    python scripts/replay_day.py --source databento --date 2026-07-03

Args:
    --symbol    Ticker to replay (default: MNQ-09)
    --date      Session date YYYY-MM-DD (default: most recent cached day for parquet;
                required for databento)
    --warmup    Extra days of bars to pre-feed for indicator warm-up (default: 5)
    --source    parquet | databento  (default: parquet)
    --verbose   Print feature snapshot details on every bar
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import AsyncIterator

# ── project root on path ──────────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings, RuntimeSettings, StorageSettings
from alpha.core.clock import WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.feature.engine import FeatureEngine
from alpha.engines.historical.sources.databento import DatabentoHistoricalDataSource as DatabentoHistoricalSource
from alpha.engines.market_state.engine import MarketStateEngine
from alpha.engines.setup.engine import SetupEngine
from alpha.engines.storage.engine import StorageEngine
from alpha.engines.storage.parquet import ParquetStore
from alpha.engines.thesis.engine import ThesisEngine
from alpha.models.enums import AssetClass, BarTimeframe, RuntimeMode
from alpha.models.events import BarEvent
from alpha.models.symbol import Symbol

_UTC = timezone.utc
_ET  = timezone(timedelta(hours=-4))   # EDT; close enough for display

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)-8s %(name)s: %(message)s",
)

# ── ANSI colours ──────────────────────────────────────────────────────────────
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_GREEN  = "\033[32m"
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_CYAN   = "\033[36m"
_DIM    = "\033[2m"


def _ts(dt: datetime) -> str:
    return dt.astimezone(_ET).strftime("%H:%M")


def _fmt_dec(v: Decimal | None, digits: int = 2) -> str:
    if v is None:
        return "—"
    return f"{v:,.{digits}f}"


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v*100:.1f}%"


# ── Bar loading ───────────────────────────────────────────────────────────────

def _load_from_parquet(
    symbol: str,
    warmup_start: date,
    session_date: date,
    settings: AlphaSettings,
) -> list[BarEvent]:
    parquet = ParquetStore(settings.storage)
    bars: list[BarEvent] = []
    d = warmup_start
    while d <= session_date:
        table = parquet.read_range(f"bars/{BarTimeframe.M1}", symbol, d, d)
        for row in table.to_pylist():
            bar = StorageEngine._row_to_bar_event(row, BarTimeframe.M1)
            if bar.symbol != symbol:
                bar = bar.model_copy(update={"symbol": symbol})
            bars.append(bar)
        d += timedelta(days=1)
    bars.sort(key=lambda b: b.timestamp)
    return bars


async def _load_from_databento(
    symbol: str,
    warmup_start: date,
    session_date: date,
    settings: AlphaSettings,
    registry: SymbolRegistry,
) -> list[BarEvent]:
    """Download M1 bars from Databento for [warmup_start, session_date] inclusive.

    CME MNQ July 3 2026 is a holiday-shortened session. The full CME Globex
    session window is 18:00 ET the prior calendar day → 17:00 ET the session
    date (or early close where applicable). We request a wide window and let
    Databento return whatever it has; the engine will see the real close.
    """
    source = DatabentoHistoricalSource(registry, settings.databento)

    # Session window: 18:00 ET warmup_start-1 → end of session_date in UTC
    start_utc = datetime(
        warmup_start.year, warmup_start.month, warmup_start.day,
        22, 0, 0, tzinfo=_UTC,                # 18:00 ET = 22:00 UTC (EDT)
    ) - timedelta(days=1)
    end_utc = datetime(
        session_date.year, session_date.month, session_date.day,
        21, 0, 0, tzinfo=_UTC,                # 17:00 ET = 21:00 UTC
    )

    print(f"  Databento window: {start_utc.isoformat()} → {end_utc.isoformat()}")

    bars: list[BarEvent] = []
    async for bar in source.fetch_bars(symbol, BarTimeframe.M1, start_utc, end_utc):
        bars.append(bar)

    bars.sort(key=lambda b: b.timestamp)
    return bars


# ── Engine pipeline ───────────────────────────────────────────────────────────

async def replay(
    symbol: str,
    session_date: date,
    warmup_days: int,
    verbose: bool,
    source: str,
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

    # Symbol object — root_symbol drives Databento continuous contract lookup
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
    registry = SymbolRegistry()
    registry.register(sym_obj)

    calendar = calendar_for_symbol(sym_obj)
    bus = EventBus(queue_size=5000)
    await bus.start()

    clock = WallClock()
    feature_engine      = FeatureEngine(settings, bus, registry, calendar, clock)
    market_state_engine = MarketStateEngine(settings, bus, registry)
    setup_engine        = SetupEngine(settings, bus, registry)
    thesis_engine       = ThesisEngine(settings, bus, registry)

    market_state_engine.set_feature_engine(feature_engine)
    setup_engine.set_feature_engine(feature_engine)
    setup_engine.set_market_state_engine(market_state_engine)
    thesis_engine.set_feature_engine(feature_engine)

    await feature_engine.initialize()
    await market_state_engine.initialize()
    await setup_engine.initialize()
    await thesis_engine.initialize()

    await feature_engine.start()
    await market_state_engine.start()
    await setup_engine.start()
    await thesis_engine.start()

    # ── Load bars ─────────────────────────────────────────────────────────────
    warmup_start = session_date - timedelta(days=warmup_days + 2)
    print(f"\n{_BOLD}Replay: {symbol} session {session_date}  "
          f"(source={source}, warmup from {warmup_start}){_RESET}\n")

    if source == "databento":
        print("  Fetching from Databento API…")
        all_bars = await _load_from_databento(
            symbol, warmup_start, session_date, settings, registry
        )
    else:
        all_bars = _load_from_parquet(symbol, warmup_start, session_date, settings)

    if not all_bars:
        print(f"{_RED}No bars returned for {symbol} {warmup_start}→{session_date}.{_RESET}")
        return

    # Split into warmup (before session date) and session bars
    # For CME futures, the session date's bars start at 18:00 ET the prior calendar day.
    # We define "session" as bars whose ET date equals session_date OR the overnight
    # window that belongs to this CME session (18:00–23:59 ET on the prior calendar day).
    session_date_utc_start = datetime(
        session_date.year, session_date.month, session_date.day,
        22, 0, 0, tzinfo=_UTC,
    ) - timedelta(days=1)   # 18:00 ET prior day = session open

    warmup_bars  = [b for b in all_bars if b.timestamp < session_date_utc_start]
    session_bars = [b for b in all_bars if b.timestamp >= session_date_utc_start]

    print(f"  Warmup bars  : {len(warmup_bars)}")
    print(f"  Session bars : {len(session_bars)}")

    if not session_bars:
        print(f"{_RED}No session bars found — check date or Databento entitlements.{_RESET}")
        return

    # ── Feed warmup bars ──────────────────────────────────────────────────────
    print(f"\n{_DIM}Feeding {len(warmup_bars)} warmup bars…{_RESET}", end="", flush=True)
    for bar in warmup_bars:
        await bus.publish(bar)
        await bus.flush()
    print(f" done.{_RESET}\n")

    # ── Feed session bars with live output ────────────────────────────────────
    prev_thesis_state = None
    prev_thesis_type  = None

    print(f"{'Time':>6}  {'Close':>10}  {'VWAP':>10}  {'Thesis':>30}  {'State':>12}  {'Conf':>6}  {'Setups':>6}")
    print("-" * 95)

    for bar in session_bars:
        await bus.publish(bar)
        await bus.flush()

        ts            = _ts(bar.timestamp)
        snap          = feature_engine.get_snapshot(symbol)
        thesis        = thesis_engine.get_thesis(symbol)
        active_setups = setup_engine.active_setups(symbol)
        vwap_str      = _fmt_dec(snap.vwap if snap else None)

        if thesis and thesis.dominant:
            dom = thesis.dominant
            ttype  = dom.thesis_type.value if hasattr(dom.thesis_type, "value") else str(dom.thesis_type)
            tstate = dom.state.value if hasattr(dom.state, "value") else str(dom.state)
            changed = (dom.state != prev_thesis_state or dom.thesis_type != prev_thesis_type)
            prev_thesis_state = dom.state
            prev_thesis_type  = dom.thesis_type

            sv = tstate.lower()
            col = (_GREEN + _BOLD if "ready" in sv
                   else _YELLOW if "building" in sv
                   else _RED if ("invalid" in sv or "expired" in sv)
                   else _DIM)

            marker = " *" if changed else "  "
            line = (
                f"{ts:>6}  {_fmt_dec(bar.close, 2):>10}  {vwap_str:>10}  "
                f"{ttype[-30:]:>30}  {col}{tstate:>12}{_RESET}  "
                f"{_fmt_pct(dom.confidence):>6}  {len(active_setups):>6}{marker}"
            )
        else:
            prev_thesis_state = None
            prev_thesis_type  = None
            line = (
                f"{ts:>6}  {_fmt_dec(bar.close, 2):>10}  {vwap_str:>10}  "
                f"{'—':>30}  {_DIM}{'none':>12}{_RESET}  {'—':>6}  {len(active_setups):>6}"
            )

        print(line)

        if verbose and snap:
            print(f"         EMA9={_fmt_dec(snap.ema_9)}  EMA20={_fmt_dec(snap.ema_20)}  "
                  f"ATR14={_fmt_dec(snap.atr_14)}  AboveVWAP={'Y' if snap.is_above_vwap else 'N'}  "
                  f"ORBhigh={_fmt_dec(snap.orb_high)}  ORBlow={_fmt_dec(snap.orb_low)}")

        for s in active_setups:
            stype  = s.setup_type.value if hasattr(s.setup_type, "value") else str(s.setup_type)
            sstate = s.state.value if hasattr(s.state, "value") else str(s.state)
            print(f"         {_CYAN}SETUP: {stype} | {sstate} | score={s.score}{_RESET}")

    # ── Final summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 95)
    print(f"{_BOLD}Session complete.{_RESET}")

    thesis = thesis_engine.get_thesis(symbol)
    if thesis and thesis.dominant:
        dom = thesis.dominant
        print(f"\nFinal thesis:")
        print(f"  Type       : {dom.thesis_type}")
        print(f"  State      : {dom.state}")
        print(f"  Confidence : {_fmt_pct(dom.confidence)}")
        print(f"  Entry      : {_fmt_dec(dom.entry)}")
        print(f"  Stop       : {_fmt_dec(dom.stop)}")
        print(f"  Target     : {_fmt_dec(dom.target)}")
        print(f"  Bars alive : {dom.bars_alive}")
        if dom.evidence:
            print("  Evidence:")
            for ev in dom.evidence:
                print(f"    - {ev}")
        if dom.commit_conditions:
            print("  Need to commit:")
            for c in dom.commit_conditions:
                print(f"    - {c}")

    active_setups = setup_engine.active_setups(symbol)
    if active_setups:
        print(f"\nActive setups at session end: {len(active_setups)}")
        for s in active_setups:
            print(f"  {s.setup_type} | {s.state} | score={s.score}")

    snap = feature_engine.get_snapshot(symbol)
    if snap:
        print(f"\nFinal indicators:")
        print(f"  VWAP  : {_fmt_dec(snap.vwap)}")
        print(f"  EMA9  : {_fmt_dec(snap.ema_9)}")
        print(f"  EMA20 : {_fmt_dec(snap.ema_20)}")
        print(f"  EMA50 : {_fmt_dec(snap.ema_50)}")
        print(f"  ATR14 : {_fmt_dec(snap.atr_14)}")
        print(f"  ORB H : {_fmt_dec(snap.orb_high)}")
        print(f"  ORB L : {_fmt_dec(snap.orb_low)}")

    await thesis_engine.stop()
    await setup_engine.stop()
    await market_state_engine.stop()
    await feature_engine.stop()
    await bus.stop()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _find_most_recent_date(symbol: str) -> date | None:
    base = _REPO / "data" / "parquet" / "bars" / "1m" / symbol
    if not base.exists():
        return None
    dates = []
    for p in base.rglob("data.parquet"):
        parts = {seg.split("=")[0]: seg.split("=")[1] for seg in p.parts if "=" in seg}
        try:
            dates.append(date(int(parts["year"]), int(parts["month"]), int(parts["day"])))
        except (KeyError, ValueError):
            pass
    return max(dates) if dates else None


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay a single CME session through the engine pipeline")
    p.add_argument("--symbol",  default="MNQ-09",    help="Ticker (default: MNQ-09)")
    p.add_argument("--date",    default=None,         help="Session date YYYY-MM-DD")
    p.add_argument("--warmup",  type=int, default=5,  help="Warmup days (default: 5)")
    p.add_argument("--source",  default="parquet",    choices=["parquet", "databento"],
                   help="Data source (default: parquet)")
    p.add_argument("--verbose", action="store_true",  help="Print indicator snapshot each bar")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    if args.date:
        session_date = date.fromisoformat(args.date)
    elif args.source == "databento":
        print("Error: --date is required when using --source databento")
        sys.exit(1)
    else:
        session_date = _find_most_recent_date(args.symbol)
        if session_date is None:
            print(f"No cached bars found for {args.symbol} in data/parquet/bars/1m/")
            sys.exit(1)
        print(f"Auto-detected most recent cached date: {session_date}")

    asyncio.run(replay(args.symbol, session_date, args.warmup, args.verbose, args.source))


if __name__ == "__main__":
    main()
