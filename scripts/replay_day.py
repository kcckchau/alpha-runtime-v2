"""
replay_day.py — Single-day replay through FeatureEngine → SetupEngine → ThesisEngine.

Supports two data sources:
  --source parquet    Load bars from local Parquet cache (default)
  --source databento  Download bars live from Databento historical API

Usage:
    python scripts/replay_day.py [--symbol MNQ-09] [--date 2026-07-03] [--warmup 5]
    python scripts/replay_day.py --source databento --date 2026-07-03
    python scripts/replay_day.py --source databento --date 2026-07-02 --full-signals

Args:
    --symbol        Ticker to replay (default: MNQ-09)
    --date          Session date YYYY-MM-DD (default: most recent cached day for parquet;
                    required for databento)
    --warmup        Extra days of bars to pre-feed for indicator warm-up (default: 5)
    --source        parquet | databento  (default: parquet)
    --full-signals  Also fetch trades + mbp-1 quotes from Databento and feed them
                    alongside bars (requires --source databento). Activates order-flow
                    signals (bar_delta, absorption, book_imbalance) to match live fidelity.
    --verbose       Print feature snapshot details on every bar
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
from alpha.engines.context.engine import ContextEngine
from alpha.engines.feature.engine import FeatureEngine
from alpha.engines.historical.sources.databento import DatabentoHistoricalDataSource as DatabentoHistoricalSource
from alpha.engines.market_state.engine import MarketStateEngine
from alpha.engines.setup.engine import SetupEngine
from alpha.engines.storage.engine import StorageEngine
from alpha.engines.storage.parquet import ParquetStore
from alpha.engines.thesis.engine import ThesisEngine
from alpha.models.enums import AssetClass, BarTimeframe, RuntimeMode
from alpha.models.events import BarEvent, QuoteEvent, TradeEvent
from alpha.models.symbol import Symbol
from replay_cache import ReplayCache, ReplayResultSaver

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
    cache: ReplayCache | None = None,
    warmup_days: int = 5,
    no_cache: bool = False,
) -> tuple[list[BarEvent], list[BarEvent], list[BarEvent], list[BarEvent]]:
    """Download M1, M5, H1, and D1 bars from Databento for [warmup_start, session_date].

    Returns (m1_bars, m5_bars, h1_bars, d1_bars).
    D1 bars are fetched from 60 days before warmup_start to warm EMA10/EMA20.
    """
    source: DatabentoHistoricalSource | None = None

    start_utc = datetime(
        warmup_start.year, warmup_start.month, warmup_start.day,
        22, 0, 0, tzinfo=_UTC,
    ) - timedelta(days=1)
    end_utc = datetime(
        session_date.year, session_date.month, session_date.day,
        21, 0, 0, tzinfo=_UTC,
    )

    # ── M1 bars ───────────────────────────────────────────────────────────────
    if cache is not None and not no_cache and cache.bars_cached(symbol, session_date, warmup_days):
        print("  M1 bars: loading from cache…", end="", flush=True)
        m1_bars = cache.load_bars(symbol, session_date, warmup_days)
        print(f" {len(m1_bars):,} bars")
    else:
        source = DatabentoHistoricalSource(registry, settings.databento)
        print(f"  Fetching M1 bars from Databento ({start_utc.date()} → {end_utc.date()})…",
              end="", flush=True)
        m1_bars = []
        async for bar in source.fetch_bars(symbol, BarTimeframe.M1, start_utc, end_utc):
            m1_bars.append(bar)
        m1_bars.sort(key=lambda b: b.timestamp)
        print(f" {len(m1_bars):,} bars")
        if cache is not None and m1_bars:
            cache.save_bars(m1_bars, symbol, session_date, warmup_days)

    # ── M5 bars ───────────────────────────────────────────────────────────────
    if cache is not None and not no_cache and cache.m5_bars_cached(symbol, session_date, warmup_days):
        print("  M5 bars: loading from cache…", end="", flush=True)
        m5_bars = cache.load_m5_bars(symbol, session_date, warmup_days)
        print(f" {len(m5_bars):,} bars")
    else:
        if source is None:
            source = DatabentoHistoricalSource(registry, settings.databento)
        print("  Fetching M5 bars from Databento…", end="", flush=True)
        m5_bars = []
        async for bar in source.fetch_bars(symbol, BarTimeframe.M5, start_utc, end_utc):
            m5_bars.append(bar)
        m5_bars.sort(key=lambda b: b.timestamp)
        print(f" {len(m5_bars):,} bars")
        if cache is not None and m5_bars:
            cache.save_m5_bars(m5_bars, symbol, session_date, warmup_days)

    # ── H1 bars ───────────────────────────────────────────────────────────────
    # SMA200 on H1 requires 200 bars ≈ 8.5 trading days. We always fetch at
    # least 12 calendar days back so the SMA200 is warm by RTH open.
    h1_warmup_days = max(warmup_days, 12)
    h1_start_utc = datetime(
        warmup_start.year, warmup_start.month, warmup_start.day,
        22, 0, 0, tzinfo=_UTC,
    ) - timedelta(days=max(0, h1_warmup_days - warmup_days) + 1)

    if cache is not None and not no_cache and cache.h1_bars_cached(symbol, session_date, warmup_days):
        print("  H1 bars: loading from cache…", end="", flush=True)
        h1_bars = cache.load_h1_bars(symbol, session_date, warmup_days)
        print(f" {len(h1_bars):,} bars")
    else:
        if source is None:
            source = DatabentoHistoricalSource(registry, settings.databento)
        print("  Fetching H1 bars from Databento…", end="", flush=True)
        h1_bars = []
        async for bar in source.fetch_bars(symbol, BarTimeframe.H1, h1_start_utc, end_utc):
            h1_bars.append(bar)
        h1_bars.sort(key=lambda b: b.timestamp)
        print(f" {len(h1_bars):,} bars")
        if cache is not None and h1_bars:
            cache.save_h1_bars(h1_bars, symbol, session_date, warmup_days)

    # ── D1 bars ───────────────────────────────────────────────────────────────
    # EMA20 on daily needs 20 trading days; fetch 60 calendar days back to be safe.
    # D1 bars from the current session date are incomplete, so we end at session_date.
    d1_start_utc = datetime(
        warmup_start.year, warmup_start.month, warmup_start.day,
        0, 0, 0, tzinfo=_UTC,
    ) - timedelta(days=60)
    d1_end_utc = datetime(
        session_date.year, session_date.month, session_date.day,
        0, 0, 0, tzinfo=_UTC,
    )

    if cache is not None and not no_cache and cache.d1_bars_cached(symbol, session_date):
        print("  D1 bars: loading from cache…", end="", flush=True)
        d1_bars = cache.load_d1_bars(symbol, session_date)
        print(f" {len(d1_bars):,} bars")
    else:
        if source is None:
            source = DatabentoHistoricalSource(registry, settings.databento)
        print("  Fetching D1 bars from Databento…", end="", flush=True)
        d1_bars = []
        async for bar in source.fetch_bars(symbol, BarTimeframe.D1, d1_start_utc, d1_end_utc):
            d1_bars.append(bar)
        d1_bars.sort(key=lambda b: b.timestamp)
        print(f" {len(d1_bars):,} bars")
        if cache is not None and d1_bars:
            cache.save_d1_bars(d1_bars, symbol, session_date)

    return m1_bars, m5_bars, h1_bars, d1_bars


async def _load_session_orderflow(
    symbol: str,
    session_start: datetime,
    session_end: datetime,
    settings: AlphaSettings,
    registry: SymbolRegistry,
    session_date: date | None = None,
    cache: ReplayCache | None = None,
    no_cache: bool = False,
) -> tuple[list[TradeEvent], list[QuoteEvent]]:
    """Fetch trades + debounced mbp-1 quotes for the session window from Databento.

    Only called when --full-signals is set. Warmup bars don't need order flow;
    we fetch only the session window to keep data volume and cost down.

    mbp-1 fires on every book change including size-only updates. We debounce
    here by keeping only records where bid or ask PRICE changed — same logic
    the live engine uses.
    """
    # ── trades ────────────────────────────────────────────────────────────────
    trades: list[TradeEvent] = []
    if cache is not None and session_date is not None and not no_cache and cache.trades_cached(symbol, session_date):
        print("  Trades: loading from cache…", end="", flush=True)
        trades = cache.load_trades(symbol, session_date)
        print(f" {len(trades):,} records")
    else:
        source = DatabentoHistoricalSource(registry, settings.databento)
        print("  Fetching trades from Databento…", end="", flush=True)
        async for t in source.fetch_trades(symbol, session_start, session_end):
            trades.append(t)
        print(f" {len(trades):,} records")
        if cache is not None and session_date is not None and trades:
            cache.save_trades(trades, symbol, session_date)
            print(f"  Trades cached ({len(trades):,} records)")

    # ── quotes ────────────────────────────────────────────────────────────────
    quotes: list[QuoteEvent] = []
    if cache is not None and session_date is not None and not no_cache and cache.quotes_cached(symbol, session_date):
        print("  Quotes: loading from cache…", end="", flush=True)
        quotes = cache.load_quotes(symbol, session_date)
        print(f" {len(quotes):,} records")
    else:
        if not trades:  # source may already be created above; create if not
            source = DatabentoHistoricalSource(registry, settings.databento)
        print("  Fetching mbp-1 quotes from Databento (debouncing price changes)…", end="", flush=True)
        last_bid: object = None
        last_ask: object = None
        async for q in source.fetch_quotes(symbol, session_start, session_end):
            if q.bid_price != last_bid or q.ask_price != last_ask:
                quotes.append(q)
                last_bid = q.bid_price
                last_ask = q.ask_price
        print(f" {len(quotes):,} records (after debounce)")
        if cache is not None and session_date is not None and quotes:
            cache.save_quotes(quotes, symbol, session_date)
            print(f"  Quotes cached ({len(quotes):,} records)")

    # ── write meta ────────────────────────────────────────────────────────────
    if cache is not None and session_date is not None:
        cache.write_meta(symbol, session_date, {
            "symbol": symbol,
            "session_date": str(session_date),
            "cached_at": datetime.now(_UTC).isoformat(),
            "has_trades": bool(trades),
            "trades_count": len(trades),
            "has_quotes": bool(quotes),
            "quotes_count_debounced": len(quotes),
            "session_start_utc": session_start.isoformat(),
            "session_end_utc": session_end.isoformat(),
        })

    return trades, quotes


# ── Engine pipeline ───────────────────────────────────────────────────────────

async def replay(
    symbol: str,
    session_date: date,
    warmup_days: int,
    verbose: bool,
    source: str,
    full_signals: bool = False,
    no_cache: bool = False,
    save_results: bool = False,
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
    context_engine      = ContextEngine(settings, bus, registry, calendar, clock)
    market_state_engine = MarketStateEngine(settings, bus, registry)
    setup_engine        = SetupEngine(settings, bus, registry)
    thesis_engine       = ThesisEngine(settings, bus, registry)

    market_state_engine.set_feature_engine(feature_engine)
    setup_engine.set_feature_engine(feature_engine)
    setup_engine.set_market_state_engine(market_state_engine)
    thesis_engine.set_feature_engine(feature_engine)
    context_engine.set_feature_engine(feature_engine)
    market_state_engine.set_context_engine(context_engine)
    setup_engine.set_context_engine(context_engine)
    thesis_engine.set_context_engine(context_engine)

    await feature_engine.initialize()
    await context_engine.initialize()
    await market_state_engine.initialize()
    await setup_engine.initialize()
    await thesis_engine.initialize()

    # ContextEngine must start AFTER FeatureEngine so its BAR subscription
    # fires second — guaranteeing FeatureEngine.get_snapshot() is current.
    await feature_engine.start()
    await context_engine.start()
    await market_state_engine.start()
    await setup_engine.start()
    await thesis_engine.start()

    # ── Cache setup ───────────────────────────────────────────────────────────
    cache: ReplayCache | None = None
    if source == "databento":
        cache = ReplayCache(_REPO)

    # ── Load bars ─────────────────────────────────────────────────────────────
    warmup_start = session_date - timedelta(days=warmup_days + 2)
    signals_label = " +full-signals" if full_signals else ""
    cache_label = " no-cache" if no_cache else ""
    print(f"\n{_BOLD}Replay: {symbol} session {session_date}  "
          f"(source={source}{signals_label}{cache_label}, warmup from {warmup_start}){_RESET}\n")

    if source == "databento":
        print("  Loading bars…")
        all_bars, all_m5_bars, all_h1_bars, all_d1_bars = await _load_from_databento(
            symbol, warmup_start, session_date, settings, registry,
            cache=cache, warmup_days=warmup_days, no_cache=no_cache,
        )
    else:
        all_bars = _load_from_parquet(symbol, warmup_start, session_date, settings)
        all_m5_bars = []
        all_h1_bars = []
        all_d1_bars = []

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

    warmup_bars  = [b for b in all_bars    if b.timestamp < session_date_utc_start]
    session_bars = [b for b in all_bars    if b.timestamp >= session_date_utc_start]
    warmup_m5    = [b for b in all_m5_bars if b.timestamp < session_date_utc_start]
    session_m5   = [b for b in all_m5_bars if b.timestamp >= session_date_utc_start]
    warmup_h1    = [b for b in all_h1_bars if b.timestamp < session_date_utc_start]
    session_h1   = [b for b in all_h1_bars if b.timestamp >= session_date_utc_start]
    # D1 bars are always prior closed sessions — all go into warmup.
    warmup_d1    = list(all_d1_bars)

    print(f"  Warmup bars  : {len(warmup_bars)} M1 + {len(warmup_m5)} M5 + {len(warmup_h1)} H1 + {len(warmup_d1)} D1")
    print(f"  Session bars : {len(session_bars)} M1 + {len(session_m5)} M5 + {len(session_h1)} H1")

    if not session_bars:
        print(f"{_RED}No session bars found — check date or Databento entitlements.{_RESET}")
        return

    # ── Optionally load order-flow data ───────────────────────────────────────
    session_end_utc = datetime(
        session_date.year, session_date.month, session_date.day,
        21, 0, 0, tzinfo=_UTC,
    )
    session_trades: list[TradeEvent] = []
    session_quotes: list[QuoteEvent] = []
    if full_signals:
        if source != "databento":
            print(f"{_YELLOW}Warning: --full-signals requires --source databento; ignoring.{_RESET}")
            full_signals = False
        else:
            session_trades, session_quotes = await _load_session_orderflow(
                symbol, session_date_utc_start, session_end_utc, settings, registry,
                session_date=session_date, cache=cache, no_cache=no_cache,
            )

    # ── Feed warmup bars (M1 + M5 + H1 + D1 interleaved by timestamp) ────────
    # Priority within same timestamp: D1(-1) → H1(0) → M5(1) → M1(2)
    # HTF bars processed first so EMA values are ready before the M1 snapshot is built.
    warmup_all = sorted(
        [(b.timestamp, 3, b) for b in warmup_bars] +
        [(b.timestamp, 2, b) for b in warmup_m5] +
        [(b.timestamp, 1, b) for b in warmup_h1] +
        [(b.timestamp, 0, b) for b in warmup_d1],
        key=lambda x: (x[0], x[1]),
    )
    print(f"\n{_DIM}Feeding {len(warmup_all)} warmup bars (M1+M5+H1+D1)…{_RESET}", end="", flush=True)
    for _, _, bar in warmup_all:
        await bus.publish(bar)
        await bus.flush()
    print(f" done.{_RESET}\n")

    # ── Build merged session event stream (bars + optional trades/quotes) ─────
    # Priority for same-timestamp ties: trades(0) → quotes(1) → bars(2)
    # This mirrors live behaviour: tick events arrive intra-minute, bar arrives at close.
    # Interleave all timeframes. Priority for same-timestamp ties:
    #   trades(0) → quotes(1) → H1(2) → M5(3) → M1(4)
    # D1 is not in the session stream (current day's D1 bar is incomplete until EOD).
    # HTF bars before M1 so EMAs are updated before the M1 snapshot is built.
    merged: list[tuple] = []
    for b in session_h1:
        merged.append((b.timestamp, 2, b))
    for b in session_m5:
        merged.append((b.timestamp, 3, b))
    for b in session_bars:
        merged.append((b.timestamp, 4, b))
    if full_signals and (session_trades or session_quotes):
        for t in session_trades:
            merged.append((t.timestamp, 0, t))
        for q in session_quotes:
            merged.append((q.timestamp, 1, q))
    merged.sort(key=lambda x: (x[0], x[1]))
    session_stream = [item[2] for item in merged]

    extra = ""
    if full_signals and (session_trades or session_quotes):
        extra = f", {len(session_trades):,} trades, {len(session_quotes):,} quotes"
    print(f"  Session stream: {len(session_stream):,} events "
          f"({len(session_bars)} M1, {len(session_m5)} M5, {len(session_h1)} H1{extra})\n")

    # ── Result saver ──────────────────────────────────────────────────────────
    saver = ReplayResultSaver(_REPO, symbol, session_date) if save_results else None

    # ── Feed session events with live output ──────────────────────────────────
    prev_thesis_state = None
    prev_thesis_type  = None

    print(f"{'Time':>6}  {'Close':>10}  {'VWAP':>10}  {'Thesis':>30}  {'State':>12}  {'Conf':>6}  {'Setups':>6}")
    print("-" * 95)

    for event in session_stream:
        await bus.publish(event)
        # Only flush + render output on bar events
        if not isinstance(event, BarEvent):
            continue
        bar = event
        await bus.flush()

        ts            = _ts(bar.timestamp)
        snap          = feature_engine.get_snapshot(symbol)
        ctx           = context_engine.get_context(symbol)
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
        if verbose and ctx:
            wz = f"{ctx.nearest_war_zone}@{_fmt_dec(ctx.nearest_war_zone_price)} ({_fmt_dec(ctx.nearest_war_zone_dist, 1)}pts)" if ctx.nearest_war_zone else "—"
            gap_str = f"{ctx.gap_points:+.2f}pts ({ctx.gap_pct:+.2f}%)" if ctx.gap_points is not None else "—"
            print(f"         ONH={_fmt_dec(ctx.onh)}  ONL={_fmt_dec(ctx.onl)}  "
                  f"PDH={_fmt_dec(ctx.pdh)}  PDL={_fmt_dec(ctx.pdl)}  "
                  f"Gap={gap_str}  NearestWZ={wz}")

        for s in active_setups:
            stype  = s.setup_type.value if hasattr(s.setup_type, "value") else str(s.setup_type)
            sstate = s.state.value if hasattr(s.state, "value") else str(s.state)
            print(f"         {_CYAN}SETUP: {stype} | {sstate} | score={s.score}{_RESET}")

        if saver:
            market_state = market_state_engine.get_state(symbol)
            saver.record_bar(bar, snap, thesis, active_setups, market_state,
                             prev_thesis_type, prev_thesis_state)

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

    ctx_final = context_engine.get_context(symbol)
    if ctx_final:
        print(f"\nSession context:")
        print(f"  ONH      : {_fmt_dec(ctx_final.onh)}")
        print(f"  ONL      : {_fmt_dec(ctx_final.onl)}")
        print(f"  PDH      : {_fmt_dec(ctx_final.pdh)}")
        print(f"  PDL      : {_fmt_dec(ctx_final.pdl)}")
        print(f"  PrevClose: {_fmt_dec(ctx_final.prev_rth_close)}")
        print(f"  RTH Open : {_fmt_dec(ctx_final.rth_open)}")
        if ctx_final.gap_points is not None:
            print(f"  Gap      : {ctx_final.gap_points:+.2f} pts ({ctx_final.gap_pct:+.2f}%)")
        if ctx_final.nearest_war_zone:
            print(f"  Nearest WZ: {ctx_final.nearest_war_zone} @ {_fmt_dec(ctx_final.nearest_war_zone_price)}")

    snap = feature_engine.get_snapshot(symbol)
    if snap:
        print(f"\nFinal indicators:")
        print(f"  VWAP     : {_fmt_dec(snap.vwap)}")
        print(f"  EMA9     : {_fmt_dec(snap.ema_9)}")
        print(f"  EMA20    : {_fmt_dec(snap.ema_20)}")
        print(f"  EMA50    : {_fmt_dec(snap.ema_50)}")
        print(f"  ATR14    : {_fmt_dec(snap.atr_14)}")
        print(f"  ORB H    : {_fmt_dec(snap.orb_high)}")
        print(f"  ORB L    : {_fmt_dec(snap.orb_low)}")
        print(f"  EMA10 1d : {_fmt_dec(snap.ema10_1d)}")
        print(f"  EMA20 1d : {_fmt_dec(snap.ema20_1d)}")

    if saver:
        thesis_final = thesis_engine.get_thesis(symbol)
        snap_final   = feature_engine.get_snapshot(symbol)
        active_setups_final = setup_engine.active_setups(symbol)
        json_path, csv_path = saver.save(
            meta={
                "symbol": symbol,
                "session_date": str(session_date),
                "source": source,
                "warmup_days": warmup_days,
            },
            full_signals=full_signals,
            thesis_final=thesis_final.dominant if thesis_final else None,
            snap_final=snap_final,
            active_setups_at_close=active_setups_final,
        )
        print(f"\n{_GREEN}Results saved:{_RESET}")
        print(f"  JSON → {json_path}")
        print(f"  CSV  → {csv_path}")

    await thesis_engine.stop()
    await setup_engine.stop()
    await market_state_engine.stop()
    await context_engine.stop()
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
    p.add_argument("--full-signals", action="store_true",
                   help="Also fetch trades+mbp-1 from Databento to activate order-flow signals "
                        "(requires --source databento)")
    p.add_argument("--no-cache", action="store_true",
                   help="Force re-download from Databento even if a local cache exists")
    p.add_argument("--save-results", action="store_true",
                   help="Save structured JSON + CSV results to data/replay_results/ after replay")
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

    asyncio.run(replay(
        args.symbol, session_date, args.warmup, args.verbose,
        args.source,
        full_signals=args.full_signals,
        no_cache=args.no_cache,
        save_results=args.save_results,
    ))


if __name__ == "__main__":
    main()
