"""
On-demand date backfill worker.

Runs as an asyncio background task triggered by POST /runtime/backfill-date.

Flow for a given (symbol, date):
  1. Check whether 1m bars exist in Parquet.
     - If missing, connect to IBKR and fetch them, saving to Parquet.
  2. Load all 1m BarEvents from Parquet for the date.
  3. Wire a fresh EventBus + FeatureEngine + MarketStateEngine + SetupEngine.
  4. Replay every bar event through the pipeline (no speed throttle).
  5. Extract the resulting SessionSetupContext from the SetupEngine.
  6. Persist it via SessionContextStore.
  7. Update the job record so the API can report progress.

The worker is self-contained — it does not touch the live runtime state.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Any

from alpha.config.settings import AlphaSettings
from alpha.core.clock import WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.historical.sources.base import HistoricalDataSource
from alpha.engines.storage.parquet import ParquetStore
from alpha.engines.storage.session_context_store import SessionContextStore
from alpha.models.enums import AssetClass, BarTimeframe, EventType
from alpha.models.events import AnyEvent
from alpha.models.symbol import Symbol

logger = logging.getLogger(__name__)

# ─── Known futures tickers (infer asset class without broker call) ────────────

_FUTURES_TICKERS = frozenset({
    "MNQ", "NQ", "ES", "MES", "RTY", "M2K",
    "YM", "MYM", "GC", "MGC", "SI", "CL", "QM", "NG",
})

# ─── Job state ────────────────────────────────────────────────────────────────

class JobStatus:
    PENDING = "pending"
    FETCHING_BARS = "fetching_bars"
    DETECTING_SETUPS = "detecting_setups"
    DONE = "done"
    ERROR = "error"


def _make_job(symbol: str, d: date) -> dict[str, Any]:
    return {
        "job_id": f"{symbol}:{d.isoformat()}",
        "symbol": symbol,
        "date": d.isoformat(),
        "status": JobStatus.PENDING,
        "bars_fetched": 0,
        "setups_detected": 0,
        "error": None,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    }


def job_id(symbol: str, d: date) -> str:
    return f"{symbol}:{d.isoformat()}"


# ─── Symbol inference ─────────────────────────────────────────────────────────

def _infer_symbol(ticker: str) -> Symbol:
    root = ticker.rstrip("0123456789")
    asset_class = (
        AssetClass.FUTURE if root.upper() in _FUTURES_TICKERS else AssetClass.EQUITY
    )
    exchange = "CME" if asset_class == AssetClass.FUTURE else "NASDAQ"
    return Symbol(ticker=ticker.upper(), exchange=exchange, asset_class=asset_class)


# ─── Main worker ──────────────────────────────────────────────────────────────

async def run_date_backfill(
    symbol: str,
    d: date,
    settings: AlphaSettings,
    jobs: dict[str, dict[str, Any]],
) -> None:
    """
    Background task. Updates `jobs[job_id(symbol, d)]` in-place as it runs.
    Safe to run concurrently for different (symbol, date) pairs.
    """
    jid = job_id(symbol, d)
    job = jobs.get(jid)
    if job is None:
        logger.error("Job %s not found in registry", jid)
        return

    data_root = settings.storage.parquet_root.parent
    parquet = ParquetStore(settings.storage)
    context_store = SessionContextStore(data_root)

    try:
        # ── Step 1: ensure 1m bars are in Parquet ────────────────────────────
        has_bars = parquet.exists(f"bars/{BarTimeframe.M1}", symbol.upper(), d)

        if not has_bars:
            job["status"] = JobStatus.FETCHING_BARS
            logger.info("[backfill] Fetching 1m bars for %s %s", symbol, d)
            bars_count = await _fetch_bars_from_best_source(symbol, d, settings, parquet)
            job["bars_fetched"] = bars_count
            if bars_count == 0:
                raise RuntimeError(
                    f"No bars found for {symbol} on {d}. "
                    "Provide a JSON file in data/imports/{symbol}/{date}.json "
                    "or ensure IBKR (TWS/Gateway) is running."
                )
        else:
            # Count existing bars
            table = parquet.read(f"bars/{BarTimeframe.M1}", symbol.upper(), d)
            job["bars_fetched"] = len(table) if table is not None else 0
            logger.info(
                "[backfill] 1m bars already exist for %s %s (%d bars)",
                symbol, d, job["bars_fetched"],
            )

        # ── Step 1b: best-effort fetch prior-day bars ─────────────────────────
        # The chart pulls histStart = D-1 for 24h futures (MNQ, NQ, ES, …) so
        # that overnight bars (18:00–23:59 ET) are visible.  Those bars live in
        # the D-1 Parquet partition.  The warmup phase also needs several prior
        # trading days to seed ATR/EMA/RVOL.  Fetch any missing prior days here
        # so that both the chart and the feature-engine warmup have real data.
        await _ensure_prior_bars(symbol, d, settings, parquet)

        # ── Step 2: replay through feature+setup pipeline ────────────────────
        job["status"] = JobStatus.DETECTING_SETUPS
        logger.info("[backfill] Running setup detection replay for %s %s", symbol, d)
        context = await _replay_setup_detection(symbol, d, settings, parquet)

        if context is not None:
            context_store.save(symbol.upper(), d, context)
            job["setups_detected"] = len(context.setups)
            logger.info(
                "[backfill] Done: %s %s — %d setups detected",
                symbol, d, len(context.setups),
            )
        else:
            logger.warning("[backfill] Replay produced no session context for %s %s", symbol, d)

        job["status"] = JobStatus.DONE

    except Exception as exc:
        logger.exception("[backfill] Failed for %s %s: %s", symbol, d, exc)
        job["status"] = JobStatus.ERROR
        job["error"] = str(exc)

    finally:
        job["finished_at"] = datetime.now(timezone.utc).isoformat()


# ─── Prior-day bar seeder ─────────────────────────────────────────────────────

async def _ensure_prior_bars(
    symbol: str,
    d: date,
    settings: AlphaSettings,
    parquet: ParquetStore,
) -> None:
    """
    Best-effort: ensure bars exist in Parquet for the calendar day immediately
    before `d` PLUS the `_WARMUP_DAYS` prior trading days.

    - The prior calendar day (D-1) is required so the chart can display the
      overnight session for 24h futures (18:00–23:59 ET bars sit in the D-1
      partition because those timestamps are UTC of the previous day).
    - The warmup trading days feed ATR, EMA, and RVOL so that setup detection
      is properly seeded.

    Failures are logged and swallowed — missing prior-day bars degrade quality
    but must not abort the target-date backfill.
    """
    from datetime import timedelta

    from alpha.calendar.resolver import calendar_for_symbol

    sym = _infer_symbol(symbol)
    calendar = calendar_for_symbol(sym)

    # Collect calendar D-1 + the last _WARMUP_DAYS trading days before D.
    prior_dates: list[date] = []

    prev_cal = d - timedelta(days=1)
    if prev_cal not in prior_dates:
        prior_dates.append(prev_cal)

    prev_trading = d
    for _ in range(_WARMUP_DAYS):
        prev_trading = calendar.prev_trading_day(prev_trading)
        if prev_trading not in prior_dates:
            prior_dates.append(prev_trading)

    for prior in prior_dates:
        if parquet.exists(f"bars/{BarTimeframe.M1}", symbol.upper(), prior):
            logger.debug("[backfill] Prior-day bars already in Parquet: %s %s", symbol, prior)
            continue
        logger.info("[backfill] Fetching prior-day bars for %s %s (chart/warmup)", symbol, prior)
        try:
            count = await _fetch_bars_from_best_source(symbol, prior, settings, parquet)
            logger.info("[backfill] Prior-day bars fetched: %s %s (%d bars)", symbol, prior, count)
        except Exception as exc:
            logger.warning(
                "[backfill] Could not fetch prior-day bars for %s %s: %s — skipping",
                symbol, prior, exc,
            )


# ─── Bar fetch: JSON first, IBKR fallback ────────────────────────────────────

async def _fetch_bars_from_best_source(
    symbol: str,
    d: date,
    settings: AlphaSettings,
    parquet: ParquetStore,
) -> int:
    """
    Fetch 1m bars for symbol+date and write them to Parquet.

    Priority:
      1. JSON import file  → data/imports/{SYMBOL}/{YYYY-MM-DD}.json
      2. IBKR              → requires live TWS/Gateway connection

    Returns the number of bars written (0 if nothing was found).
    """
    from alpha.engines.historical.sources.json_file import JSONFileHistoricalSource

    data_root = settings.storage.parquet_root.parent
    imports_dir = data_root / "imports"
    json_source = JSONFileHistoricalSource(imports_dir)

    if json_source.has_date(symbol, d):
        logger.info("[backfill] Using JSON import file for %s %s", symbol, d)
        return await _drain_source_to_parquet(json_source, symbol, d, parquet)

    # No local file — try IBKR
    logger.info("[backfill] No JSON file found, trying IBKR for %s %s", symbol, d)
    return await _fetch_bars_from_ibkr(symbol, d, settings, parquet)


async def _drain_source_to_parquet(
    source: "HistoricalDataSource",
    symbol: str,
    d: date,
    parquet: ParquetStore,
) -> int:
    """Stream BarEvents from any source and write them all to Parquet."""
    import pyarrow as pa

    start_dt = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(d, datetime.max.time().replace(microsecond=0), tzinfo=timezone.utc)

    count = 0
    rows: list[dict] = []

    async for event in source.fetch_bars(symbol, BarTimeframe.M1, start_dt, end_dt):
        rows.append({
            "event_id": str(event.metadata.event_id),
            "event_type": str(event.event_type),
            "symbol": event.symbol,
            "timestamp": event.timestamp.isoformat(),
            "source": str(event.metadata.source),
            "received_at": event.metadata.received_at.isoformat(),
            "is_replay": True,
            "sequence_num": event.metadata.sequence_num,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "timeframe": str(event.timeframe),
            "open": str(event.open),
            "high": str(event.high),
            "low": str(event.low),
            "close": str(event.close),
            "volume": event.volume,
            "vwap": str(event.vwap) if event.vwap is not None else None,
            "trade_count": event.trade_count,
            "is_partial": event.is_partial,
        })
        count += 1

    if rows:
        table = pa.Table.from_pylist(rows)
        parquet.write(table, f"bars/{BarTimeframe.M1}", symbol.upper(), d)
        logger.info("[backfill] Wrote %d bars to Parquet for %s %s", count, symbol, d)

    return count


async def _fetch_bars_from_ibkr(
    symbol: str,
    d: date,
    settings: AlphaSettings,
    parquet: ParquetStore,
) -> int:
    """Connect to IBKR, fetch 1m bars for symbol+date, write to Parquet."""
    from alpha.engines.historical.sources.ibkr import IBKRHistoricalDataSource
    from alpha.engines.ibkr.connection import IBKRConnection

    sym = _infer_symbol(symbol)
    registry = SymbolRegistry()
    registry.register(sym)

    # Use backfill_client_id so this connection does not conflict with the
    # live trading engine (alpha run) which uses the primary client_id.
    backfill_ibkr = settings.ibkr.model_copy(update={"client_id": settings.ibkr.backfill_client_id})
    conn = IBKRConnection(backfill_ibkr)
    source = IBKRHistoricalDataSource(conn, registry, backfill_ibkr)

    try:
        return await _drain_source_to_parquet(source, symbol, d, parquet)
    finally:
        await conn.disconnect()


# ─── Setup detection replay ───────────────────────────────────────────────────

_WARMUP_DAYS = 5  # trading days of prior M1 bars fed to the feature engine before target date


async def _replay_setup_detection(
    symbol: str,
    d: date,
    settings: AlphaSettings,
    parquet: ParquetStore,
) -> "SessionSetupContext | None":
    """
    Replay bars through a fresh FeatureEngine → MarketStateEngine → SetupEngine pipeline.

    Warm-up phase: replay the 2 prior trading days so that ATR(14), RVOL,
    EMA, and session-phase indicators are properly seeded before the target
    date starts.  The SetupEngine rolls its session on the first bar of the
    target date, so warmup-day setups are discarded automatically.

    Returns the SessionSetupContext for date `d`, or None if no bars exist.
    """
    from alpha.calendar.resolver import calendar_for_symbol
    from alpha.engines.feature.engine import FeatureEngine
    from alpha.engines.market_state.engine import MarketStateEngine
    from alpha.engines.setup.engine import SetupEngine
    from alpha.engines.storage.engine import StorageEngine
    from alpha.models.events import MarketStateEvent

    sym = _infer_symbol(symbol)
    registry = SymbolRegistry()
    registry.register(sym)

    calendar = calendar_for_symbol(sym)
    clock = WallClock()
    bus = EventBus()
    await bus.start()  # must start bus before publish() calls are live

    feature_engine = FeatureEngine(settings, bus, registry, calendar, clock)
    market_state_engine = MarketStateEngine(settings, bus, registry)
    setup_engine = SetupEngine(settings, bus, registry)

    market_state_engine.set_feature_engine(feature_engine)
    setup_engine.set_feature_engine(feature_engine)
    setup_engine.set_market_state_engine(market_state_engine)

    await feature_engine.initialize()
    await market_state_engine.initialize()
    await setup_engine.initialize()

    await feature_engine.start()
    await market_state_engine.start()
    await setup_engine.start()

    storage = StorageEngine(settings, bus)

    # Collect per-bar MarketState snapshots so the dashboard can show the live
    # regime at any replay position (not just the end-of-day state).
    bar_market_states: dict[str, Any] = {}

    async def _collect_market_state(event: "AnyEvent") -> None:
        if isinstance(event, MarketStateEvent):
            bar_market_states[event.timestamp.isoformat()] = event.state_data

    bus.subscribe(EventType.MARKET_STATE, _collect_market_state)

    # ── Warmup: prior trading days ──────────────────────────────────────────
    warmup_dates: list[date] = []
    prev = d
    for _ in range(_WARMUP_DAYS):
        prev = calendar.prev_trading_day(prev)
        warmup_dates.insert(0, prev)

    for warmup_date in warmup_dates:
        warmup_bars = await storage.load_bar_events(
            symbol.upper(), BarTimeframe.M1, warmup_date, warmup_date
        )
        if warmup_bars:
            warmup_bars.sort(key=lambda e: e.timestamp)
            logger.warning(
                "[backfill] Warmup: replaying %d bars for %s %s",
                len(warmup_bars), symbol, warmup_date,
            )
            for event in warmup_bars:
                await bus.publish(event)
                await asyncio.sleep(0)
            await bus.flush()
        else:
            logger.warning(
                "[backfill] Warmup: no bars in Parquet for %s %s (skipping)",
                symbol, warmup_date,
            )

    # ── Target date replay ──────────────────────────────────────────────────
    bar_events = await storage.load_bar_events(symbol.upper(), BarTimeframe.M1, d, d)
    bar_events.sort(key=lambda e: e.timestamp)
    logger.warning("[backfill] Replaying %d target bars for %s %s", len(bar_events), symbol, d)

    for event in bar_events:
        await bus.publish(event)
        await asyncio.sleep(0)

    # Wait for all queued events to finish processing before reading state.
    await bus.flush()

    await setup_engine.stop()
    await market_state_engine.stop()
    await feature_engine.stop()
    await bus.stop()

    # For CME futures the last ~2 h of bars (18:00–19:59 ET) roll the session
    # key forward to the *next* trading day.  Capture the target-date context
    # before that roll happens by falling back to prev_session_setup_context.
    context = setup_engine.session_setup_context(symbol.upper())
    if context is not None and context.session_date != d.isoformat():
        context = setup_engine.prev_session_setup_context(symbol.upper())

    # Attach per-bar MarketState snapshots. Only keep bars that belong to the
    # target date to avoid leaking warmup-day states into the context.
    if context is not None and bar_market_states:
        date_prefix = d.isoformat()
        target_states = {
            ts: state
            for ts, state in bar_market_states.items()
            if ts.startswith(date_prefix)
        }
        context = context.model_copy(update={"bar_market_states": target_states})

    return context
