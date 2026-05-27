"""
Engine 0 — Bootstrap Engine

Responsibilities:
  - Load configuration and validate runtime mode
  - Populate the symbol registry
  - Wire dependency graph (engines, adapters, event bus)
  - Select and initialize the session calendar
  - Start all engines in dependency order
  - Implement catch-up-then-live transition for LIVE mode

Startup sequence (LIVE / PAPER):
  1. initialize()  → load config, build engines
  2. start()       → historical catch-up, then hand off to live feed
  3. Running       → all engines processing live events

Startup sequence (HISTORICAL_BACKFILL):
  1. initialize()
  2. start()       → replay historical range, write to storage
  3. stop()        → clean shutdown

Startup sequence (REPLAY):
  1. initialize()
  2. start()       → feed historical bars through pipeline at replay speed
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from alpha.calendar.base import SessionCalendar
from alpha.calendar.nyse import NYSECalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.loader import get_settings
from alpha.config.settings import AlphaSettings
from alpha.core.clock import Clock, ReplayClock, WallClock
from alpha.core.engine import BaseEngine
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.instruments import resolve_symbol
from alpha.models.enums import AssetClass, BarTimeframe, EngineState, RuntimeMode
from alpha.models.symbol import Symbol
from alpha.runtime_status import write_snapshot
from alpha.timeframe_context import build_symbol_context

if TYPE_CHECKING:
    from alpha.engines.feature.engine import FeatureEngine
    from alpha.engines.historical.engine import HistoricalDataEngine
    from alpha.engines.live.engine import LiveIngestionEngine
    from alpha.engines.market_state.engine import MarketStateEngine
    from alpha.engines.order.engine import OrderEngine
    from alpha.engines.risk.engine import RiskEngine
    from alpha.engines.scoring.engine import ScoringEngine
    from alpha.engines.setup.engine import SetupEngine
    from alpha.engines.storage.engine import StorageEngine

logger = logging.getLogger(__name__)


class BootstrapEngine(BaseEngine):
    """
    Orchestrates the full runtime lifecycle.

    All engines are owned and managed by this class. External code
    interacts with the runtime through the BootstrapEngine and the
    shared EventBus.
    """

    def __init__(self, settings: AlphaSettings | None = None) -> None:
        super().__init__()
        self._settings = settings or get_settings()
        self._event_bus = EventBus()
        self._registry = SymbolRegistry()
        self._clock: Clock = WallClock()
        self._calendar: SessionCalendar = NYSECalendar()

        # Engine references — populated in _on_initialize
        self._storage: StorageEngine | None = None
        self._historical: HistoricalDataEngine | None = None
        self._live: LiveIngestionEngine | None = None
        self._feature: FeatureEngine | None = None
        self._market_state: MarketStateEngine | None = None
        self._setup: SetupEngine | None = None
        self._scoring: ScoringEngine | None = None
        self._risk: RiskEngine | None = None
        self._order: OrderEngine | None = None

        self._engines: list[BaseEngine] = []
        self._status_task: asyncio.Task[None] | None = None
        self._startup_context: dict[str, dict[str, Any]] = {}
        self._ibkr_conn: object | None = None  # IBKRConnection, kept for clean shutdown

    @property
    def name(self) -> str:
        return "BootstrapEngine"

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def registry(self) -> SymbolRegistry().__class__:  # type: ignore[valid-type]
        return self._registry

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def calendar(self) -> SessionCalendar:
        return self._calendar

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        logger.info(
            "Bootstrapping runtime | mode=%s | symbols=%s",
            self._settings.runtime.mode,
            self._settings.runtime.symbols,
        )

        self._configure_clock()
        self._populate_registry()
        await self._event_bus.start()
        self._wire_engines()

        for engine in self._engines:
            await engine.initialize()
        self._write_runtime_snapshot()

    async def _on_start(self) -> None:
        mode = self._settings.runtime.mode
        for engine in self._engines:
            if mode in {RuntimeMode.LIVE, RuntimeMode.PAPER} and engine is self._live:
                continue
            await engine.start()

        self._write_runtime_snapshot()
        self._status_task = asyncio.create_task(self._status_loop())

        if mode in {RuntimeMode.LIVE, RuntimeMode.PAPER}:
            if self._live is not None:
                await self._live.start()
                self._write_runtime_snapshot()
            await self._run_catchup()
            if self._storage is not None:
                await self._storage.flush()
            logger.info("Catch-up complete — live feed remains active")
        elif mode == RuntimeMode.HISTORICAL_BACKFILL:
            await self._run_backfill()
        elif mode == RuntimeMode.REPLAY:
            await self._run_replay()
        self._write_runtime_snapshot()

    async def _on_stop(self) -> None:
        if self._status_task and not self._status_task.done():
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
        for engine in reversed(self._engines):
            await engine.stop()
        # Disconnect IBKR after engines stop (subscriptions are already cancelled
        # by LiveIngestionEngine._on_stop). Doing this while the event loop is
        # still running prevents IB.__del__ from firing on a closed loop.
        if self._ibkr_conn is not None:
            from alpha.engines.ibkr.connection import IBKRConnection
            if isinstance(self._ibkr_conn, IBKRConnection):
                await self._ibkr_conn.disconnect()
        await self._event_bus.stop()
        self._write_runtime_snapshot()

    # ── Private ───────────────────────────────────────────────────────────────

    def _configure_clock(self) -> None:
        mode = self._settings.runtime.mode
        if mode == RuntimeMode.REPLAY:
            replay_cfg = self._settings.replay
            start = datetime(
                replay_cfg.start_date.year,
                replay_cfg.start_date.month,
                replay_cfg.start_date.day,
                9, 30,
                tzinfo=timezone.utc,
            ) if replay_cfg.start_date else datetime.now(timezone.utc)
            self._clock = ReplayClock(start_time=start, speed=replay_cfg.speed)
            logger.info("Using ReplayClock speed=%.1f", replay_cfg.speed)
        else:
            self._clock = WallClock()

    def _populate_registry(self) -> None:
        for ticker in self._settings.runtime.symbols:
            sym = resolve_symbol(ticker)
            self._registry.register(sym)
        logger.info("Registry loaded: %d symbols", len(self._registry))

    def _wire_engines(self) -> None:
        """Instantiate all engines and inject shared dependencies."""
        from alpha.engines.feature.engine import FeatureEngine
        from alpha.engines.historical.engine import HistoricalDataEngine
        from alpha.engines.live.engine import LiveIngestionEngine
        from alpha.engines.market_state.engine import MarketStateEngine
        from alpha.engines.order.engine import OrderEngine
        from alpha.engines.risk.engine import RiskEngine
        from alpha.engines.scoring.engine import ScoringEngine
        from alpha.engines.setup.engine import SetupEngine
        from alpha.engines.storage.engine import StorageEngine

        self._storage = StorageEngine(self._settings, self._event_bus)
        self._historical = HistoricalDataEngine(
            self._settings, self._event_bus, self._registry, self._calendar
        )
        self._live = LiveIngestionEngine(
            self._settings, self._event_bus, self._registry
        )
        self._feature = FeatureEngine(
            self._settings, self._event_bus, self._registry, self._calendar, self._clock
        )
        self._market_state = MarketStateEngine(self._settings, self._event_bus, self._registry)
        self._market_state.set_feature_engine(self._feature)
        self._setup = SetupEngine(self._settings, self._event_bus, self._registry)
        self._setup.set_feature_engine(self._feature)
        self._setup.set_market_state_engine(self._market_state)
        self._scoring = ScoringEngine(self._settings, self._event_bus)
        self._scoring.set_setup_engine(self._setup)
        self._risk = RiskEngine(self._settings, self._event_bus)
        self._risk.set_setup_engine(self._setup)
        self._order = OrderEngine(self._settings, self._event_bus, self._registry)
        self._risk.set_order_engine(self._order)

        self._wire_ibkr()

        self._engines = [
            self._storage,
            self._historical,
            self._live,
            self._feature,
            self._market_state,
            self._setup,
            self._scoring,
            self._risk,
            self._order,
        ]

    def _wire_ibkr(self) -> None:
        """Register IBKR adapters if IBKR is configured as the data/order source."""
        from alpha.models.enums import DataSourceId

        hist_src = self._settings.historical.primary_source
        live_src = self._settings.live.primary_source

        if DataSourceId.INTERACTIVE_BROKERS not in {hist_src, live_src}:
            return

        from alpha.engines.ibkr.connection import IBKRConnection
        from alpha.engines.historical.sources.ibkr import IBKRHistoricalDataSource
        from alpha.engines.live.adapters.ibkr import IBKRLiveFeedAdapter
        from alpha.engines.order.adapters.ibkr import IBKRBrokerAdapter

        conn = IBKRConnection(self._settings.ibkr)
        self._ibkr_conn = conn

        if hist_src == DataSourceId.INTERACTIVE_BROKERS:
            self._historical.register_source(
                IBKRHistoricalDataSource(conn, self._registry, self._settings.ibkr)
            )

        if live_src == DataSourceId.INTERACTIVE_BROKERS:
            self._live.register_adapter(
                IBKRLiveFeedAdapter(conn, self._settings.ibkr, self._registry.active())
            )

        self._order.register_adapter(
            IBKRBrokerAdapter(conn, self._settings.ibkr, self._registry)
        )
        logger.info("IBKR adapters registered (paper=%s)", self._settings.ibkr.is_paper)

    async def _run_catchup(self) -> None:
        """Load recent history before connecting the live feed."""
        hist = self._settings.historical
        logger.info(
            "Running catch-up | symbols=%s | 1m_bars=%d | 5m_bars=%d | 1h_bars=%d | 1d_bars=%d | vwap=%s",
            self._settings.runtime.symbols,
            hist.minute1_warmup_bars,
            hist.minute5_warmup_bars,
            hist.hourly_warmup_bars,
            hist.daily_warmup_bars,
            hist.vwap_session,
        )
        if self._historical is None or self._storage is None:
            return

        end = datetime.now(timezone.utc)
        vwap_start = self._vwap_session_start(end)

        # Per-timeframe warmup windows sized to the deepest indicator on each timeframe.
        # M1/M5 windows are also extended to cover the full current VWAP session so that
        # session-VWAP can be computed accurately regardless of when the system starts.
        # Calendar-day multipliers account for overnight and weekend gaps (≈5/7 trading ratio).
        m1_start = min(end - timedelta(days=max(3, hist.minute1_warmup_bars // 390 + 1)), vwap_start)
        m5_start = min(end - timedelta(days=max(7, hist.minute5_warmup_bars // 78 + 2)), vwap_start)
        h1_start = end - timedelta(days=int(hist.hourly_warmup_bars * 0.22) + 30)
        d1_start = end - timedelta(days=int(hist.daily_warmup_bars * 1.5))

        for symbol in self._settings.runtime.symbols:
            logger.info("Catch-up starting for %s", symbol)
            symbol_def = self._registry.get(symbol)
            symbol_d1_start = d1_start
            if symbol_def.asset_class == AssetClass.FUTURE:
                # Avoid multi-year expired-contract daily backfills on futures during startup.
                # The web UI doesn't chart 1d, and a short recent window is enough for
                # previous-day levels while keeping startup responsive.
                symbol_d1_start = max(d1_start, end - timedelta(days=45))
            minute_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.M1,
                start=m1_start,
                end=end,
                emit=True,
                persist_direct=False,
            )
            minute5_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.M5,
                start=m5_start,
                end=end,
                emit=True,
                persist_direct=False,
            )
            hourly_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.H1,
                start=h1_start,
                end=end,
                emit=False,
                persist_direct=True,
            )
            daily_bars = await self._load_or_fetch_bars(
                symbol=symbol,
                timeframe=BarTimeframe.D1,
                start=symbol_d1_start,
                end=end,
                emit=False,
                persist_direct=True,
            )

            self._startup_context[symbol] = build_symbol_context(
                symbol=symbol,
                minute_bars=minute_bars,
                hourly_bars=hourly_bars,
                daily_bars=daily_bars,
                calendar=calendar_for_symbol(self._registry.get(symbol)),
            )
            logger.info(
                "Catch-up complete for %s | 1m=%d 5m=%d 1h=%d 1d=%d",
                symbol,
                len(minute_bars),
                len(minute5_bars),
                len(hourly_bars),
                len(daily_bars),
            )

    def _vwap_session_start(self, now: datetime) -> datetime:
        """Return the start of the current (or most recent completed) VWAP session in UTC.

        Ensures M1/M5 catch-up windows always reach back to the session open so that
        session-VWAP can be computed accurately from the first bar.
        """
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/New_York")
        now_et = now.astimezone(tz)
        today = now_et.date()

        if self._settings.historical.vwap_session == "extended":
            hour, minute = 4, 0
        else:  # "rth" (default): regular session opens at 09:30 ET
            hour, minute = 9, 30

        session_open_et = now_et.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # If we haven't reached today's open yet, fall back to the previous trading day
        if now_et < session_open_et:
            prev_day = self._calendar.prev_trading_day(today)
            session_open_et = session_open_et.replace(
                year=prev_day.year, month=prev_day.month, day=prev_day.day
            )

        return session_open_et.astimezone(timezone.utc)

    async def _load_or_fetch_bars(
        self,
        symbol: str,
        timeframe: BarTimeframe,
        start: datetime,
        end: datetime,
        *,
        emit: bool,
        persist_direct: bool,
    ) -> list[Any]:
        if self._historical is None or self._storage is None:
            return []

        stored = await self._storage.load_bar_events(symbol, timeframe, start.date(), end.date())
        stored = [bar for bar in stored if start <= bar.timestamp <= end]
        missing_ranges = self._missing_ranges(symbol, stored, start, end, timeframe)
        logger.info(
            "Catch-up %s %s | stored=%d | missing_ranges=%d | window=%s -> %s",
            symbol,
            timeframe,
            len(stored),
            len(missing_ranges),
            start.isoformat(),
            end.isoformat(),
        )

        fetched: list[Any] = []
        for gap_start, gap_end in missing_ranges:
            logger.info(
                "Fetching gap for %s %s | %s -> %s",
                symbol,
                timeframe,
                gap_start.isoformat(),
                gap_end.isoformat(),
            )
            bars = await self._historical.fetch_bars(
                symbol=symbol,
                timeframe=timeframe,
                start=gap_start,
                end=gap_end,
                emit=emit,
            )
            fetched.extend(bars)
            if persist_direct:
                for bar in bars:
                    await self._storage.save_bar(bar)

        merged_by_ts = {bar.timestamp: bar for bar in stored}
        for bar in fetched:
            merged_by_ts[bar.timestamp] = bar

        merged = sorted(merged_by_ts.values(), key=lambda bar: bar.timestamp)
        logger.info(
            "Catch-up %s %s complete | fetched=%d | merged_total=%d",
            symbol,
            timeframe,
            len(fetched),
            len(merged),
        )
        return merged

    @staticmethod
    def _timeframe_delta(timeframe: BarTimeframe) -> timedelta:
        mapping = {
            BarTimeframe.M1: timedelta(minutes=1),
            BarTimeframe.M5: timedelta(minutes=5),
            BarTimeframe.H1: timedelta(hours=1),
            BarTimeframe.D1: timedelta(days=1),
        }
        return mapping[timeframe]

    def _missing_ranges(
        self,
        symbol: str,
        stored_bars: list[Any],
        start: datetime,
        end: datetime,
        timeframe: BarTimeframe,
    ) -> list[tuple[datetime, datetime]]:
        step = BootstrapEngine._timeframe_delta(timeframe)
        calendar = calendar_for_symbol(self._registry.get(symbol))
        relevant = sorted(
            [bar for bar in stored_bars if start <= bar.timestamp <= end],
            key=lambda bar: bar.timestamp,
        )
        if not relevant:
            return [(start, end)]

        missing: list[tuple[datetime, datetime]] = []

        first = relevant[0]
        if self._should_fill_head_gap(calendar, timeframe, start, first.timestamp):
            missing.append((start, first.timestamp - step))

        for left, right in zip(relevant, relevant[1:]):
            expected_next = left.timestamp + step
            if self._should_fill_internal_gap(calendar, timeframe, left.timestamp, right.timestamp, expected_next):
                missing.append((expected_next, right.timestamp - step))

        last = relevant[-1]
        if self._should_fill_tail_gap(calendar, timeframe, last.timestamp, end):
            missing.append((last.timestamp + step, end))

        return [
            (gap_start, gap_end)
            for gap_start, gap_end in missing
            if gap_start <= gap_end
        ]

    @staticmethod
    def _should_fill_head_gap(
        calendar: SessionCalendar,
        timeframe: BarTimeframe,
        start: datetime,
        first: datetime,
    ) -> bool:
        # Always fill if data starts after the requested window — applies to all timeframes.
        # H1/D1 previously used a same-session check here which prevented filling when the
        # head gap spanned session boundaries (e.g. fresh run after a weekend).
        return first > start

    @staticmethod
    def _should_fill_internal_gap(
        calendar: SessionCalendar,
        timeframe: BarTimeframe,
        left: datetime,
        right: datetime,
        expected_next: datetime,
    ) -> bool:
        if right <= expected_next:
            return False
        if timeframe in {BarTimeframe.H1, BarTimeframe.D1}:
            return calendar.session_key(left) == calendar.session_key(right)
        return True

    @staticmethod
    def _should_fill_tail_gap(
        calendar: SessionCalendar,
        timeframe: BarTimeframe,
        last: datetime,
        end: datetime,
    ) -> bool:
        # Always fill if stored data ends before the requested window — applies to all timeframes.
        # H1/D1 previously used a same-session check here which caused stale caches to never
        # catch up across session boundaries (e.g. running after a multi-day gap).
        return last < end

    async def _status_loop(self) -> None:
        ticks = 0
        while True:
            self._write_runtime_snapshot()
            if ticks % 30 == 0:
                self._log_runtime_summary()
            ticks += 1
            await asyncio.sleep(1)

    def _write_runtime_snapshot(self) -> None:
        try:
            write_snapshot(self._settings, self._build_runtime_snapshot())
        except Exception:
            logger.exception("Failed to write runtime snapshot")

    def _build_runtime_snapshot(self) -> dict[str, Any]:
        return {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "runtime_state": self.state,
            "mode": self._settings.runtime.mode,
            "symbols": self._settings.runtime.symbols,
            "engines": [self._serialize_engine(engine) for engine in self._engines],
            "quotes": self._serialize_quotes(),
            "bars": self._serialize_bars(),
            "contexts": self._startup_context,
            "market_states": self._serialize_market_states(),
            "setups": self._serialize_setups(),
            "setup_contexts": self._serialize_setup_contexts(),
            "orders": self._serialize_orders(),
        }

    def _serialize_engine(self, engine: BaseEngine) -> dict[str, Any]:
        details = self._engine_details(engine)
        if engine.state == EngineState.RUNNING:
            health_status = "healthy"
        elif engine.state == EngineState.ERROR:
            health_status = "unhealthy"
        else:
            health_status = "degraded"
        return {
            "name": engine.name,
            "state": str(engine.state),
            "health": health_status,
            "details": details,
        }

    def _engine_details(self, engine: BaseEngine) -> dict[str, Any]:
        if engine is self._storage:
            return {
                "queue_depth": self._storage._write_queue.qsize(),  # type: ignore[union-attr]
                "writes_total": self._storage._writes_total,  # type: ignore[union-attr]
            }
        if engine is self._historical:
            return {}
        if engine is self._live:
            return {
                "bars_received": self._live._bars_received,  # type: ignore[union-attr]
                "quotes_received": self._live._quotes_received,  # type: ignore[union-attr]
                "trades_received": self._live._trades_received,  # type: ignore[union-attr]
            }
        if engine is self._feature:
            return {
                "snapshots_emitted": self._feature._snapshots_emitted,  # type: ignore[union-attr]
            }
        if engine is self._market_state:
            return {
                "classifications_total": self._market_state._classifications_total,  # type: ignore[union-attr]
            }
        if engine is self._setup:
            return {
                "active_setups": sum(len(v) for v in self._setup._active.values()),  # type: ignore[union-attr]
                "detected_total": self._setup._setups_detected,  # type: ignore[union-attr]
                "triggered_total": self._setup._setups_triggered,  # type: ignore[union-attr]
            }
        if engine is self._scoring:
            return {}
        if engine is self._risk:
            return {}
        if engine is self._order:
            return {
                "orders_submitted": self._order._orders_submitted,  # type: ignore[union-attr]
                "orders_filled": self._order._orders_filled,  # type: ignore[union-attr]
                "open_orders": len(self._order.get_open_orders()),  # type: ignore[union-attr]
            }
        return {}

    def _serialize_quotes(self) -> dict[str, Any]:
        if self._live is None:
            return {}
        quotes = {}
        for symbol, event in self._live.latest_quotes().items():
            quotes[symbol] = event.model_dump(mode="json")
        return quotes

    def _serialize_bars(self) -> dict[str, Any]:
        if self._live is None:
            return {}
        completed = self._live.latest_bars()
        partial = self._live.latest_partial_bars()
        bars = {}
        for symbol in set(completed) | set(partial):
            # Prefer the in-progress partial bar — it has the current price.
            # Fall back to the last completed bar if no partial bar exists yet.
            event = partial.get(symbol) or completed.get(symbol)
            if event:
                bars[symbol] = event.model_dump(mode="json")
        return bars

    def _serialize_market_states(self) -> dict[str, Any]:
        if self._market_state is None:
            return {}
        states = {}
        for symbol in self._settings.runtime.symbols:
            state = self._market_state.get_state(symbol)
            if state is not None:
                states[symbol] = state.model_dump(mode="json")
        return states

    def _serialize_setups(self) -> list[dict[str, Any]]:
        if self._setup is None:
            return []
        setups: list[dict[str, Any]] = []
        for symbol in self._settings.runtime.symbols:
            for setup in self._setup.active_setups(symbol):
                setups.append(setup.model_dump(mode="json"))
        return setups

    def _serialize_orders(self) -> list[dict[str, Any]]:
        if self._order is None:
            return []
        return [order.model_dump(mode="json") for order in self._order.get_open_orders()]

    def _serialize_setup_contexts(self) -> dict[str, Any]:
        if self._setup is None:
            return {}
        contexts: dict[str, Any] = {}
        for symbol in self._settings.runtime.symbols:
            context = self._setup.session_setup_context(symbol)
            if context is not None:
                contexts[symbol] = context.model_dump(mode="json")
        return contexts

    def _log_runtime_summary(self) -> None:
        if self._live is None:
            return
        quote_chunks: list[str] = []
        for symbol in self._settings.runtime.symbols:
            quote = self._live.latest_quotes().get(symbol)
            bar = self._live.latest_bars().get(symbol)
            if quote is None and bar is None:
                continue
            close = f"{bar.close}" if bar is not None else "-"
            bid = f"{quote.bid_price}" if quote is not None else "-"
            ask = f"{quote.ask_price}" if quote is not None else "-"
            quote_chunks.append(f"{symbol} c={close} bid={bid} ask={ask}")
        if quote_chunks:
            logger.info("Runtime summary | %s", " | ".join(quote_chunks))

    async def _run_backfill(self) -> None:
        """Full historical backfill for configured symbols and timeframes.

        Uses replay.start_date / replay.end_date when set (via --start / --end CLI flags).
        Falls back to warmup-driven windows sized to the deepest indicator per timeframe.
        Idempotent: only fetches gaps not already in local Parquet cache.
        """
        if self._historical is None or self._storage is None:
            logger.error("Cannot run backfill: historical or storage engine not initialized")
            return

        hist = self._settings.historical
        replay = self._settings.replay

        now = datetime.now(timezone.utc)
        end = (
            datetime(
                replay.end_date.year, replay.end_date.month, replay.end_date.day,
                23, 59, 59, tzinfo=timezone.utc,
            )
            if replay.end_date
            else now
        )

        # Warmup-driven start dates (calendar-day adjusted: ≈1.4× trading days for weekends)
        default_starts: dict[BarTimeframe, datetime] = {
            BarTimeframe.M1: end - timedelta(days=max(3, hist.minute1_warmup_bars // 390 + 1)),
            BarTimeframe.M5: end - timedelta(days=max(7, hist.minute5_warmup_bars // 78 + 2)),
            BarTimeframe.H1: end - timedelta(days=int(hist.hourly_warmup_bars * 0.22) + 30),
            BarTimeframe.D1: end - timedelta(days=int(hist.daily_warmup_bars * 1.5)),
        }

        # An explicit --start extends the window further back on all timeframes
        if replay.start_date:
            explicit = datetime(
                replay.start_date.year, replay.start_date.month, replay.start_date.day,
                tzinfo=timezone.utc,
            )
            starts: dict[BarTimeframe, datetime] = {
                tf: min(default, explicit) for tf, default in default_starts.items()
            }
        else:
            starts = default_starts

        logger.info(
            "Running full historical backfill | symbols=%s | end=%s"
            " | 1m_start=%s | 5m_start=%s | 1h_start=%s | 1d_start=%s",
            self._settings.runtime.symbols,
            end.date().isoformat(),
            starts[BarTimeframe.M1].date().isoformat(),
            starts[BarTimeframe.M5].date().isoformat(),
            starts[BarTimeframe.H1].date().isoformat(),
            starts[BarTimeframe.D1].date().isoformat(),
        )

        for symbol in self._settings.runtime.symbols:
            logger.info("Backfilling %s", symbol)
            for timeframe in (BarTimeframe.M1, BarTimeframe.M5, BarTimeframe.H1, BarTimeframe.D1):
                await self._load_or_fetch_bars(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=starts[timeframe],
                    end=end,
                    emit=False,
                    persist_direct=True,
                )

        await self._storage.flush()
        logger.info("Historical backfill complete")

    async def _run_replay(self) -> None:
        """Replay historical data through the full pipeline."""
        logger.info("Running replay")
        # TODO: drive HistoricalDataEngine with ReplayClock pacing
