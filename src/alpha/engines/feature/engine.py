"""
Engine 4 — Feature Engine

Responsibilities:
  - Subscribe to BarEvent and QuoteEvent from the EventBus
  - Maintain per-symbol rolling state (VWAP, EMAs, ATR, ORB, session context)
  - Compute all indicators and microstructure features per bar
  - Produce a BarSnapshot available to downstream engines

Input:  BarEvent, QuoteEvent (from EventBus)
Output: BarSnapshot (stored in-engine; downstream engines call get_snapshot())

No trading logic here — only pure feature computation.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime
from decimal import Decimal
from typing import Deque

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.clock import Clock
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import AssetClass, EventType, HealthStatus, ORBState, SessionPhase
from alpha.models.events import AnyEvent, BarEvent, QuoteEvent
from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


class SymbolFeatureState:
    """Per-symbol rolling indicator state."""

    def __init__(self, ticker: str, orb_minutes: int) -> None:
        self.ticker = ticker
        self.orb_minutes = orb_minutes
        self.bars: Deque[BarEvent] = deque(maxlen=200)
        self.bars_since_open: int = 0
        self.cumulative_volume: int = 0
        self.cumulative_vwap_num: Decimal = _ZERO
        self.orb_high: Decimal | None = None
        self.orb_low: Decimal | None = None
        self.orb_established: bool = False
        self.session_date: datetime | None = None
        self.ema_9: Decimal | None = None
        self.ema_20: Decimal | None = None
        self.ema_50: Decimal | None = None
        self.latest_bid: Decimal | None = None
        self.latest_ask: Decimal | None = None
        self.atr_buffer: Deque[Decimal] = deque(maxlen=14)
        self.prev_close: Decimal | None = None
        self.volume_buffer: Deque[int] = deque(maxlen=20)  # completed bar volumes for RVOL
        self.relative_volume: float | None = None

        # ── Setup detection state ─────────────────────────────────────────────
        self.bars_above_vwap: int = 0
        self.bars_below_vwap: int = 0
        self.prev_above_vwap: bool | None = None
        self.prev_vwap_deviation_pct: float | None = None
        self.vwap_deviation_shrinking: bool = False
        self.vwap_cross_up: bool = False
        self.vwap_cross_down: bool = False
        self.intraday_high: Decimal | None = None
        self.intraday_low: Decimal | None = None
        self.is_new_hod: bool = False
        self.is_new_lod: bool = False
        self.is_higher_high: bool = False
        self.is_lower_low: bool = False
        self.prev_bar_high: Decimal | None = None
        self.prev_bar_low: Decimal | None = None
        self.session_key: str | None = None

    def reset_session(self) -> None:
        self.bars_since_open = 0
        self.cumulative_volume = 0
        self.cumulative_vwap_num = _ZERO
        self.orb_high = None
        self.orb_low = None
        self.orb_established = False
        self.bars_above_vwap = 0
        self.bars_below_vwap = 0
        self.prev_above_vwap = None
        self.prev_vwap_deviation_pct = None
        self.vwap_deviation_shrinking = False
        self.vwap_cross_up = False
        self.vwap_cross_down = False
        self.intraday_high = None
        self.intraday_low = None
        self.is_new_hod = False
        self.is_new_lod = False
        self.is_higher_high = False
        self.is_lower_low = False
        self.prev_bar_high = None
        self.prev_bar_low = None
        self.session_key = None

    @property
    def vwap(self) -> Decimal:
        if self.cumulative_volume == 0:
            return _ZERO
        return self.cumulative_vwap_num / Decimal(self.cumulative_volume)


class FeatureEngine(BaseEngine):
    """
    Computes indicators and microstructure features per bar.

    Downstream engines (MarketState, Setup, Risk) call `get_snapshot(symbol)`
    to obtain the latest BarSnapshot after subscribing to BarEvent.
    """

    def __init__(
        self,
        settings: AlphaSettings,
        event_bus: EventBus,
        registry: SymbolRegistry,
        calendar: SessionCalendar,
        clock: Clock,
    ) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._registry = registry
        self._calendar = calendar
        self._clock = clock
        self._states: dict[str, SymbolFeatureState] = {}
        self._snapshots: dict[str, BarSnapshot] = {}
        self._symbol_calendars: dict[str, SessionCalendar] = {}
        self._snapshots_emitted: int = 0

    @property
    def name(self) -> str:
        return "FeatureEngine"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        for sym in self._registry.active():
            self._symbol_calendars[sym.ticker] = calendar_for_symbol(sym)
            self._states[sym.ticker] = SymbolFeatureState(
                sym.ticker, self._settings.runtime.orb_minutes
            )

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.BAR, self._handle_bar)
        self._event_bus.subscribe(EventType.QUOTE, self._handle_quote)

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        return EngineHealth(
            HealthStatus.HEALTHY,
            self.name,
            {"snapshots_emitted": self._snapshots_emitted, "symbols": len(self._states)},
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def get_snapshot(self, symbol: str) -> BarSnapshot | None:
        return self._snapshots.get(symbol)

    def record_trade(self, symbol: str, price: float, size: int) -> None:
        """Sync tick handler: update intraday high/low from raw trade ticks.

        Called directly from the live adapter on every trade tick — no asyncio.
        Keeps intraday_high / intraday_low accurate between bar completions.
        """
        state = self._states.get(symbol)
        if state is None:
            return
        p = Decimal(str(price))
        if state.intraday_high is None or p > state.intraday_high:
            state.intraday_high = p
        if state.intraday_low is None or p < state.intraday_low:
            state.intraday_low = p

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return
        state = self._get_or_create(event.symbol)
        self._update_state(state, event)
        snapshot = self._build_snapshot(state, event)
        self._snapshots[event.symbol] = snapshot
        self._snapshots_emitted += 1

    async def _handle_quote(self, event: AnyEvent) -> None:
        if not isinstance(event, QuoteEvent):
            return
        state = self._get_or_create(event.symbol)
        state.latest_bid = event.bid_price
        state.latest_ask = event.ask_price

    # ── State ─────────────────────────────────────────────────────────────────

    def _get_or_create(self, ticker: str) -> SymbolFeatureState:
        if ticker not in self._states:
            self._symbol_calendars[ticker] = calendar_for_symbol(self._registry.get(ticker))
            self._states[ticker] = SymbolFeatureState(
                ticker, self._settings.runtime.orb_minutes
            )
        return self._states[ticker]

    def _update_state(self, state: SymbolFeatureState, bar: BarEvent) -> None:
        calendar = self._calendar_for_symbol(bar.symbol)
        phase = calendar.session_phase(bar.timestamp)
        session_key = calendar.session_key(bar.timestamp)
        session_date = calendar.session_date(bar.timestamp)

        if state.session_key != session_key:
            state.reset_session()
            state.session_key = session_key
            state.session_date = datetime.combine(
                session_date,
                datetime.min.time(),
                tzinfo=bar.timestamp.tzinfo,
            )

        state.bars.append(bar)

        is_rth = phase not in {SessionPhase.PRE_MARKET, SessionPhase.AFTER_HOURS, SessionPhase.CLOSED}
        # Futures trade ~23h/day — compute VWAP and setup flags for all non-CLOSED
        # phases so premarket detection works. Equities keep the RTH-only gate.
        sym = self._registry.get(bar.symbol)
        is_futures = sym is not None and sym.asset_class == AssetClass.FUTURE
        active_session = is_rth or (is_futures and phase != SessionPhase.CLOSED)

        if active_session:
            state.cumulative_volume += bar.volume
            typical = (bar.high + bar.low + bar.close) / 3
            state.cumulative_vwap_num += typical * bar.volume

            # ORB and bars_since_open are cash-session concepts — RTH only.
            if is_rth:
                state.bars_since_open += 1
                orb_end = calendar.opening_range_end(session_date, state.orb_minutes)
                if not state.orb_established and bar.timestamp < orb_end:
                    if state.orb_high is None or bar.high > state.orb_high:
                        state.orb_high = bar.high
                    if state.orb_low is None or bar.low < state.orb_low:
                        state.orb_low = bar.low
                elif not state.orb_established and bar.timestamp >= orb_end:
                    state.orb_established = True

            # ── Setup detection features ──────────────────────────────────────
            vwap = state.vwap
            if vwap > _ZERO:
                is_above = bar.close >= vwap
                dev_pct = float((bar.close - vwap) / vwap * 100)

                if is_above:
                    state.bars_above_vwap += 1
                    state.bars_below_vwap = 0
                else:
                    state.bars_below_vwap += 1
                    state.bars_above_vwap = 0

                state.vwap_cross_up = (
                    state.prev_above_vwap is not None
                    and not state.prev_above_vwap
                    and is_above
                )
                state.vwap_cross_down = (
                    state.prev_above_vwap is not None
                    and state.prev_above_vwap
                    and not is_above
                )
                state.prev_above_vwap = is_above

                state.vwap_deviation_shrinking = (
                    state.prev_vwap_deviation_pct is not None
                    and state.prev_vwap_deviation_pct > 0
                    and dev_pct < state.prev_vwap_deviation_pct
                    and dev_pct > 0
                )
                state.prev_vwap_deviation_pct = dev_pct
            else:
                state.vwap_cross_up = False
                state.vwap_cross_down = False
                state.vwap_deviation_shrinking = False

            state.is_new_hod = state.intraday_high is None or bar.high > state.intraday_high
            state.is_new_lod = state.intraday_low is None or bar.low < state.intraday_low
            if state.is_new_hod:
                state.intraday_high = bar.high
            if state.is_new_lod:
                state.intraday_low = bar.low

            state.is_higher_high = (
                state.prev_bar_high is not None and bar.high > state.prev_bar_high
            )
            state.is_lower_low = (
                state.prev_bar_low is not None and bar.low < state.prev_bar_low
            )
            state.prev_bar_high = bar.high
            state.prev_bar_low = bar.low
        else:
            state.vwap_cross_up = False
            state.vwap_cross_down = False
            state.vwap_deviation_shrinking = False
            state.is_new_hod = False
            state.is_new_lod = False
            state.is_higher_high = False
            state.is_lower_low = False

        state.ema_9 = self._ema(bar.close, state.ema_9, 9)
        state.ema_20 = self._ema(bar.close, state.ema_20, 20)
        state.ema_50 = self._ema(bar.close, state.ema_50, 50)

        if state.prev_close is not None:
            tr = max(
                bar.high - bar.low,
                abs(bar.high - state.prev_close),
                abs(bar.low - state.prev_close),
            )
            state.atr_buffer.append(tr)
        state.prev_close = bar.close

        # RVOL — compare this bar's volume to the average of the last 20 bars.
        if state.volume_buffer:
            avg_vol = sum(state.volume_buffer) / len(state.volume_buffer)
            state.relative_volume = float(bar.volume) / avg_vol if avg_vol > 0 else None
        else:
            state.relative_volume = None
        state.volume_buffer.append(bar.volume)

    def _build_snapshot(self, state: SymbolFeatureState, bar: BarEvent) -> BarSnapshot:
        from alpha.models.bar import Bar

        vwap = state.vwap
        vwap_dev = float((bar.close - vwap) / vwap * 100) if vwap else 0.0
        orb_state = self._orb_state(state, bar)
        atr = (
            Decimal(str(round(float(sum(state.atr_buffer) / len(state.atr_buffer)), 4)))
            if state.atr_buffer else None
        )
        spread = (
            state.latest_ask - state.latest_bid
            if state.latest_bid and state.latest_ask else None
        )
        mid = (
            (state.latest_bid + state.latest_ask) / 2
            if state.latest_bid and state.latest_ask else None
        )
        spread_pct = float(spread / mid * 100) if spread and mid else None

        bar_range = bar.high - bar.low
        bar_close_pos: float | None = (
            float((bar.close - bar.low) / bar_range) if bar_range > _ZERO else None
        )
        swept_below = bool(vwap > _ZERO and bar.low < vwap and bar.close >= vwap)
        swept_orl = bool(state.orb_low is not None and bar.low < state.orb_low)
        or_mid = (
            (state.orb_high + state.orb_low) / 2
            if state.orb_high and state.orb_low else None
        )

        b = Bar(
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            timestamp=bar.timestamp,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            vwap=bar.vwap,
            trade_count=bar.trade_count,
            source=bar.metadata.source,
        )
        return BarSnapshot(
            symbol=bar.symbol,
            timestamp=bar.timestamp,
            timeframe=bar.timeframe,
            bar=b,
            vwap=vwap,
            vwap_deviation_pct=vwap_dev,
            cumulative_volume=state.cumulative_volume,
            relative_volume=state.relative_volume,
            orb_high=state.orb_high,
            orb_low=state.orb_low,
            orb_range=(state.orb_high - state.orb_low)
            if state.orb_high and state.orb_low else None,
            orb_state=orb_state,
            session_phase=self._calendar_for_symbol(bar.symbol).session_phase(bar.timestamp),
            bars_since_open=state.bars_since_open,
            ema_9=state.ema_9,
            ema_20=state.ema_20,
            ema_50=state.ema_50,
            atr_14=atr,
            bid_price=state.latest_bid,
            ask_price=state.latest_ask,
            bid_ask_spread=spread,
            bid_ask_spread_pct=spread_pct,
            is_above_vwap=bar.close >= vwap,
            is_above_ema20=bar.close >= state.ema_20 if state.ema_20 else None,
            is_extended=abs(vwap_dev) > 2.0 if vwap else False,
            bars_above_vwap=state.bars_above_vwap,
            bars_below_vwap=state.bars_below_vwap,
            vwap_cross_up=state.vwap_cross_up,
            vwap_cross_down=state.vwap_cross_down,
            vwap_deviation_shrinking=state.vwap_deviation_shrinking,
            bar_close_position_pct=bar_close_pos,
            intraday_high=state.intraday_high,
            intraday_low=state.intraday_low,
            is_new_hod=state.is_new_hod,
            is_new_lod=state.is_new_lod,
            is_higher_high=state.is_higher_high,
            is_lower_low=state.is_lower_low,
            or_mid=or_mid,
            swept_below_vwap=swept_below,
            swept_orl=swept_orl,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _ema(price: Decimal, prev: Decimal | None, period: int) -> Decimal:
        if prev is None:
            return price
        k = Decimal(2) / Decimal(period + 1)
        return price * k + prev * (1 - k)

    @staticmethod
    def _orb_state(state: SymbolFeatureState, bar: BarEvent) -> ORBState:
        if not state.orb_established or state.orb_high is None or state.orb_low is None:
            return ORBState.NOT_SET
        if bar.close > state.orb_high:
            return ORBState.BREAKOUT_UP
        if bar.close < state.orb_low:
            return ORBState.BREAKOUT_DOWN
        return ORBState.INSIDE

    def _calendar_for_symbol(self, ticker: str) -> SessionCalendar:
        return self._symbol_calendars.get(ticker, self._calendar)
