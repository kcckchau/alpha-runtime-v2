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
from alpha.config.settings import AlphaSettings
from alpha.core.clock import Clock
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import EventType, HealthStatus, ORBState, SessionPhase
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

    def reset_session(self) -> None:
        self.bars_since_open = 0
        self.cumulative_volume = 0
        self.cumulative_vwap_num = _ZERO
        self.orb_high = None
        self.orb_low = None
        self.orb_established = False

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
        self._snapshots_emitted: int = 0

    @property
    def name(self) -> str:
        return "FeatureEngine"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        for sym in self._registry.active():
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
            self._states[ticker] = SymbolFeatureState(
                ticker, self._settings.runtime.orb_minutes
            )
        return self._states[ticker]

    def _update_state(self, state: SymbolFeatureState, bar: BarEvent) -> None:
        phase = self._calendar.session_phase(bar.timestamp)
        bar_date = bar.timestamp.date()

        if state.session_date is None or state.session_date.date() != bar_date:
            if phase == SessionPhase.OPENING_RANGE:
                state.reset_session()
                state.session_date = bar.timestamp

        state.bars.append(bar)

        if phase not in {SessionPhase.PRE_MARKET, SessionPhase.AFTER_HOURS, SessionPhase.CLOSED}:
            state.bars_since_open += 1
            state.cumulative_volume += bar.volume
            typical = (bar.high + bar.low + bar.close) / 3
            state.cumulative_vwap_num += typical * bar.volume

            orb_end = self._calendar.opening_range_end(bar_date, state.orb_minutes)
            if not state.orb_established and bar.timestamp < orb_end:
                if state.orb_high is None or bar.high > state.orb_high:
                    state.orb_high = bar.high
                if state.orb_low is None or bar.low < state.orb_low:
                    state.orb_low = bar.low
            elif not state.orb_established and bar.timestamp >= orb_end:
                state.orb_established = True

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
            orb_high=state.orb_high,
            orb_low=state.orb_low,
            orb_range=(state.orb_high - state.orb_low)
            if state.orb_high and state.orb_low else None,
            orb_state=orb_state,
            session_phase=self._calendar.session_phase(bar.timestamp),
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
