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
from datetime import datetime, timezone
from decimal import Decimal
from typing import Deque

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.clock import Clock
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import AssetClass, BarTimeframe, EventType, HealthStatus, ORBState, SessionPhase
from alpha.models.events import AnyEvent, BarBundleEvent, BarEvent, QuoteEvent
from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


def _percentile(data: list[Decimal], pct: float) -> Decimal | None:
    """Linear-interpolation percentile over a list of Decimals.  No minimum size."""
    if not data:
        return None
    s = sorted(data)
    idx = pct * (len(s) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    if lo == hi:
        return s[lo]
    frac = Decimal(str(round(idx - lo, 6)))
    return s[lo] + (s[hi] - s[lo]) * frac


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
        self.ema_21: Decimal | None = None
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
        self.bars_below_vwap_at_cross: int = 0   # bars below VWAP before the last cross-up
        self.bars_above_vwap_at_cross: int = 0   # bars above VWAP before the last cross-down
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
        self.is_lower_high: bool = False
        self.prev_bar_high: Decimal | None = None
        self.prev_bar_low: Decimal | None = None
        self.prev_ema_9: Decimal | None = None
        self.prev_ema_20: Decimal | None = None
        self.prev_vwap: Decimal | None = None
        self.bars_since_last_lower_low: int = 999  # 999 = no lower low yet this session
        self.session_key: str | None = None

        # VWAP interaction memory
        self.vwap_touch_count: int = 0
        self.last_vwap_outcome: str | None = None
        self.bars_since_last_vwap_touch: int = 0
        self._vwap_last_cross_direction: str | None = None  # "up" or "down"
        self._vwap_bars_since_cross: int = 0
        self.session_sweep_low: Decimal | None = None
        self.session_reject_high: Decimal | None = None
        # ATR-30
        self.atr_30_buffer: Deque[Decimal] = deque(maxlen=30)
        self.atr_30: Decimal | None = None
        # EMA9 slope acceleration
        self.prev_ema_9_slope: float | None = None
        self.ema_9_slope_accel: float | None = None
        # RTH candle-range distribution (reset each RTH session)
        self.rth_ranges: list[Decimal] = []
        self.rth_median_1m_range: Decimal | None = None
        self.rth_p75_1m_range: Decimal | None = None
        self.rth_p90_1m_range: Decimal | None = None
        # RTH-specific low — resets at RTH open, independent of Globex low
        self.rth_intraday_low: Decimal | None = None
        self.is_new_rth_lod: bool = False

    def reset_session(self) -> None:
        self.bars_since_open = 0
        self.cumulative_volume = 0
        self.cumulative_vwap_num = _ZERO
        self.orb_high = None
        self.orb_low = None
        self.orb_established = False
        self.bars_above_vwap = 0
        self.bars_below_vwap = 0
        self.bars_below_vwap_at_cross = 0
        self.bars_above_vwap_at_cross = 0
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
        self.is_lower_high = False
        self.prev_bar_high = None
        self.prev_bar_low = None
        self.prev_ema_9 = None
        self.prev_ema_20 = None
        self.prev_vwap = None
        self.bars_since_last_lower_low = 999
        self.session_key = None
        self.vwap_touch_count = 0
        self.last_vwap_outcome = None
        self.bars_since_last_vwap_touch = 0
        self._vwap_last_cross_direction = None
        self._vwap_bars_since_cross = 0
        self.session_sweep_low = None
        self.session_reject_high = None
        self.atr_30_buffer.clear()
        self.atr_30 = None
        self.prev_ema_9_slope = None
        self.ema_9_slope_accel = None
        self.rth_ranges = []
        self.rth_median_1m_range = None
        self.rth_p75_1m_range = None
        self.rth_p90_1m_range = None
        self.rth_intraday_low = None
        self.is_new_rth_lod = False

    @property
    def vwap(self) -> Decimal:
        if self.cumulative_volume == 0:
            return _ZERO
        return self.cumulative_vwap_num / Decimal(self.cumulative_volume)


class _HTFEMAState:
    """Lightweight EMA/SMA state for a single higher timeframe (M5, H1, D1).

    Holds only what's needed to compute EMAs bar-by-bar. Values are carried
    forward to the M1 BarSnapshot so downstream engines always have the latest
    HTF EMA without needing to subscribe to HTF bar events themselves.

    EMA50 and SMA200 are optional — only computed when the state is created
    with track_ema50=True / track_sma200=True (used for H1 and D1).
    """

    def __init__(
        self,
        track_ema10: bool = False,
        track_ema20: bool = False,
        track_ema50: bool = False,
        track_sma200: bool = False,
    ) -> None:
        self.ema_9: Decimal | None = None
        self.ema_21: Decimal | None = None
        self.ema_10: Decimal | None = None          # None unless track_ema10
        self.ema_20: Decimal | None = None          # None unless track_ema20
        self.ema_50: Decimal | None = None          # None unless track_ema50
        self._track_ema10 = track_ema10
        self._track_ema20 = track_ema20
        self._track_ema50 = track_ema50
        self._sma200_buf: deque[Decimal] | None = (
            deque(maxlen=200) if track_sma200 else None
        )
        self.sma_200: Decimal | None = None         # None until 200 bars seen


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
        # Higher-timeframe EMA state — keyed by symbol, updated on HTF bar events,
        # carried forward into every M1 BarSnapshot.
        self._m5_ema: dict[str, _HTFEMAState] = {}
        self._h1_ema: dict[str, _HTFEMAState] = {}
        self._d1_ema: dict[str, _HTFEMAState] = {}

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
        self._event_bus.subscribe(EventType.QUOTE, self._handle_quote, drop_if_full=True)
        self._pipeline_mode: bool = False  # set True by BarPipeline to skip M1 in _handle_bar

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

    def get_level_memory(self, ticker: str) -> "LevelMemorySnapshot | None":
        """Return VWAP interaction memory for a symbol. Used by ThesisEngine."""
        from alpha.models.thesis import LevelMemorySnapshot
        state = self._states.get(ticker)
        if state is None:
            return None
        snap = self._snapshots.get(ticker)
        return LevelMemorySnapshot(
            symbol=ticker,
            timestamp=snap.timestamp if snap else datetime.now(timezone.utc),
            vwap_touch_count=state.vwap_touch_count,
            last_vwap_outcome=state.last_vwap_outcome,
            bars_since_last_vwap_touch=state.bars_since_last_vwap_touch,
            current_side="above" if state.prev_above_vwap else ("below" if state.prev_above_vwap is not None else None),
            session_sweep_low=state.session_sweep_low,
            session_reject_high=state.session_reject_high,
        )

    def record_trade(self, symbol: str, price: float, size: int) -> None:
        """Sync tick handler: update intraday high/low from raw trade ticks.

        Called directly from the live adapter on every trade tick — no asyncio.
        Keeps intraday_high / intraday_low accurate between bar completions.
        """
        state = self._states.get(symbol)
        if state is None:
            return
        p = Decimal(str(price))
        # Guard against off-market prints (block trades, spread-implied fills)
        # that sit far outside the current bid/ask — otherwise a single bad tick
        # can falsely mark a new intraday high/low and trigger false breakout setups.
        if state.latest_bid is not None and state.latest_ask is not None:
            anchor = (state.latest_bid + state.latest_ask) / 2
            spread = state.latest_ask - state.latest_bid
            band = max(anchor * Decimal("0.001"), spread * Decimal("30"))
            if band > 0 and abs(p - anchor) > band:
                return
        if state.intraday_high is None or p > state.intraday_high:
            state.intraday_high = p
        if state.intraday_low is None or p < state.intraday_low:
            state.intraday_low = p

    # ── Public pipeline API ───────────────────────────────────────────────────

    def process_bar(self, bundle: BarBundleEvent) -> BarSnapshot | None:
        """
        Process a sealed 1m BarBundle and return the updated BarSnapshot.

        Called by BarPipeline as Stage 1 of the sequential pipeline.
        Guarantees the snapshot is current before any downstream engine runs.
        Returns None for non-M1 bundles (should not happen but safe to guard).
        """
        if bundle.timeframe != BarTimeframe.M1:
            return None
        bar = bundle.to_bar_event()
        state = self._get_or_create(bar.symbol)
        self._update_state(state, bar)
        snapshot = self._build_snapshot(state, bar)
        if bundle.flow is not None:
            snapshot = snapshot.model_copy(update={"flow": bundle.flow})
        self._snapshots[bar.symbol] = snapshot
        self._snapshots_emitted += 1
        return snapshot

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return
        if event.timeframe == BarTimeframe.D1:
            self._update_htf_ema(self._d1_ema, event, track_ema10=True, track_ema20=True)
            return
        if event.timeframe == BarTimeframe.M5:
            self._update_htf_ema(self._m5_ema, event)
            return
        if event.timeframe == BarTimeframe.H1:
            self._update_htf_ema(self._h1_ema, event, track_ema50=True, track_sma200=True)
            return
        if event.timeframe != BarTimeframe.M1:
            return  # S1 and other bars not processed here
        if self._pipeline_mode:
            return  # BarPipeline owns M1 processing; avoid double-update
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
            state.prev_vwap = state.vwap if state.cumulative_volume > 0 else None
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

                # RTH-specific intraday low — resets at RTH open each session.
                # is_new_rth_lod is True whenever this bar makes a new RTH low,
                # even if the all-session low (Globex) is lower. Overnight lows
                # are excluded, so opening panics and ORL breaks are correctly
                # flagged for intraday pattern detectors.
                state.is_new_rth_lod = (
                    state.rth_intraday_low is None or bar.low < state.rth_intraday_low
                )
                if state.is_new_rth_lod:
                    state.rth_intraday_low = bar.low

                # RTH candle-range distribution — used to normalize sweep depth.
                # Compute percentiles from PRIOR bars before appending the current
                # bar's range, so classifiers on the current bar are not polluted
                # by the candle they are evaluating.
                state.rth_median_1m_range = _percentile(state.rth_ranges, 0.50)
                state.rth_p75_1m_range = _percentile(state.rth_ranges, 0.75)
                state.rth_p90_1m_range = _percentile(state.rth_ranges, 0.90)
                state.rth_ranges.append(bar.high - bar.low)

            # ── Setup detection features ──────────────────────────────────────
            vwap = state.vwap
            if vwap > _ZERO:
                is_above = bar.close >= vwap
                dev_pct = float((bar.close - vwap) / vwap * 100)

                # Save counts before resetting — needed for cross-up/down context.
                prev_bars_below = state.bars_below_vwap
                prev_bars_above = state.bars_above_vwap

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
                if state.vwap_cross_up:
                    state.bars_below_vwap_at_cross = prev_bars_below
                if state.vwap_cross_down:
                    state.bars_above_vwap_at_cross = prev_bars_above
                state.prev_above_vwap = is_above

                state.vwap_deviation_shrinking = (
                    state.prev_vwap_deviation_pct is not None
                    and state.prev_vwap_deviation_pct > 0
                    and dev_pct < state.prev_vwap_deviation_pct
                    and dev_pct > 0
                )
                state.prev_vwap_deviation_pct = dev_pct

                # ── VWAP interaction tracking ─────────────────────────────────────────
                # Note: prev_above_vwap was already set to is_above above; capture the
                # prior-bar side before it was updated for the swept check below.
                swept = vwap > _ZERO and bar.low < vwap <= bar.close
                if state.vwap_cross_up:
                    state.vwap_touch_count += 1
                    state.last_vwap_outcome = "reclaimed"
                    state.bars_since_last_vwap_touch = 0
                    state._vwap_last_cross_direction = "up"
                    state._vwap_bars_since_cross = 0
                elif state.vwap_cross_down:
                    state.vwap_touch_count += 1
                    if (
                        state._vwap_last_cross_direction == "up"
                        and state._vwap_bars_since_cross <= 3
                    ):
                        state.last_vwap_outcome = "rejected"
                    else:
                        state.last_vwap_outcome = "broken"
                    state.bars_since_last_vwap_touch = 0
                    state._vwap_last_cross_direction = "down"
                    state._vwap_bars_since_cross = 0
                elif swept and is_above:
                    # Low dipped below VWAP but close recovered — approached from above
                    state.vwap_touch_count += 1
                    state.last_vwap_outcome = "swept"
                    state.bars_since_last_vwap_touch = 0
                else:
                    if state.last_vwap_outcome is not None:
                        state.bars_since_last_vwap_touch += 1
                    if state._vwap_last_cross_direction is not None:
                        state._vwap_bars_since_cross += 1

                # Track session extremes for thesis context
                if vwap > _ZERO and bar.low < vwap:
                    if state.session_sweep_low is None or bar.low < state.session_sweep_low:
                        state.session_sweep_low = bar.low
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
            state.is_lower_high = (
                state.prev_bar_high is not None and bar.high < state.prev_bar_high
            )
            state.prev_bar_high = bar.high
            state.prev_bar_low = bar.low
            if state.is_lower_low:
                state.bars_since_last_lower_low = 0
            elif state.bars_since_last_lower_low < 999:
                state.bars_since_last_lower_low += 1
        else:
            state.vwap_cross_up = False
            state.vwap_cross_down = False
            state.vwap_deviation_shrinking = False
            state.is_new_hod = False
            state.is_new_lod = False
            state.is_new_rth_lod = False
            state.is_higher_high = False
            state.is_lower_low = False
            state.is_lower_high = False

        prev_ema_9 = state.ema_9
        prev_ema_20 = state.ema_20
        state.ema_9 = self._ema(bar.close, state.ema_9, 9)
        state.ema_20 = self._ema(bar.close, state.ema_20, 20)
        state.ema_21 = self._ema(bar.close, state.ema_21, 21)
        state.ema_50 = self._ema(bar.close, state.ema_50, 50)
        state.prev_ema_9 = prev_ema_9
        state.prev_ema_20 = prev_ema_20

        if state.prev_close is not None:
            tr = max(
                bar.high - bar.low,
                abs(bar.high - state.prev_close),
                abs(bar.low - state.prev_close),
            )
            state.atr_buffer.append(tr)
            # ATR-30 (alongside existing ATR-14)
            state.atr_30_buffer.append(tr)
            if len(state.atr_30_buffer) >= 2:
                state.atr_30 = sum(state.atr_30_buffer) / len(state.atr_30_buffer)
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

        ema_9_slope: float | None = (
            float((state.ema_9 - state.prev_ema_9) / state.prev_ema_9 * 100)
            if state.ema_9 is not None and state.prev_ema_9 is not None and state.prev_ema_9 > _ZERO
            else None
        )
        ema_9_slope_accel: float | None = None
        if state.prev_ema_9_slope is not None and ema_9_slope is not None:
            ema_9_slope_accel = ema_9_slope - state.prev_ema_9_slope
        state.prev_ema_9_slope = ema_9_slope
        state.ema_9_slope_accel = ema_9_slope_accel
        ema_20_slope: float | None = (
            float((state.ema_20 - state.prev_ema_20) / state.prev_ema_20 * 100)
            if state.ema_20 is not None and state.prev_ema_20 is not None and state.prev_ema_20 > _ZERO
            else None
        )
        vwap_slope: float | None = (
            float((state.vwap - state.prev_vwap) / state.prev_vwap * 100)
            if state.prev_vwap is not None and state.prev_vwap > _ZERO
            else None
        )
        ema_9_slope_direction = self._slope_direction(ema_9_slope, 0.005)
        ema_20_slope_direction = self._slope_direction(ema_20_slope, 0.005)
        vwap_slope_direction = self._slope_direction(vwap_slope, 0.002)
        recent_lower_low = state.bars_since_last_lower_low <= 10

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
            ema_21=state.ema_21,
            ema_50=state.ema_50,
            ema9_5m=self._m5_ema[bar.symbol].ema_9 if bar.symbol in self._m5_ema else None,
            ema21_5m=self._m5_ema[bar.symbol].ema_21 if bar.symbol in self._m5_ema else None,
            ema9_1h=self._h1_ema[bar.symbol].ema_9 if bar.symbol in self._h1_ema else None,
            ema21_1h=self._h1_ema[bar.symbol].ema_21 if bar.symbol in self._h1_ema else None,
            ema50_1h=self._h1_ema[bar.symbol].ema_50 if bar.symbol in self._h1_ema else None,
            sma200_1h=self._h1_ema[bar.symbol].sma_200 if bar.symbol in self._h1_ema else None,
            ema10_1d=self._d1_ema[bar.symbol].ema_10 if bar.symbol in self._d1_ema else None,
            ema20_1d=self._d1_ema[bar.symbol].ema_20 if bar.symbol in self._d1_ema else None,
            ema_9_slope=ema_9_slope,
            ema_9_slope_direction=ema_9_slope_direction,
            ema_20_slope=ema_20_slope,
            ema_20_slope_direction=ema_20_slope_direction,
            vwap_slope=vwap_slope,
            vwap_slope_direction=vwap_slope_direction,
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
            vwap_cross_up_after_bars=state.bars_below_vwap_at_cross,
            vwap_cross_down=state.vwap_cross_down,
            vwap_cross_down_after_bars=state.bars_above_vwap_at_cross,
            vwap_deviation_shrinking=state.vwap_deviation_shrinking,
            bar_close_position_pct=bar_close_pos,
            intraday_high=state.intraday_high,
            intraday_low=state.intraday_low,
            is_new_hod=state.is_new_hod,
            is_new_lod=state.is_new_lod,
            is_new_rth_lod=state.is_new_rth_lod,
            is_higher_high=state.is_higher_high,
            is_lower_low=state.is_lower_low,
            is_lower_high=state.is_lower_high,
            recent_lower_low=recent_lower_low,
            or_mid=or_mid,
            swept_below_vwap=swept_below,
            swept_orl=swept_orl,
            vwap_touch_count=state.vwap_touch_count,
            last_vwap_outcome=state.last_vwap_outcome,
            bars_since_last_vwap_touch=state.bars_since_last_vwap_touch,
            atr_30=state.atr_30,
            ema_9_slope_accel=state.ema_9_slope_accel,
            rth_median_1m_range=state.rth_median_1m_range,
            rth_p75_1m_range=state.rth_p75_1m_range,
            rth_p90_1m_range=state.rth_p90_1m_range,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _update_htf_ema(
        self,
        store: dict[str, _HTFEMAState],
        bar: BarEvent,
        track_ema10: bool = False,
        track_ema20: bool = False,
        track_ema50: bool = False,
        track_sma200: bool = False,
    ) -> None:
        """Update EMA9/EMA21 (and optionally EMA10/EMA20/EMA50/SMA200) for a HTF bar."""
        sym = bar.symbol
        if sym not in store:
            store[sym] = _HTFEMAState(
                track_ema10=track_ema10,
                track_ema20=track_ema20,
                track_ema50=track_ema50,
                track_sma200=track_sma200,
            )
        s = store[sym]
        s.ema_9 = self._ema(bar.close, s.ema_9, 9)
        s.ema_21 = self._ema(bar.close, s.ema_21, 21)
        if s._track_ema10:
            s.ema_10 = self._ema(bar.close, s.ema_10, 10)
        if s._track_ema20:
            s.ema_20 = self._ema(bar.close, s.ema_20, 20)
        if s._track_ema50:
            s.ema_50 = self._ema(bar.close, s.ema_50, 50)
        if s._sma200_buf is not None:
            s._sma200_buf.append(bar.close)
            if len(s._sma200_buf) == 200:
                s.sma_200 = sum(s._sma200_buf) / Decimal(200)

    @staticmethod
    def _ema(price: Decimal, prev: Decimal | None, period: int) -> Decimal:
        if prev is None:
            return price
        k = Decimal(2) / Decimal(period + 1)
        return price * k + prev * (1 - k)

    @staticmethod
    def _slope_direction(slope: float | None, flat_threshold: float) -> str | None:
        """Classify a % rate-of-change slope as "up", "flat", or "down".
        flat_threshold is the %-ROC boundary; values within ±threshold = "flat".
        EMA slopes use 0.005% (≈1pt on MNQ at 19000); VWAP uses 0.002%.
        """
        if slope is None:
            return None
        if slope > flat_threshold:
            return "up"
        if slope < -flat_threshold:
            return "down"
        return "flat"

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
