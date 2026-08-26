"""
BarFlowAggregator
=================
Buffers nanosecond-resolution trades and mbp-1 quotes within each 1-minute bar
window, then seals a BarFlowContext when the 1m BarEvent arrives.

Emits BarBundleEvent (BAR + sealed flow) on every 1m bar close.
All downstream sequential-pipeline engines subscribe to BAR_BUNDLE instead of BAR.

Flow:
    TradeEvent  ──┐
    QuoteEvent  ──┤──► BarFlowAggregator ──► BarBundleEvent ──► pipeline
    BarEvent(S1)──┤                           (1m only)
    BarEvent(M1)──┘ (seal trigger)

Design notes:
  - One aggregator instance per symbol.
  - The 1m BarEvent arrival is the seal trigger. We do NOT rely on a timer because
    replay and live have different timing characteristics.
  - 1s bars (S1) are used to compute split-bar delta (sweep phase vs recovery phase).
  - Trades/quotes outside the current window are discarded with a warning.
  - When trade or quote data is unavailable (e.g. historical replay without full-signals
    cache), the aggregator still emits BarBundleEvent with flow=None so the pipeline
    degrades gracefully.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from alpha.models.enums import BarTimeframe, EventType, TakerSide
from alpha.models.events import BarBundleEvent, BarEvent, QuoteEvent, TradeEvent
from alpha.models.flow import (
    AbsorptionSignal,
    BarFlowContext,
    SplitBarDelta,
)

logger = logging.getLogger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────

# Minimum recovery/sweep ratio to NOT be classified as a V-reversal
_V_REVERSAL_MAX_RATIO = 0.30
# Maximum seconds for recovery phase to still be called a V-reversal
_V_REVERSAL_MAX_SECONDS = 5
# Minimum sweep_volume as multiple of avg_bar_volume for "genuine flush" classification
_GENUINE_FLUSH_VOLUME_MULTIPLIER = 1.2
# Minimum recovery/sweep ratio for genuine reversal
_GENUINE_REVERSAL_MIN_RATIO = 0.50
# Large trade threshold (contracts); configurable per symbol via constructor
_DEFAULT_LARGE_TRADE_THRESHOLD = 10


# ── Window state ──────────────────────────────────────────────────────────────


class _Window:
    """Mutable accumulator for one 1m bar window."""

    __slots__ = (
        "open_ts",
        "close_ts",
        # trade tape
        "buy_vol",
        "sell_vol",
        "unknown_vol",
        "large_buy_count",
        "large_sell_count",
        "large_trade_threshold",
        "trade_count",
        # 1s bar split tracking (keyed by second-bar open_ts)
        "s1_deltas",       # list of (ts, delta, volume) per 1s bar
        "bar_low",         # running bar low (updated from S1 bars or M1 bar)
        "bar_low_ts",      # timestamp of the 1s bar that set the bar low
        # quote imbalance — incremental TWAP (O(1) per update, O(1) at seal)
        "quote_count",
        "_twap_last_ts",       # datetime | None — timestamp of previous quote
        "_twap_last_bid_sz",   # int — bid_size of previous quote
        "_twap_last_ask_sz",   # int — ask_size of previous quote
        "_twap_weighted_bid",  # float — running time-weighted bid accumulator
        "_twap_weighted_ask",  # float — running time-weighted ask accumulator
        "_twap_total_time",    # float — total seconds elapsed between quotes
    )

    def __init__(self, open_ts: datetime, close_ts: datetime, large_trade_threshold: int):
        self.open_ts = open_ts
        self.close_ts = close_ts
        self.buy_vol = 0
        self.sell_vol = 0
        self.unknown_vol = 0
        self.large_buy_count = 0
        self.large_sell_count = 0
        self.large_trade_threshold = large_trade_threshold
        self.trade_count = 0
        self.s1_deltas: list[tuple[datetime, int, int]] = []   # (ts, delta, volume)
        self.bar_low: Decimal | None = None
        self.bar_low_ts: datetime | None = None
        self.quote_count = 0
        self._twap_last_ts: datetime | None = None
        self._twap_last_bid_sz: int = 0
        self._twap_last_ask_sz: int = 0
        self._twap_weighted_bid: float = 0.0
        self._twap_weighted_ask: float = 0.0
        self._twap_total_time: float = 0.0

    def add_trade(self, event: TradeEvent) -> None:
        size = event.size
        side = event.taker_side
        if side == TakerSide.BUY:
            self.buy_vol += size
        elif side == TakerSide.SELL:
            self.sell_vol += size
        else:
            self.unknown_vol += size
        if size >= self.large_trade_threshold:
            if side == TakerSide.BUY:
                self.large_buy_count += 1
            elif side == TakerSide.SELL:
                self.large_sell_count += 1
        self.trade_count += 1

    def add_quote(self, event: QuoteEvent) -> None:
        if self._twap_last_ts is not None:
            dt = (event.timestamp - self._twap_last_ts).total_seconds()
            if dt > 0:
                self._twap_weighted_bid += self._twap_last_bid_sz * dt
                self._twap_weighted_ask += self._twap_last_ask_sz * dt
                self._twap_total_time += dt
        self._twap_last_ts = event.timestamp
        self._twap_last_bid_sz = event.bid_size
        self._twap_last_ask_sz = event.ask_size
        self.quote_count += 1

    def add_s1_bar(self, event: BarEvent) -> None:
        """Accumulate 1s bar delta and track running bar low."""
        # Approximate delta from 1s bar: we don't have tick-level side for OHLCV bars,
        # so we use: if close > open → positive delta (buy pressure), else negative.
        # This is a rough proxy; true delta requires trade tape.
        delta = event.volume if event.close >= event.open else -event.volume
        self.s1_deltas.append((event.timestamp, delta, event.volume))
        if self.bar_low is None or event.low < self.bar_low:
            self.bar_low = event.low
            self.bar_low_ts = event.timestamp

    def total_volume(self) -> int:
        return self.buy_vol + self.sell_vol + self.unknown_vol

    def delta(self) -> int:
        return self.buy_vol - self.sell_vol


# ── Aggregator ────────────────────────────────────────────────────────────────


class BarFlowAggregator:
    """
    One instance per symbol. Attach to the EventBus by calling on_trade(),
    on_quote(), and on_bar() from EventBus subscriptions.

    Usage (in engine setup):
        agg = BarFlowAggregator(symbol="MNQ-09", event_bus=bus)
        agg.attach()

    The aggregator subscribes to TRADE, QUOTE, and BAR internally, and publishes
    BAR_BUNDLE events that the sequential pipeline consumes.
    """

    def __init__(
        self,
        symbol: str,
        event_bus,                          # alpha.eventbus.EventBus — avoid circular import
        large_trade_threshold: int = _DEFAULT_LARGE_TRADE_THRESHOLD,
    ):
        self._symbol = symbol
        self._bus = event_bus
        self._large_trade_threshold = large_trade_threshold
        self._window: _Window | None = None
        self._avg_bar_volume: float = 0.0   # rolling mean over last 20 bars
        self._vol_history: list[int] = []
        self._vol_history_size = 20

    def attach(self) -> None:
        """Subscribe to the EventBus. Call once during engine startup."""
        self._bus.subscribe(EventType.TRADE, self._on_trade, symbol=self._symbol)
        self._bus.subscribe(EventType.QUOTE, self._on_quote, symbol=self._symbol, drop_if_full=True)
        self._bus.subscribe(EventType.BAR, self._on_bar, symbol=self._symbol)
        logger.info("BarFlowAggregator attached for %s", self._symbol)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _window_for(self, ts: datetime) -> _Window | None:
        """Return current window if ts falls within it, else None."""
        if self._window is None:
            return None
        if self._window.open_ts <= ts < self._window.close_ts:
            return self._window
        return None

    def _open_window(self, open_ts: datetime, timeframe_seconds: int = 60) -> _Window:
        close_ts = open_ts + timedelta(seconds=timeframe_seconds)
        self._window = _Window(open_ts, close_ts, self._large_trade_threshold)
        return self._window

    def _update_vol_history(self, volume: int) -> None:
        self._vol_history.append(volume)
        if len(self._vol_history) > self._vol_history_size:
            self._vol_history.pop(0)
        self._avg_bar_volume = sum(self._vol_history) / len(self._vol_history)

    # ── EventBus handlers ─────────────────────────────────────────────────────

    async def _on_trade(self, event: TradeEvent) -> None:
        w = self._window_for(event.timestamp)
        if w is None:
            return  # pre-window or between bars — discard
        w.add_trade(event)

    async def _on_quote(self, event: QuoteEvent) -> None:
        w = self._window_for(event.timestamp)
        if w is None:
            return
        w.add_quote(event)

    async def _on_bar(self, event: BarEvent) -> None:
        if event.symbol != self._symbol:
            return
        # Ignore BarEvents re-published by BarPipeline as backward-compat fallback.
        # Those are derived from a bundle we already sealed — processing them again
        # would open a duplicate window and emit a second BarBundleEvent.
        if event.is_pipeline_fallback:
            return

        if event.timeframe == BarTimeframe.S1:
            # Feed into current window for split-bar delta tracking
            w = self._window_for(event.timestamp)
            if w is not None:
                w.add_s1_bar(event)

        elif event.timeframe == BarTimeframe.M1:
            # Seal current window, open next window, emit BarBundleEvent
            await self._seal_and_emit(event)

    async def _seal_and_emit(self, bar: BarEvent) -> None:
        """Seal the current window and emit a BarBundleEvent."""
        w = self._window

        # Build flow context (or None if no data at all)
        if w is not None:
            flow = self._build_flow_context(w, bar)
        else:
            # No window open — happens for the very first bar or after a gap.
            # Emit with flow=None so the pipeline can still process the bar.
            flow = None

        bundle = BarBundleEvent(
            symbol=bar.symbol,
            timestamp=bar.timestamp,
            timeframe=bar.timeframe,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            vwap=bar.vwap,
            trade_count=bar.trade_count,
            flow=flow,
            metadata=bar.metadata,
        )

        # Update volume rolling average using the bar's reported volume
        self._update_vol_history(bar.volume)

        # Open next window aligned to bar close (= next bar's open)
        next_open = bar.timestamp + timedelta(minutes=1)
        self._open_window(next_open, timeframe_seconds=60)

        await self._bus.publish(bundle)

    def _build_flow_context(self, w: _Window, bar: BarEvent) -> BarFlowContext:
        total_vol = w.total_volume()
        delta = w.delta()
        delta_pct = (delta / total_vol * 100.0) if total_vol > 0 else None
        trade_velocity = (w.trade_count / 60.0) if w.trade_count > 0 else None
        avg_trade_size = (total_vol / w.trade_count) if w.trade_count > 0 else None

        # ── Quote imbalance (time-weighted) ────────────────────────────────
        twap_bid, twap_ask, bid_ask_imbalance = self._compute_quote_imbalance(w)

        # ── Split-bar delta ────────────────────────────────────────────────
        # Use bar.low as authoritative bar low when S1 bars weren't available
        bar_low = w.bar_low if w.bar_low is not None else bar.low
        split = self._compute_split_delta(w, bar, bar_low)

        # ── Absorption ─────────────────────────────────────────────────────
        absorption = self._compute_absorption(w, bar, bar_low)

        # ── Classification ─────────────────────────────────────────────────
        is_v_reversal = False
        is_genuine = False

        if split is not None and split.recovery_ratio is not None:
            is_v_reversal = (
                split.recovery_ratio < _V_REVERSAL_MAX_RATIO
                and split.recovery_seconds <= _V_REVERSAL_MAX_SECONDS
                and not absorption.detected
            )
            is_genuine = (
                w.buy_vol + w.sell_vol > self._avg_bar_volume * _GENUINE_FLUSH_VOLUME_MULTIPLIER
                and absorption.detected
                and split.recovery_ratio > _GENUINE_REVERSAL_MIN_RATIO
                and w.large_buy_count >= 2
            )

        return BarFlowContext(
            total_volume=total_vol,
            buy_volume=w.buy_vol,
            sell_volume=w.sell_vol,
            unknown_volume=w.unknown_vol,
            delta=delta,
            delta_pct=delta_pct,
            large_trade_count=w.large_buy_count + w.large_sell_count,
            large_buy_count=w.large_buy_count,
            large_sell_count=w.large_sell_count,
            trade_count=w.trade_count,
            trade_velocity=trade_velocity,
            avg_trade_size=avg_trade_size,
            twap_bid_size=twap_bid,
            twap_ask_size=twap_ask,
            bid_ask_imbalance=bid_ask_imbalance,
            quote_count=w.quote_count,
            split=split,
            absorption=absorption,
            is_v_reversal=is_v_reversal,
            is_genuine_sweep_reversal=is_genuine,
            large_trade_threshold=self._large_trade_threshold,
            has_trade_data=w.trade_count > 0,
            has_quote_data=w.quote_count > 0,
            has_1s_bars=len(w.s1_deltas) > 0,
        )

    def _compute_quote_imbalance(
        self, w: _Window
    ) -> tuple[float | None, float | None, float | None]:
        if w.quote_count < 2 or w._twap_total_time <= 0:
            return None, None, None
        twap_bid = w._twap_weighted_bid / w._twap_total_time
        twap_ask = w._twap_weighted_ask / w._twap_total_time
        total = twap_bid + twap_ask
        imbalance = (twap_bid / total) if total > 0 else None
        return twap_bid, twap_ask, imbalance

    def _compute_split_delta(
        self, w: _Window, bar: BarEvent, bar_low: Decimal
    ) -> SplitBarDelta | None:
        """
        Divide 1s bars into sweep phase (open → bar low timestamp) and
        recovery phase (bar low timestamp → bar close).
        Only meaningful when the bar made a significant new low.
        """
        if not w.s1_deltas or w.bar_low_ts is None:
            return None

        # Only compute split when bar made a notable move to low (wicked down)
        bar_range = bar.high - bar.low
        low_position = (bar.low - bar.open) / bar_range if bar_range > 0 else Decimal(0)
        if low_position > Decimal("-0.2"):
            # Bar didn't wick significantly — no meaningful sweep phase
            return None

        sweep_delta = 0
        sweep_volume = 0
        sweep_seconds = 0
        recovery_delta = 0
        recovery_volume = 0
        recovery_seconds = 0

        for ts, delta, volume in w.s1_deltas:
            if ts <= w.bar_low_ts:
                sweep_delta += delta
                sweep_volume += volume
                sweep_seconds += 1
            else:
                recovery_delta += delta
                recovery_volume += volume
                recovery_seconds += 1

        sweep_abs = abs(sweep_delta)
        recovery_ratio: float | None = None
        recovery_speed: float | None = None

        if sweep_abs > 0:
            recovery_ratio = abs(recovery_delta) / sweep_abs
        if recovery_seconds > 0:
            recovery_speed = recovery_delta / recovery_seconds

        return SplitBarDelta(
            sweep_low_ts=w.bar_low_ts,
            sweep_volume=sweep_volume,
            sweep_delta=sweep_delta,
            recovery_volume=recovery_volume,
            recovery_delta=recovery_delta,
            sweep_seconds=sweep_seconds,
            recovery_seconds=recovery_seconds,
            recovery_ratio=recovery_ratio,
            recovery_speed=recovery_speed,
        )

    def _compute_absorption(
        self, w: _Window, bar: BarEvent, bar_low: Decimal
    ) -> AbsorptionSignal:
        """
        Detect absorption at the bar low.
        Requires trade tape data; returns detected=False when unavailable.

        Heuristic: large sell cluster at the low, followed by price holding or rising.
        Uses 1s bar sequence to count how many seconds price stayed at/above the low
        after the sell cluster.
        """
        if not w.s1_deltas or w.bar_low_ts is None or w.sell_vol == 0:
            return AbsorptionSignal()

        # Gather 1s bars near the bar low (within 2 ticks — approximate with price proximity)
        tick_size = Decimal("0.25")   # MNQ tick size; TODO: make symbol-configurable
        low_zone = bar_low + tick_size * 2

        sell_at_low = 0
        buy_response = 0
        ticks_held = 0
        in_response_phase = False

        for ts, delta, volume in w.s1_deltas:
            if ts <= w.bar_low_ts:
                if delta < 0:
                    sell_at_low += abs(delta)
            else:
                in_response_phase = True
                if delta > 0:
                    buy_response += delta
                ticks_held += 1

        if sell_at_low == 0:
            return AbsorptionSignal()

        buy_ratio = buy_response / sell_at_low if sell_at_low > 0 else 0.0
        # Confidence: blend of buy_ratio and how long price held
        hold_score = min(ticks_held / 10.0, 1.0)   # saturates at 10 seconds
        confidence = (buy_ratio * 0.6 + hold_score * 0.4)
        detected = confidence >= 0.5 and sell_at_low >= self._large_trade_threshold

        return AbsorptionSignal(
            detected=detected,
            price_level=bar_low,
            sell_volume_at_low=sell_at_low,
            buy_volume_response=buy_response,
            ticks_held=ticks_held,
            confidence=min(confidence, 1.0),
        )
