"""
IntrabarFlowEngine
==================
Runs in parallel with BarFlowAggregator on every 1-second bar.
Its job is early detection — it does NOT wait for the 1m bar to close.

Outputs:
  - Running delta accumulation (updated every 1s)
  - Sweep detection: when a new bar low is set, start tracking recovery phase
  - Early absorption signal: if large sell + price hold detected before bar close
  - Pre-alert: published as a lightweight dict (not a full event) for the thesis engine

Design:
  - Very lightweight — no model instantiation per tick; accumulates primitive scalars.
  - Read-only output: publishes nothing to EventBus; exposes get_intrabar_state()
    for engines that want early context before the bundle is sealed.
  - Optional: can publish a SystemEvent("intrabar_absorption_alert") when absorption
    is detected > 45 seconds before bar close, giving the thesis engine a head start.

Usage:
    engine = IntrabarFlowEngine(symbol="MNQ-09", event_bus=bus)
    engine.attach()
    # Any engine can call engine.get_state() to get current intrabar context
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from alpha.models.enums import BarTimeframe, EventType, TakerSide
from alpha.models.events import BarEvent, QuoteEvent, TradeEvent

logger = logging.getLogger(__name__)


@dataclass
class IntrabarState:
    """
    Snapshot of the running intrabar flow state at any moment within a 1m bar.
    Returned by IntrabarFlowEngine.get_state().
    """

    symbol: str
    window_open_ts: datetime | None = None

    # Running trade tape (reset per bar)
    buy_volume: int = 0
    sell_volume: int = 0
    trade_count: int = 0
    delta: int = 0              # buy_vol - sell_vol so far this bar

    # Running bar extreme
    running_high: Decimal | None = None
    running_low: Decimal | None = None

    # Sweep tracking (set when running_low drops below prior bar low)
    sweep_detected: bool = False
    sweep_low: Decimal | None = None
    sweep_low_ts: datetime | None = None
    sweep_phase_delta: int = 0       # delta accumulated before new low was set
    recovery_phase_delta: int = 0    # delta accumulated after new low (recovery)
    recovery_seconds: int = 0        # 1s bars elapsed since new low

    # Early absorption signal
    early_absorption: bool = False
    absorption_confidence: float = 0.0

    # Timing (seconds into the 1m bar)
    seconds_elapsed: int = 0
    seconds_remaining: int = 60

    def is_v_reversal_risk(self) -> bool:
        """
        True when mid-bar indicators suggest a snap-back rather than genuine reversal.
        Only meaningful when sweep_detected=True.
        """
        if not self.sweep_detected:
            return False
        if self.recovery_seconds < 3:
            return False   # too early to tell
        sweep_abs = abs(self.sweep_phase_delta)
        if sweep_abs == 0:
            return False
        ratio = abs(self.recovery_phase_delta) / sweep_abs
        return ratio < 0.2 and self.recovery_seconds <= 4 and not self.early_absorption

    def is_absorption_forming(self) -> bool:
        return self.early_absorption and self.absorption_confidence >= 0.4


class IntrabarFlowEngine:
    """
    Subscribes to BAR(S1), TRADE, and QUOTE events.
    Resets its state on each new 1m bar window (triggered by S1 bar timestamps).
    """

    def __init__(
        self,
        symbol: str,
        event_bus,
        prior_bar_low: Decimal | None = None,     # seed from BootstrapEngine if available
        large_trade_threshold: int = 10,
    ):
        self._symbol = symbol
        self._bus = event_bus
        self._large_trade_threshold = large_trade_threshold
        self._state = IntrabarState(symbol=symbol)
        self._prior_bar_low: Decimal | None = prior_bar_low
        self._current_window_open: datetime | None = None

    def attach(self) -> None:
        self._bus.subscribe(EventType.BAR, self._on_bar, symbol=self._symbol)
        self._bus.subscribe(EventType.TRADE, self._on_trade, symbol=self._symbol)
        logger.info("IntrabarFlowEngine attached for %s", self._symbol)

    def get_state(self) -> IntrabarState:
        """Return a copy of the current intrabar state (safe to read from other engines)."""
        import copy
        return copy.copy(self._state)

    def set_prior_bar_low(self, low: Decimal) -> None:
        """Called by FeatureEngine or BootstrapEngine after each sealed bar."""
        self._prior_bar_low = low

    # ── EventBus handlers ─────────────────────────────────────────────────────

    async def _on_bar(self, event: BarEvent) -> None:
        if event.symbol != self._symbol:
            return

        if event.timeframe == BarTimeframe.S1:
            self._process_s1_bar(event)
        elif event.timeframe == BarTimeframe.M1:
            # 1m bar closed — update prior_bar_low for next bar sweep detection
            self._prior_bar_low = event.low

    async def _on_trade(self, event: TradeEvent) -> None:
        s = self._state
        size = event.size
        side = event.taker_side

        if side == TakerSide.BUY:
            s.buy_volume += size
            s.delta += size
        elif side == TakerSide.SELL:
            s.sell_volume += size
            s.delta -= size
        s.trade_count += 1

        # Accumulate sweep vs recovery phase delta
        if s.sweep_detected:
            s.recovery_phase_delta += size if side == TakerSide.BUY else -size
        else:
            s.sweep_phase_delta += size if side == TakerSide.BUY else -size

    def _process_s1_bar(self, bar: BarEvent) -> None:
        s = self._state
        bar_ts = bar.timestamp

        # ── New 1m window detection ────────────────────────────────────────
        # Each 1m window contains 60 S1 bars starting at the minute boundary.
        minute_boundary = bar_ts.replace(second=0, microsecond=0)
        if self._current_window_open != minute_boundary:
            self._reset_window(minute_boundary)

        # ── Update running extremes ────────────────────────────────────────
        if s.running_high is None or bar.high > s.running_high:
            s.running_high = bar.high
        if s.running_low is None or bar.low < s.running_low:
            s.running_low = bar.low
            # Check if this S1 bar broke below prior bar low → sweep detected
            if (
                self._prior_bar_low is not None
                and bar.low < self._prior_bar_low
                and not s.sweep_detected
            ):
                s.sweep_detected = True
                s.sweep_low = bar.low
                s.sweep_low_ts = bar_ts
                logger.debug(
                    "%s intrabar sweep detected at %.2f (prior low %.2f) ts=%s",
                    self._symbol, float(bar.low), float(self._prior_bar_low), bar_ts,
                )

        s.seconds_elapsed += 1
        s.seconds_remaining = max(0, 60 - s.seconds_elapsed)

        # Count recovery seconds after sweep
        if s.sweep_detected and s.sweep_low_ts is not None and bar_ts > s.sweep_low_ts:
            s.recovery_seconds += 1

        # ── Early absorption check (every 5 seconds after sweep) ──────────
        if s.sweep_detected and s.recovery_seconds > 0 and s.recovery_seconds % 5 == 0:
            self._update_absorption_signal()

    def _reset_window(self, window_open: datetime) -> None:
        self._current_window_open = window_open
        self._state = IntrabarState(
            symbol=self._symbol,
            window_open_ts=window_open,
        )

    def _update_absorption_signal(self) -> None:
        s = self._state
        sweep_abs = abs(s.sweep_phase_delta)
        if sweep_abs == 0:
            return

        recovery_ratio = abs(s.recovery_phase_delta) / sweep_abs
        hold_score = min(s.recovery_seconds / 15.0, 1.0)
        confidence = recovery_ratio * 0.6 + hold_score * 0.4

        s.absorption_confidence = confidence
        s.early_absorption = confidence >= 0.5 and s.sell_volume >= self._large_trade_threshold

        if s.early_absorption and s.seconds_remaining >= 15:
            logger.info(
                "%s early absorption signal (conf=%.2f, recovery_ratio=%.2f, %ds remaining)",
                self._symbol, confidence, recovery_ratio, s.seconds_remaining,
            )
