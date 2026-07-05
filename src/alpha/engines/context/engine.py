"""
Engine 5 — Context Engine

Responsibilities:
  - Track overnight (Globex) high/low for the current session
  - Track previous RTH high/low and close (becomes PDH/PDL next session)
  - Detect RTH open and compute gap vs previous RTH close
  - Compute signed distances from current price to all key war-zone levels
  - Expose ContextSnapshot as the single "where are we?" reference

Input:  BarEvent (M1 only, from EventBus) — updates session-level state only
Output: ContextSnapshot (via get_context(), which computes distances lazily)

Ordering design:
  _handle_bar updates session state (ONH/ONL, PDH/PDL, RTH open, gap) only.
  Distance computation is deferred to get_context(), which is called AFTER
  bus.flush() completes. At that point FeatureEngine.get_snapshot() is
  guaranteed to reflect the current bar. This removes any dependency on
  EventBus subscription order.

Session boundaries (CME equity-index futures / MNQ):
  Overnight / Globex : SessionPhase.PRE_MARKET
                       = 18:00 ET prior calendar day → 09:30 ET session date
                       This covers the FULL Globex window (Asia + London + US
                       pre-market). CME calendar pre_market_open(d) = 18:00 ET
                       on (d-1), so no bars from that window are missed.
  RTH                : 09:30 ET → 16:00 ET  (all non-PRE_MARKET / AFTER_HOURS)
  After-hours        : 16:00 ET → 17:00 ET  (not tracked; used as cooldown)

No datetime.now() is used — all session boundaries derived from bar timestamps.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from alpha.calendar.base import SessionCalendar
from alpha.calendar.resolver import calendar_for_symbol
from alpha.config.settings import AlphaSettings
from alpha.core.clock import Clock
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.context import ContextSnapshot
from alpha.models.enums import BarTimeframe, EventType, HealthStatus, SessionPhase
from alpha.models.events import AnyEvent, BarEvent

if TYPE_CHECKING:
    from alpha.engines.feature.engine import FeatureEngine

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


class _ContextState:
    """Per-symbol session-level accumulator."""

    def __init__(self) -> None:
        self.session_key: str | None = None

        # ── Previous RTH (locked in at session rollover) ──────────────────────
        self.pdh: Decimal | None = None
        self.pdl: Decimal | None = None
        self.prev_rth_close: Decimal | None = None

        # ── Current RTH accumulator (promoted at next session start) ──────────
        self._cur_rth_high: Decimal | None = None
        self._cur_rth_low: Decimal | None = None
        self._cur_rth_close: Decimal | None = None

        # ── Overnight (Globex) ────────────────────────────────────────────────
        self.onh: Decimal | None = None
        self.onl: Decimal | None = None

        # ── RTH open and gap ──────────────────────────────────────────────────
        self.rth_open: Decimal | None = None
        self.gap_points: float | None = None
        self.gap_pct: float | None = None
        self.gap_midpoint: Decimal | None = None

    def rollover(self) -> None:
        """Promote current-session RTH accumulators to previous-session levels."""
        if self._cur_rth_high is not None:
            self.pdh = self._cur_rth_high
        if self._cur_rth_low is not None:
            self.pdl = self._cur_rth_low
        if self._cur_rth_close is not None:
            self.prev_rth_close = self._cur_rth_close
        self._cur_rth_high = None
        self._cur_rth_low = None
        self._cur_rth_close = None
        self.onh = None
        self.onl = None
        self.rth_open = None
        self.gap_points = None
        self.gap_pct = None
        self.gap_midpoint = None


class ContextEngine(BaseEngine):
    """
    Produces a ContextSnapshot on demand with current session-level key levels
    and signed distances from price to each war zone.

    Downstream engines and the replay loop call get_context(symbol) after
    bus.flush() to get a fully populated snapshot with current distances.
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
        self._states: dict[str, _ContextState] = {}
        self._last_bars: dict[str, BarEvent] = {}   # for lazy snapshot building
        self._symbol_calendars: dict[str, SessionCalendar] = {}
        self._feature_engine: FeatureEngine | None = None

    @property
    def name(self) -> str:
        return "ContextEngine"

    def set_feature_engine(self, fe: "FeatureEngine") -> None:
        self._feature_engine = fe

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        for sym in self._registry.active():
            self._symbol_calendars[sym.ticker] = calendar_for_symbol(sym)
            self._states[sym.ticker] = _ContextState()

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.BAR, self._handle_bar)

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        return EngineHealth(
            HealthStatus.HEALTHY,
            self.name,
            {"symbols": len(self._states)},
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def get_context(self, symbol: str) -> ContextSnapshot | None:
        """Build and return a ContextSnapshot with current distances.

        Called after bus.flush() — FeatureEngine.get_snapshot() is guaranteed
        to be current for the same bar, so distances are always accurate.
        """
        state = self._states.get(symbol)
        bar = self._last_bars.get(symbol)
        if state is None or bar is None:
            return None
        feat = (
            self._feature_engine.get_snapshot(symbol)
            if self._feature_engine else None
        )
        return self._build_snapshot(state, bar, feat)

    # ── Handler — session state only, no distances ────────────────────────────

    async def _handle_bar(self, event: AnyEvent) -> None:
        if not isinstance(event, BarEvent):
            return
        if event.timeframe != BarTimeframe.M1:
            return

        sym = event.symbol
        if sym not in self._states:
            sym_obj = self._registry.get(sym)
            cal = calendar_for_symbol(sym_obj) if sym_obj else self._calendar
            self._symbol_calendars[sym] = cal
            self._states[sym] = _ContextState()

        state = self._states[sym]
        cal = self._symbol_calendars.get(sym, self._calendar)

        phase = cal.session_phase(event.timestamp)
        session_key = cal.session_key(event.timestamp)

        # ── Session rollover ──────────────────────────────────────────────────
        if state.session_key != session_key:
            state.rollover()
            state.session_key = session_key

        is_rth = phase not in {
            SessionPhase.PRE_MARKET,
            SessionPhase.AFTER_HOURS,
            SessionPhase.CLOSED,
        }

        # ── Overnight high/low ────────────────────────────────────────────────
        # For CME: PRE_MARKET = 18:00 ET prior day → 09:30 ET = full Globex window
        if phase == SessionPhase.PRE_MARKET:
            if state.onh is None or event.high > state.onh:
                state.onh = event.high
            if state.onl is None or event.low < state.onl:
                state.onl = event.low

        # ── RTH tracking ─────────────────────────────────────────────────────
        if is_rth:
            # First RTH bar: capture open and compute gap
            if state.rth_open is None:
                state.rth_open = event.open
                if state.prev_rth_close is not None and state.prev_rth_close > _ZERO:
                    gap = float(state.rth_open - state.prev_rth_close)
                    state.gap_points = gap
                    state.gap_pct = gap / float(state.prev_rth_close) * 100
                    state.gap_midpoint = (state.rth_open + state.prev_rth_close) / 2

            # Accumulate RTH high/low/close (promoted to PDH/PDL next session)
            if state._cur_rth_high is None or event.high > state._cur_rth_high:
                state._cur_rth_high = event.high
            if state._cur_rth_low is None or event.low < state._cur_rth_low:
                state._cur_rth_low = event.low
            state._cur_rth_close = event.close

        # Store bar for lazy snapshot building in get_context()
        self._last_bars[sym] = event

    # ── Snapshot builder ──────────────────────────────────────────────────────

    def _build_snapshot(self, state: _ContextState, bar: BarEvent, feat) -> ContextSnapshot:
        price = bar.close

        # Pull live indicators from FeatureEngine snapshot
        vwap    = feat.vwap if feat else None
        m5_21   = feat.ema21_5m if feat else None
        orb_h   = feat.orb_high if feat else None
        orb_l   = feat.orb_low if feat else None
        or_mid  = feat.or_mid if feat else None

        # ── War zone candidates ───────────────────────────────────────────────
        # All named key levels: structural, session, and live indicators.
        candidates: list[tuple[str, Decimal]] = []
        if state.onl is not None:             candidates.append(("ONL",        state.onl))
        if state.onh is not None:             candidates.append(("ONH",        state.onh))
        if state.pdl is not None:             candidates.append(("PDL",        state.pdl))
        if state.pdh is not None:             candidates.append(("PDH",        state.pdh))
        if state.prev_rth_close is not None:  candidates.append(("PREV_CLOSE", state.prev_rth_close))
        if state.rth_open is not None:        candidates.append(("RTH_OPEN",   state.rth_open))
        if state.gap_midpoint is not None:    candidates.append(("GAP_MID",    state.gap_midpoint))
        if vwap and vwap > _ZERO:             candidates.append(("VWAP",       vwap))
        if m5_21 is not None:                 candidates.append(("5M21",       m5_21))
        if orb_h is not None:                 candidates.append(("ORH",        orb_h))
        if orb_l is not None:                 candidates.append(("ORL",        orb_l))

        nearest_name: str | None = None
        nearest_price: Decimal | None = None
        nearest_dist: float | None = None
        if candidates:
            name, lvl = min(candidates, key=lambda lp: abs(float(price - lp[1])))
            nearest_name = name
            nearest_price = lvl
            nearest_dist = abs(float(price - lvl))

        # ── Signed distances (positive = price above level) ───────────────────
        def _dist(level: Decimal | None) -> float | None:
            return float(price - level) if level is not None else None

        return ContextSnapshot(
            symbol=bar.symbol,
            timestamp=bar.timestamp,
            onh=state.onh,
            onl=state.onl,
            pdh=state.pdh,
            pdl=state.pdl,
            prev_rth_close=state.prev_rth_close,
            rth_open=state.rth_open,
            gap_points=state.gap_points,
            gap_pct=state.gap_pct,
            gap_midpoint=state.gap_midpoint,
            orb_high=orb_h,
            orb_low=orb_l,
            or_mid=or_mid,
            nearest_war_zone=nearest_name,
            nearest_war_zone_price=nearest_price,
            nearest_war_zone_dist=nearest_dist,
            dist_to_onl=_dist(state.onl),
            dist_to_onh=_dist(state.onh),
            dist_to_pdl=_dist(state.pdl),
            dist_to_pdh=_dist(state.pdh),
            dist_to_vwap=_dist(vwap),
            dist_to_5m21=_dist(m5_21),
            dist_to_orb_high=_dist(orb_h),
            dist_to_orb_low=_dist(orb_l),
            dist_to_prev_close=_dist(state.prev_rth_close),
        )
