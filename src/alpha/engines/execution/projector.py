"""
MarketStateProjector — implements MarketStateStore Protocol.

Bridges the ThesisEngine event stream into a queryable MarketContextSnapshot
per instrument. The execution subsystem needs a synchronous point-in-time
snapshot at intent evaluation time — not a stream of events.

Subscription model:
  MarketStateEvent → regime, day_type, vwap_state, structure fields
  BarBundleEvent   → last_price, vwap, signal freshness timestamp
  QuoteEvent       → bid/ask (best bid/ask from live feed)

FeatureEngine.get_snapshot() is called on each MarketStateEvent to pull
ema9, ema21, rvol, orb bounds — same pattern used by MarketStateEngine itself.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from alpha.models.enums import (
    BarTimeframe,
    DataQualityState,
    EventType,
)
from alpha.engines.execution.models import MarketContextSnapshot

if TYPE_CHECKING:
    from alpha.core.event_bus import EventBus
    from alpha.engines.feature.engine import FeatureEngine
    from alpha.models.events import AnyEvent

logger = logging.getLogger(__name__)

_UTC = timezone.utc


class MarketStateProjector:
    """
    Subscribes to EventBus events and maintains the latest MarketContextSnapshot
    per instrument. Thread-safe for asyncio (single-threaded event loop).

    Inject into ExecutionCoordinator as the MarketStateStore dependency.
    """

    def __init__(
        self,
        event_bus: "EventBus",
        feature_engine: "FeatureEngine",
        *,
        max_freshness_ms: int = 5_000,   # snapshot older than this = not fresh
    ) -> None:
        self._feature_engine = feature_engine
        self._max_freshness_ms = max_freshness_ms

        # Latest snapshot per instrument_id
        self._snapshots: dict[str, MarketContextSnapshot] = {}
        # Latest bid/ask from QuoteEvent
        self._quotes: dict[str, tuple[Decimal, Decimal]] = {}   # bid, ask
        # monotonic version counter per instrument
        self._version: dict[str, int] = {}

        event_bus.subscribe(EventType.MARKET_STATE, self._on_market_state)
        event_bus.subscribe(EventType.BAR_BUNDLE, self._on_bar_bundle)
        event_bus.subscribe(EventType.QUOTE, self._on_quote)

    # ── MarketStateStore Protocol ─────────────────────────────────────────────

    def snapshot(self, instrument_id: str) -> MarketContextSnapshot | None:
        return self._snapshots.get(instrument_id)

    def latest_snapshot_id(self, instrument_id: str) -> str | None:
        snap = self._snapshots.get(instrument_id)
        return snap.snapshot_id if snap else None

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_market_state(self, event: "AnyEvent") -> None:
        from alpha.models.market_state import MarketState
        symbol = event.symbol
        try:
            state = MarketState.model_validate(event.state_data)
        except Exception as exc:
            logger.warning("MarketStateProjector: failed to parse MarketState for %s: %s", symbol, exc)
            return

        bar_snap = self._feature_engine.get_snapshot(symbol)
        bid, ask = self._quotes.get(symbol, (Decimal("0"), Decimal("0")))
        last_price = bar_snap.bar.close if bar_snap else Decimal("0")

        # Fallback bid/ask from bar close when no live quote available
        if bid == Decimal("0") and last_price > 0:
            # Half-tick spread proxy (MNQ tick = 0.25)
            bid = last_price - Decimal("0.125")
            ask = last_price + Decimal("0.125")

        vwap = bar_snap.vwap if bar_snap else Decimal("0")
        above_vwap = last_price >= vwap if vwap > 0 else False

        version = self._version.get(symbol, 0) + 1
        self._version[symbol] = version

        snap = MarketContextSnapshot(
            instrument_id=symbol,
            snapshot_id=f"{symbol}-{version}",
            as_of=datetime.now(_UTC),
            bar_timestamp=event.timestamp,
            regime=state.trend,
            day_type=state.day_type,
            last_price=last_price,
            bid=bid,
            ask=ask,
            vwap=vwap,
            above_vwap=above_vwap,
            bars_since_vwap_cross=0,   # not tracked per-event in V1
            ema9=bar_snap.ema_9 if bar_snap else None,
            ema21=bar_snap.ema_21 if bar_snap else None,
            opening_range_high=bar_snap.orb_high if bar_snap else None,
            opening_range_low=bar_snap.orb_low if bar_snap else None,
            rvol=bar_snap.relative_volume if bar_snap else None,
            signal_freshness_ms=0,
            data_quality=DataQualityState.CLEAN,
            setup_scores={},
        )
        self._snapshots[symbol] = snap
        logger.debug("MarketStateProjector: snapshot updated for %s (v%d)", symbol, version)

    async def _on_bar_bundle(self, event: "AnyEvent") -> None:
        # Only 1m bars drive signal freshness
        if event.timeframe != BarTimeframe.M1:
            return
        symbol = event.symbol
        snap = self._snapshots.get(symbol)
        if snap is None:
            return
        # Recompute freshness as 0 (just received a bar) and update last_price
        last_price = event.close
        bid, ask = self._quotes.get(symbol, (Decimal("0"), Decimal("0")))
        if bid == Decimal("0"):
            bid = last_price - Decimal("0.125")
            ask = last_price + Decimal("0.125")

        version = self._version.get(symbol, 0) + 1
        self._version[symbol] = version
        self._snapshots[symbol] = snap.model_copy(update={
            "snapshot_id": f"{symbol}-{version}",
            "as_of": datetime.now(_UTC),
            "bar_timestamp": event.timestamp,
            "last_price": last_price,
            "bid": bid,
            "ask": ask,
            "signal_freshness_ms": 0,
        })

    async def _on_quote(self, event: "AnyEvent") -> None:
        symbol = event.symbol
        bid = event.bid_price
        ask = event.ask_price
        self._quotes[symbol] = (bid, ask)
        snap = self._snapshots.get(symbol)
        if snap is None:
            return
        version = self._version.get(symbol, 0) + 1
        self._version[symbol] = version
        self._snapshots[symbol] = snap.model_copy(update={
            "snapshot_id": f"{symbol}-{version}",
            "as_of": datetime.now(_UTC),
            "bid": bid,
            "ask": ask,
            "last_price": (bid + ask) / 2,
        })

    def freshness_ms(self, instrument_id: str) -> int:
        """Milliseconds since last snapshot update. Used by readiness checks."""
        snap = self._snapshots.get(instrument_id)
        if snap is None:
            return 999_999
        return int((datetime.now(_UTC) - snap.as_of).total_seconds() * 1000)

    def is_fresh(self, instrument_id: str) -> bool:
        return self.freshness_ms(instrument_id) <= self._max_freshness_ms
