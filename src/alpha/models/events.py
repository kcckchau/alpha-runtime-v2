"""
Normalized event contracts.

ALL data — historical, replay, and live — flows through these types.
No engine should ever receive raw source-specific data directly.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from alpha.models.enums import (
    BarTimeframe,
    DataSourceId,
    EventType,
    OrderSide,
    OrderStatus,
    ORBState,
    SetupState,
    SetupType,
    TakerSide,
)
from alpha.models.flow import BarFlowContext


# ── Metadata ─────────────────────────────────────────────────────────────────


class EventMetadata(BaseModel):
    model_config = {"frozen": True}

    event_id: UUID = Field(default_factory=uuid4)
    source: DataSourceId = DataSourceId.UNKNOWN
    received_at: datetime              # wall-clock time this was ingested / emitted
    is_replay: bool = False
    sequence_num: int | None = None    # monotonic ordering guarantee within a source


# ── Base ──────────────────────────────────────────────────────────────────────


class BaseEvent(BaseModel):
    model_config = {"frozen": True}

    event_type: EventType
    symbol: str
    timestamp: datetime               # event time (bar open, trade time, etc.)
    metadata: EventMetadata


# ── Market data events ────────────────────────────────────────────────────────


class BarEvent(BaseEvent):
    event_type: Literal[EventType.BAR] = EventType.BAR
    timeframe: BarTimeframe
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal | None = None
    trade_count: int | None = None
    is_partial: bool = False           # in-progress real-time bar
    is_pipeline_fallback: bool = False # True = re-published by BarPipeline; aggregator must ignore


class TradeEvent(BaseEvent):
    event_type: Literal[EventType.TRADE] = EventType.TRADE
    price: Decimal
    size: int
    conditions: list[str] = Field(default_factory=list)
    exchange: str | None = None
    taker_side: TakerSide = TakerSide.UNKNOWN
    trade_id: str | None = None


class QuoteEvent(BaseEvent):
    event_type: Literal[EventType.QUOTE] = EventType.QUOTE
    bid_price: Decimal
    bid_size: int
    ask_price: Decimal
    ask_size: int
    last_price: Decimal | None = None   # last trade price from the exchange
    last_size: int | None = None
    bid_exchange: str | None = None
    ask_exchange: str | None = None


class OrderBookEvent(BaseEvent):
    event_type: Literal[EventType.ORDER_BOOK] = EventType.ORDER_BOOK
    # (price, size) pairs; bids desc, asks asc
    bids: list[tuple[Decimal, int]] = Field(default_factory=list)
    asks: list[tuple[Decimal, int]] = Field(default_factory=list)
    is_snapshot: bool = False


# ── Engine output events ──────────────────────────────────────────────────────


class MarketStateEvent(BaseEvent):
    """Emitted by MarketStateEngine after each bar is classified."""

    event_type: Literal[EventType.MARKET_STATE] = EventType.MARKET_STATE
    # Full MarketState embedded to keep engines decoupled from model imports
    state_data: dict[str, Any] = Field(default_factory=dict)


class SetupEvent(BaseEvent):
    """Emitted by SetupEngine on any setup state transition."""

    event_type: Literal[EventType.SETUP] = EventType.SETUP
    setup_id: UUID
    setup_type: SetupType
    setup_state: SetupState
    prev_state: SetupState | None = None


class ThesisEvent(BaseEvent):
    """Emitted by ThesisEngine on every thesis state change or bar update."""

    event_type: Literal[EventType.THESIS] = EventType.THESIS

    thesis_id: UUID
    thesis_type: str          # ThesisType value
    thesis_state: str         # ThesisState value
    confidence: float
    bars_alive: int

    # Entry plan
    entry: Decimal | None = None
    stop: Decimal | None = None
    target: Decimal | None = None
    risk_pts: Decimal | None = None
    reward_pts: Decimal | None = None
    risk_ratio: float | None = None

    # Key levels
    key_level: Decimal | None = None
    sweep_low: Decimal | None = None
    rejection_high: Decimal | None = None

    # Narrative
    evidence_positive: list[str] = Field(default_factory=list)   # supporting items
    evidence_negative: list[str] = Field(default_factory=list)   # weakening items
    commit_conditions: list[str] = Field(default_factory=list)
    invalidation_conditions: list[str] = Field(default_factory=list)
    possible_flip: str | None = None
    invalidation_reason: str | None = None

    # Context at time of emission
    vwap_touch_count: int = 0
    last_vwap_outcome: str | None = None
    session_phase: str | None = None


class BarBundleEvent(BaseEvent):
    """
    Emitted by BarFlowAggregator when a 1m bar is sealed with its flow context.

    This is the primary input for the sequential pipeline:
      BarBundleEvent → FeatureEngine → MarketState → Thesis → Setup

    flow: None when trade/quote data is unavailable (e.g. historical bars without full-signals cache).
    """

    event_type: Literal[EventType.BAR_BUNDLE] = EventType.BAR_BUNDLE
    timeframe: BarTimeframe
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal | None = None
    trade_count: int | None = None
    flow: BarFlowContext | None = None   # None = flow data not available for this bar

    def to_bar_event(self) -> "BarEvent":
        """Return a BarEvent with the same OHLCV fields (no flow context).
        is_pipeline_fallback=True so BarFlowAggregator ignores it and avoids
        re-sealing the already-closed window."""
        return BarEvent(
            event_type=EventType.BAR,
            symbol=self.symbol,
            timestamp=self.timestamp,
            timeframe=self.timeframe,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            vwap=self.vwap,
            trade_count=self.trade_count,
            is_pipeline_fallback=True,
            metadata=self.metadata,
        )


class OrderUpdateEvent(BaseEvent):
    """Emitted by OrderEngine on any order lifecycle change."""

    event_type: Literal[EventType.ORDER_UPDATE] = EventType.ORDER_UPDATE
    order_id: UUID
    broker_order_id: str | None = None
    order_status: OrderStatus
    filled_quantity: int = 0
    avg_fill_price: Decimal | None = None
    reject_reason: str | None = None
    account_id: str = "default"
    side: OrderSide | None = None


class SystemEvent(BaseEvent):
    """Runtime lifecycle and control events (not symbol-specific)."""

    event_type: Literal[EventType.SYSTEM] = EventType.SYSTEM
    symbol: str = "*"                 # wildcard — system events broadcast to all
    event_name: str                   # e.g. "session_open", "catchup_complete"
    payload: dict[str, Any] = Field(default_factory=dict)


# ── Discriminated union ───────────────────────────────────────────────────────

AnyEvent = Annotated[
    BarEvent
    | BarBundleEvent
    | TradeEvent
    | QuoteEvent
    | OrderBookEvent
    | MarketStateEvent
    | SetupEvent
    | ThesisEvent
    | OrderUpdateEvent
    | SystemEvent,
    Field(discriminator="event_type"),
]
