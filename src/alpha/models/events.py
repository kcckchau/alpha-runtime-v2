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
    | TradeEvent
    | QuoteEvent
    | OrderBookEvent
    | MarketStateEvent
    | SetupEvent
    | OrderUpdateEvent
    | SystemEvent,
    Field(discriminator="event_type"),
]
