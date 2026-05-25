from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, model_validator

from alpha.models.enums import DataSourceId


class Quote(BaseModel):
    """Normalized NBBO quote snapshot."""

    model_config = {"frozen": True}

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: int
    ask_price: Decimal
    ask_size: int
    bid_exchange: str | None = None
    ask_exchange: str | None = None
    source: DataSourceId = DataSourceId.UNKNOWN

    @model_validator(mode="after")
    def validate_spread(self) -> "Quote":
        if self.ask_price < self.bid_price:
            raise ValueError("ask_price must be >= bid_price")
        return self

    @property
    def spread(self) -> Decimal:
        return self.ask_price - self.bid_price

    @property
    def mid(self) -> Decimal:
        return (self.bid_price + self.ask_price) / 2


class OrderBookLevel(BaseModel):
    price: Decimal
    size: int


class OrderBook(BaseModel):
    """L2 order book snapshot or delta."""

    model_config = {"frozen": True}

    symbol: str
    timestamp: datetime
    bids: list[OrderBookLevel]   # sorted descending by price
    asks: list[OrderBookLevel]   # sorted ascending by price
    is_snapshot: bool = False
    source: DataSourceId = DataSourceId.UNKNOWN

    @property
    def best_bid(self) -> OrderBookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> OrderBookLevel | None:
        return self.asks[0] if self.asks else None
