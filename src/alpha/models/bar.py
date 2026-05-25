from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, model_validator

from alpha.models.enums import BarTimeframe, DataSourceId


class Bar(BaseModel):
    """Normalized OHLCV bar. Immutable once created."""

    model_config = {"frozen": True}

    symbol: str
    timeframe: BarTimeframe
    timestamp: datetime          # bar open time, timezone-aware
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Decimal | None = None
    trade_count: int | None = None
    is_partial: bool = False     # true for in-progress real-time bars
    source: DataSourceId = DataSourceId.UNKNOWN

    @model_validator(mode="after")
    def validate_ohlc(self) -> "Bar":
        if self.high < self.low:
            raise ValueError("high must be >= low")
        if self.high < self.open or self.high < self.close:
            raise ValueError("high must be >= open and close")
        if self.low > self.open or self.low > self.close:
            raise ValueError("low must be <= open and close")
        return self

    @property
    def range(self) -> Decimal:
        return self.high - self.low

    @property
    def body(self) -> Decimal:
        return abs(self.close - self.open)

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open
