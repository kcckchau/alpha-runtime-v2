from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from alpha.models.enums import DataSourceId, TakerSide


class Trade(BaseModel):
    """Normalized single trade tick."""

    model_config = {"frozen": True}

    symbol: str
    timestamp: datetime       # trade execution time, timezone-aware
    price: Decimal
    size: int
    conditions: list[str] = []
    exchange: str | None = None
    taker_side: TakerSide = TakerSide.UNKNOWN
    trade_id: str | None = None
    source: DataSourceId = DataSourceId.UNKNOWN
