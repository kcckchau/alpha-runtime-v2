from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

from alpha.models.enums import AssetClass


class Symbol(BaseModel):
    ticker: str
    exchange: str
    asset_class: AssetClass
    root_symbol: str | None = None
    contract_month: str | None = None   # YYYYMM for futures when applicable
    currency: str = "USD"
    description: str = ""
    lot_size: int = 1
    tick_size: Decimal = Decimal("0.01")
    point_value: Decimal = Decimal("1.0")
    is_active: bool = True
    # optional: used for relative strength comparisons
    sector: str | None = None
    benchmark: str | None = None   # e.g. "SPY" for most equities

    @field_validator("ticker")
    @classmethod
    def ticker_upper(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("exchange")
    @classmethod
    def exchange_upper(cls, v: str) -> str:
        return v.upper().strip()

    def __hash__(self) -> int:
        return hash(self.ticker)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Symbol):
            return self.ticker == other.ticker
        return NotImplemented

    def __repr__(self) -> str:
        return f"Symbol({self.ticker}@{self.exchange})"


class SymbolConfig(BaseModel):
    """Runtime configuration overrides per symbol."""

    ticker: str
    enabled: bool = True
    timeframes: list[str] = Field(default_factory=lambda: ["1m"])
    orb_minutes: int = 5                  # minutes to use for opening range
    max_position_size: int | None = None  # override global risk sizing
    notes: str = ""
