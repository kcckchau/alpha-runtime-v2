from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator

from alpha.models.enums import OrderSide


class TradePlan(BaseModel):
    """Risk-validated trade plan emitted by the Risk Engine."""

    plan_id: UUID = Field(default_factory=uuid4)
    setup_id: UUID
    symbol: str
    side: OrderSide

    # ── Price levels ──────────────────────────────────────────────────────────
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal

    # ── Sizing ────────────────────────────────────────────────────────────────
    position_size: int           # shares / contracts
    risk_amount: Decimal         # dollar risk (entry - stop) * size
    reward_amount: Decimal       # dollar reward (target - entry) * size
    risk_reward_ratio: float

    # ── Account context ──────────────────────────────────────────────────────
    account_size: Decimal
    account_risk_pct: float      # risk_amount / account_size

    # ── Validity ─────────────────────────────────────────────────────────────
    created_at: datetime
    expires_at: datetime | None = None
    is_valid: bool = True
    invalidation_conditions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_levels(self) -> "TradePlan":
        if self.side == OrderSide.BUY:
            if not (self.stop_price < self.entry_price < self.target_price):
                raise ValueError("BUY plan requires stop < entry < target")
        else:
            if not (self.target_price < self.entry_price < self.stop_price):
                raise ValueError("SELL plan requires target < entry < stop")
        return self


class DailyRiskState(BaseModel):
    """Tracks live P&L and risk consumption for the current session."""

    date: str                   # YYYY-MM-DD
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    daily_loss_limit: Decimal
    trades_taken: int = 0
    open_positions: int = 0
    risk_consumed_pct: float = 0.0    # % of daily loss limit consumed
    is_halted: bool = False            # true when daily loss limit hit

    @property
    def remaining_risk(self) -> Decimal:
        return max(Decimal("0"), self.daily_loss_limit + self.realized_pnl)
