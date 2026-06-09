from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator

from alpha.models.enums import AccountType, KillSwitchReason, OrderSide


# ── Account configuration ─────────────────────────────────────────────────────

class AccountConfig(BaseModel):
    """Broker-agnostic per-account risk parameters."""

    account_id: str = "default"
    account_type: AccountType = AccountType.DAY

    # Sizing
    account_size: Decimal = Decimal("25000.00")
    max_daily_loss_pct: float = 0.02       # hard floor, e.g. 2% = $500 on $25k
    max_position_risk_pct: float = 0.01    # per-trade risk
    max_open_positions: int = 5

    # Profit protection — trailing stop on daily P&L high-water mark
    # Only activates once session_high_pnl >= profit_protect_activation
    profit_protect_activation: Decimal = Decimal("200.00")
    # Halt when giveback from peak >= peak * this fraction
    profit_protect_giveback_pct: float = 0.50

    # Kill switch behavior on halt
    kill_switch_flatten: bool = True  # True = flatten positions; False = stop new entries only


# ── Trade plan ────────────────────────────────────────────────────────────────

class TradePlan(BaseModel):
    """Risk-validated trade plan emitted by the Risk Engine."""

    plan_id: UUID = Field(default_factory=uuid4)
    setup_id: UUID
    symbol: str
    side: OrderSide
    account_id: str = "default"   # logical account routed by RiskEngine

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
    account_id: str = "default"
    account_type: AccountType = AccountType.DAY

    # P&L
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    session_high_pnl: Decimal = Decimal("0")   # high-water mark for profit protection
    max_drawdown: Decimal = Decimal("0")

    # Limits
    daily_loss_limit: Decimal

    # Counters
    trades_taken: int = 0
    open_positions: int = 0
    risk_consumed_pct: float = 0.0    # % of daily loss limit consumed

    # Account snapshot (populated by broker sync; 0.0 until first sync)
    net_liquidation: float = 0.0        # total equity in USD
    cash_balance: float = 0.0           # settled cash not in open positions
    gross_position_value: float = 0.0   # abs market value of all open positions
    leverage_ratio: float = 0.0         # gross_position_value / net_liquidation

    # Kill switch state
    is_halted: bool = False
    halt_reason: KillSwitchReason | None = None
    halt_time: datetime | None = None

    @property
    def current_pnl(self) -> Decimal:
        return self.realized_pnl + self.unrealized_pnl

    @property
    def remaining_risk(self) -> Decimal:
        return max(Decimal("0"), self.daily_loss_limit + self.realized_pnl)
