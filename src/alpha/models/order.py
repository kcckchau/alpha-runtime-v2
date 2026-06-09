from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from alpha.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)


class OrderIntent(BaseModel):
    """Instruction from the risk engine to the order engine."""

    intent_id: UUID = Field(default_factory=uuid4)
    plan_id: UUID
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY
    created_at: datetime
    account_id: str = "default"
    notes: str = ""


class Order(BaseModel):
    """Lifecycle state of a submitted order."""

    order_id: UUID = Field(default_factory=uuid4)
    broker_order_id: str | None = None
    intent_id: UUID
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    account_id: str = "default"
    filled_quantity: int = 0
    avg_fill_price: Decimal | None = None
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY
    status: OrderStatus = OrderStatus.PENDING
    submitted_at: datetime | None = None
    accepted_at: datetime | None = None
    filled_at: datetime | None = None
    cancelled_at: datetime | None = None
    rejected_at: datetime | None = None
    reject_reason: str | None = None
    commission: Decimal = Decimal("0")

    @property
    def remaining_quantity(self) -> int:
        return self.quantity - self.filled_quantity

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }


class Execution(BaseModel):
    """Individual fill event for an order."""

    execution_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    broker_execution_id: str | None = None
    symbol: str
    side: OrderSide
    quantity: int
    price: Decimal
    commission: Decimal = Decimal("0")
    timestamp: datetime
    account_id: str = "default"
