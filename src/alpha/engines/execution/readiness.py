"""
ExecutionReadiness — startup gate for the execution subsystem.

Trading is DISABLED until ALL conditions are True.
This is enforced by Invariant 1 (see invariants.py).

Startup sequence:
    STARTING
    → CONNECTING_TO_BROKER   (TCP/TWS connection established)
    → RECONCILING_POSITIONS  (broker positions loaded into PositionService)
    → RECONCILING_ORDERS     (open orders matched against local OrderStateMachine)
    → RECONCILING_EXECUTIONS (fills since last checkpoint verified; no orphan positions)
    → LOADING_ACCOUNT_STATE  (daily P&L, limits, account type confirmed)
    → WAITING_MARKET_DATA    (first clean MarketContextSnapshot received)
    → READY

Any condition reverting to False during a live session immediately
transitions readiness to NOT READY and disables new intents.
In-flight orders (already submitted) are not cancelled — only new
intents are blocked. The system waits for the condition to recover.

market_data_fresh: True iff the last MarketContextSnapshot for the
target instrument was received within max_market_data_age_ms.
Set to False on WebSocket disconnect or DataQualityState.FAILED.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class ExecutionReadiness:
    broker_connected: bool = False
    positions_reconciled: bool = False
    orders_reconciled: bool = False
    executions_reconciled: bool = False
    account_state_loaded: bool = False
    market_data_fresh: bool = False

    @property
    def ready(self) -> bool:
        return (
            self.broker_connected
            and self.positions_reconciled
            and self.orders_reconciled
            and self.executions_reconciled
            and self.account_state_loaded
            and self.market_data_fresh
        )

    @property
    def blocking_reasons(self) -> list[str]:
        reasons = []
        if not self.broker_connected:
            reasons.append("broker_not_connected")
        if not self.positions_reconciled:
            reasons.append("positions_not_reconciled")
        if not self.orders_reconciled:
            reasons.append("orders_not_reconciled")
        if not self.executions_reconciled:
            reasons.append("executions_not_reconciled")
        if not self.account_state_loaded:
            reasons.append("account_state_not_loaded")
        if not self.market_data_fresh:
            reasons.append("market_data_stale_or_disconnected")
        return reasons

    def with_update(self, **kwargs: bool) -> "ExecutionReadiness":
        """Return a new ExecutionReadiness with the given fields updated."""
        return ExecutionReadiness(
            broker_connected=kwargs.get("broker_connected", self.broker_connected),
            positions_reconciled=kwargs.get("positions_reconciled", self.positions_reconciled),
            orders_reconciled=kwargs.get("orders_reconciled", self.orders_reconciled),
            executions_reconciled=kwargs.get("executions_reconciled", self.executions_reconciled),
            account_state_loaded=kwargs.get("account_state_loaded", self.account_state_loaded),
            market_data_fresh=kwargs.get("market_data_fresh", self.market_data_fresh),
        )


EXECUTION_NOT_READY = ExecutionReadiness()
