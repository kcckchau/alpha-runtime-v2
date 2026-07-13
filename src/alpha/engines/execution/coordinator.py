"""
ExecutionCoordinator — wires the execution subsystem together.

Entry points (called by API routes):
  submit_intent(intent)         → validate, evaluate risk, return IntentResponse
  confirm_intent(intent_id)     → revalidate + submit to broker (for amber intents)
  cancel_intent(intent_id)      → cancel a pending-confirmation intent

Internal flow:
  submit_intent()
    1. assert_runtime_ready()
    2. idempotency_store.check_and_register()
    3. assemble MarketContextSnapshot, AccountSnapshot, ExecutionSnapshot
    4. risk_evaluator.evaluate()
    5. if REJECTED → journal + return
    6. if APPROVED_WITH_MODIFICATIONS → store as pending, return (requires confirmation)
    7. if APPROVED → submit_bracket() → journal + return

  confirm_intent()
    1. find pending intent (must be AWAITING_CONFIRMATION, not expired)
    2. revalidate: quote freshness, kill switch, exposure
    3. submit_bracket()
    4. journal

Readiness management:
  update_readiness(**kwargs) updates the ExecutionReadiness gate.
  No intent may reach the broker until all 6 conditions are True.

Account snapshot:
  In V1, assembled from a DailyRiskState getter (injected at construction).
  If no getter provided (standalone paper mode), uses a safe default.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable
from uuid import UUID

from pydantic import BaseModel

from alpha.engines.execution.interfaces import ReconciliationResult
from alpha.engines.execution.invariants import (
    assert_no_blind_retry_on_unknown,
    assert_not_duplicate_intent,
    assert_runtime_ready,
    check_client_quote_drift,
)
from alpha.engines.execution.models import (
    AccountSnapshot,
    ExecutionSnapshot,
    IntentAuditRecord,
    IntentTradePlan,
    TradeIntent,
    WorkingOrder,
)
from alpha.engines.execution.readiness import EXECUTION_NOT_READY, ExecutionReadiness
from alpha.engines.execution.state_machines import (
    assert_valid_intent_transition,
    is_intent_terminal,
)
from alpha.models.enums import (
    IntentStatus,
    KillSwitchReason,
    OrderRole,
    OrderSide,
    OrderStatus,
    RiskDecision,
    TradeAction,
)
from alpha.models.order import Execution, Order

logger = logging.getLogger(__name__)

_UTC = timezone.utc
_CONFIRMATION_TIMEOUT_S = 10.0
_MAX_CLIENT_DRIFT_TICKS = Decimal("0.50")   # 2 ticks on MNQ


class IntentResponse(BaseModel):
    """Response returned to the API caller after intent submission or confirmation."""
    intent_id: str
    status: IntentStatus
    decision: RiskDecision | None = None
    entry_price: Decimal | None = None
    stop_price: Decimal | None = None
    target_price: Decimal | None = None
    risk_usd: Decimal | None = None
    reward_risk: float | None = None
    explanation: list[str] = []
    risk_flags: list[str] = []
    requires_confirmation: bool = False
    confirmation_expires_at: datetime | None = None


class _PendingIntent:
    """A plan waiting for user confirmation (AWAITING_CONFIRMATION state)."""
    def __init__(self, intent: TradeIntent, plan: IntentTradePlan) -> None:
        self.intent = intent
        self.plan = plan
        self.created_at = datetime.now(_UTC)
        self.expires_at = self.created_at.replace(
            second=self.created_at.second,
            microsecond=self.created_at.microsecond,
        )
        import time
        self._expiry_ts = time.monotonic() + _CONFIRMATION_TIMEOUT_S

    def is_expired(self) -> bool:
        import time
        return time.monotonic() > self._expiry_ts


class ExecutionCoordinator:
    """
    Orchestrates the full intent lifecycle.

    Dependencies (all injected, implement their respective Protocols):
      market_store      : MarketStateStore (MarketStateProjector)
      risk_evaluator    : ExecutionRiskEvaluator (V1RiskEvaluator)
      order_router      : OrderRouter (PaperOrderRouter)
      idempotency_store : IdempotencyStore (InMemoryIdempotencyStore)
      journal           : IntentJournal (InMemoryIntentJournal)
      account_state_fn  : Callable[[], AccountSnapshot] — pulls from RiskEngine
    """

    def __init__(
        self,
        market_store: Any,
        risk_evaluator: Any,
        order_router: Any,
        idempotency_store: Any,
        journal: Any,
        *,
        account_state_fn: Callable[[], AccountSnapshot] | None = None,
        risk_policy_version: str = "v1.0.0",
    ) -> None:
        self._market = market_store
        self._risk = risk_evaluator
        self._router = order_router
        self._idempotency = idempotency_store
        self._journal = journal
        self._account_state_fn = account_state_fn
        self._risk_policy_version = risk_policy_version

        self._readiness = EXECUTION_NOT_READY
        self._pending: dict[str, _PendingIntent] = {}     # intent_id → pending
        self._intent_status: dict[str, IntentStatus] = {} # intent_id → current status

        # Position tracking (updated on fills)
        self._positions: dict[str, Decimal] = {}          # instrument_id → signed qty
        self._working_orders: list[WorkingOrder] = []

        # Register fill handler
        order_router.register_fill_callback(self._on_fill)
        order_router.register_order_callback(self._on_order_update)

    # ── Readiness management ──────────────────────────────────────────────────

    def update_readiness(self, **kwargs: bool) -> None:
        self._readiness = self._readiness.with_update(**kwargs)
        logger.info(
            "ExecutionReadiness updated | ready=%s | blocking=%s",
            self._readiness.ready,
            self._readiness.blocking_reasons,
        )

    @property
    def readiness(self) -> ExecutionReadiness:
        return self._readiness

    # ── Public API ────────────────────────────────────────────────────────────

    async def submit_intent(self, intent: TradeIntent) -> IntentResponse:
        """
        Main entry point. Validates, evaluates risk, returns response.
        If APPROVED_WITH_MODIFICATIONS, holds as pending and returns requires_confirmation=True.
        """
        intent_id = str(intent.intent_id)

        # Invariant 1: readiness gate
        try:
            assert_runtime_ready(self._readiness)
        except Exception as exc:
            await self._journal_event(intent_id, "rejected_not_ready", str(exc))
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.REJECTED,
                explanation=[str(exc)],
            )

        # Invariant 3: idempotency
        try:
            assert_not_duplicate_intent(intent.idempotency_key, set(self._idempotency._store))
        except Exception:
            existing_id = self._idempotency.get_intent_id(intent.idempotency_key)
            if existing_id:
                return self._status_response(existing_id)
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.REJECTED,
                explanation=["Duplicate idempotency_key — original response not found"],
            )

        self._idempotency.check_and_register(intent.idempotency_key, intent_id)
        self._update_status(intent_id, IntentStatus.VALIDATING)
        await self._journal_event(intent_id, "received", f"source={intent.source}")

        # Assemble evaluation inputs
        market = self._market.snapshot(intent.instrument_id)
        if market is None:
            self._update_status(intent_id, IntentStatus.REJECTED)
            await self._journal_event(intent_id, "rejected_no_market_data", "")
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.REJECTED,
                explanation=["No market data available for instrument"],
            )

        # Log client quote drift (Invariant 8 — warning only, does not block)
        drift_warnings = check_client_quote_drift(
            intent, market, max_drift_ticks=_MAX_CLIENT_DRIFT_TICKS
        )
        if drift_warnings:
            logger.warning("Client quote drift detected: %s", drift_warnings)
            await self._journal_event(intent_id, "client_quote_drift", "; ".join(drift_warnings))

        account = self._account_snapshot()
        execution = self._execution_snapshot()

        # Risk evaluation
        self._update_status(intent_id, IntentStatus.PLANNING)
        plan = self._risk.evaluate(intent, market, account, execution)
        self._update_status(intent_id, IntentStatus.PLAN_READY)

        await self._journal_event(
            intent_id, f"evaluated_{plan.decision.value}",
            f"flags={plan.risk_flags}",
            market_snapshot_id=market.snapshot_id,
            account_snapshot_id=account.snapshot_id,
        )

        if plan.decision == RiskDecision.REJECTED:
            self._update_status(intent_id, IntentStatus.REJECTED)
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.REJECTED,
                decision=RiskDecision.REJECTED,
                explanation=list(plan.explanation),
                risk_flags=list(plan.risk_flags),
            )

        if plan.decision == RiskDecision.APPROVED_WITH_MODIFICATIONS:
            self._update_status(intent_id, IntentStatus.AWAITING_CONFIRMATION)
            self._pending[intent_id] = _PendingIntent(intent, plan)
            await self._journal_event(intent_id, "awaiting_confirmation", "amber — second press required")
            return self._plan_response(intent_id, IntentStatus.AWAITING_CONFIRMATION, plan)

        # APPROVED: submit immediately
        return await self._submit_plan(intent_id, intent, plan)

    async def confirm_intent(self, intent_id: str) -> IntentResponse:
        """
        Confirm a pending-confirmation intent.

        POST /trade-intents/{intent_id}/confirm

        Revalidates quote freshness, kill switch, and exposure before submitting.
        """
        pending = self._pending.get(intent_id)
        if pending is None:
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.REJECTED,
                explanation=["No pending intent found — may have expired or already been confirmed"],
            )

        if pending.is_expired():
            del self._pending[intent_id]
            self._update_status(intent_id, IntentStatus.EXPIRED)
            await self._journal_event(intent_id, "expired", "confirmation window elapsed")
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.EXPIRED,
                explanation=[f"Confirmation window ({_CONFIRMATION_TIMEOUT_S}s) elapsed"],
            )

        # Revalidate with current state (10s may have passed)
        market = self._market.snapshot(pending.intent.instrument_id)
        account = self._account_snapshot()
        execution = self._execution_snapshot()

        if market is None or account.is_halted:
            self._update_status(intent_id, IntentStatus.REJECTED)
            del self._pending[intent_id]
            reason = "kill switch activated during confirmation window" if account.is_halted else "market data lost"
            await self._journal_event(intent_id, "rejected_on_revalidation", reason)
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.REJECTED,
                explanation=[f"Revalidation failed: {reason}"],
            )

        # Rerun full risk evaluation with fresh snapshots
        self._update_status(intent_id, IntentStatus.CONFIRMED)
        plan = self._risk.evaluate(pending.intent, market, account, execution)

        await self._journal_event(
            intent_id, f"revalidated_{plan.decision.value}",
            "confirmation revalidation",
            market_snapshot_id=market.snapshot_id,
            account_snapshot_id=account.snapshot_id,
        )

        if plan.decision == RiskDecision.REJECTED:
            del self._pending[intent_id]
            self._update_status(intent_id, IntentStatus.REJECTED)
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.REJECTED,
                decision=RiskDecision.REJECTED,
                explanation=["Revalidation rejected: " + "; ".join(plan.explanation)],
            )

        del self._pending[intent_id]
        return await self._submit_plan(intent_id, pending.intent, plan)

    async def cancel_intent(self, intent_id: str) -> IntentResponse:
        """Cancel a pending-confirmation intent (user pressed Esc)."""
        pending = self._pending.pop(intent_id, None)
        if pending:
            self._update_status(intent_id, IntentStatus.CANCELLED)
            await self._journal_event(intent_id, "cancelled_by_user", "")
        return IntentResponse(
            intent_id=intent_id,
            status=self._intent_status.get(intent_id, IntentStatus.CANCELLED),
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _submit_plan(
        self,
        intent_id: str,
        intent: TradeIntent,
        plan: IntentTradePlan,
    ) -> IntentResponse:
        self._update_status(intent_id, IntentStatus.SUBMITTING)
        try:
            identities = await self._router.submit_bracket(plan)
        except Exception as exc:
            self._update_status(intent_id, IntentStatus.FAILED)
            await self._journal_event(intent_id, "submission_failed", str(exc))
            logger.error("ExecutionCoordinator: bracket submission failed for %s: %s", intent_id, exc)
            return IntentResponse(
                intent_id=intent_id,
                status=IntentStatus.FAILED,
                explanation=[f"Broker submission failed: {exc}"],
            )

        self._update_status(intent_id, IntentStatus.ACTIVE)

        # Register working entry order for exposure tracking
        if plan.entry_order:
            self._working_orders.append(WorkingOrder(
                runtime_order_id=plan.entry_order.runtime_order_id,
                instrument_id=intent.instrument_id,
                side=plan.entry_order.side,
                quantity=plan.entry_order.quantity,
                role=OrderRole.ENTRY,
            ))

        await self._journal_event(
            intent_id, "submitted",
            f"identities={[i.runtime_order_id for i in identities]}",
        )
        logger.info(
            "ExecutionCoordinator: intent submitted | id=%s action=%s qty=%s",
            intent_id[:8], intent.action, intent.quantity,
        )
        return self._plan_response(intent_id, IntentStatus.ACTIVE, plan)

    async def _on_fill(self, fill: Execution) -> None:
        """Update position on fill. Called by order router."""
        # Determine position delta from fill
        qty = Decimal(str(fill.quantity))
        delta = qty if fill.side == OrderSide.BUY else -qty

        sym = fill.symbol
        self._positions[sym] = self._positions.get(sym, Decimal("0")) + delta

        # Remove from working orders
        self._working_orders = [
            o for o in self._working_orders
            if o.runtime_order_id != str(fill.order_id)
        ]
        logger.info(
            "ExecutionCoordinator: fill | symbol=%s side=%s qty=%s @ %s | net_position=%s",
            sym, fill.side, fill.quantity, fill.price, self._positions.get(sym, 0),
        )

    async def _on_order_update(self, order: Order) -> None:
        """Handle order status updates for working order tracking."""
        if order.is_terminal:
            self._working_orders = [
                o for o in self._working_orders
                if o.runtime_order_id != str(order.order_id)
            ]

    def _execution_snapshot(self) -> ExecutionSnapshot:
        from alpha.engines.execution.models import ExecutionSnapshot
        import time
        return ExecutionSnapshot(
            snapshot_id=f"exec-{int(time.time() * 1000)}",
            as_of=datetime.now(_UTC),
            positions={k: v for k, v in self._positions.items()},
            working_orders=list(self._working_orders),
        )

    def _account_snapshot(self) -> AccountSnapshot:
        if self._account_state_fn is not None:
            return self._account_state_fn()
        # Safe default for standalone paper mode
        import time
        return AccountSnapshot(
            snapshot_id=f"acct-{int(time.time() * 1000)}",
            as_of=datetime.now(_UTC),
            account_id="paper",
            net_liquidation=Decimal("25000.00"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            daily_loss_limit=Decimal("500.00"),
            session_high_pnl=Decimal("0"),
            is_halted=False,
        )

    def _update_status(self, intent_id: str, status: IntentStatus) -> None:
        current = self._intent_status.get(intent_id)
        if current is not None and not is_intent_terminal(current):
            try:
                assert_valid_intent_transition(current, status)
            except Exception as exc:
                logger.warning("Intent state machine: %s", exc)
                return
        self._intent_status[intent_id] = status

    def _status_response(self, intent_id: str) -> IntentResponse:
        return IntentResponse(
            intent_id=intent_id,
            status=self._intent_status.get(intent_id, IntentStatus.UNKNOWN if hasattr(IntentStatus, "UNKNOWN") else IntentStatus.REJECTED),
        )

    def _plan_response(
        self,
        intent_id: str,
        status: IntentStatus,
        plan: IntentTradePlan,
    ) -> IntentResponse:
        entry_price = plan.entry_order.limit_price if plan.entry_order else None
        stop_price = plan.stop_order.stop_price if plan.stop_order else None
        target_price = (
            plan.target_orders[0].limit_price if plan.target_orders else None
        )
        return IntentResponse(
            intent_id=intent_id,
            status=status,
            decision=plan.decision,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            risk_usd=plan.calculated_risk_usd,
            reward_risk=plan.reward_risk_ratio,
            explanation=list(plan.explanation),
            risk_flags=list(plan.risk_flags),
            requires_confirmation=plan.requires_confirmation,
            confirmation_expires_at=(
                datetime.now(_UTC).replace(second=datetime.now(_UTC).second + int(_CONFIRMATION_TIMEOUT_S))
                if plan.requires_confirmation else None
            ),
        )

    async def _journal_event(
        self,
        intent_id: str,
        event: str,
        detail: str,
        market_snapshot_id: str | None = None,
        account_snapshot_id: str | None = None,
    ) -> None:
        await self._journal.record(IntentAuditRecord(
            intent_id=intent_id,
            event=event,
            timestamp=datetime.now(_UTC),
            market_snapshot_id=market_snapshot_id,
            account_snapshot_id=account_snapshot_id,
            detail=detail,
        ))
