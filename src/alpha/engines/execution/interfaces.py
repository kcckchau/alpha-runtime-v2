"""
Execution subsystem interface boundaries.

All cross-component contracts are defined as Protocols so each implementation
(IBKR, paper, sim, replay) can be substituted without touching business logic.

Design rule: the ExecutionCoordinator (coordinator.py, V2) only imports from
these interfaces — never from concrete adapters. This enforces the broker-
agnostic guarantee.

Component responsibilities:
  MarketStateStore         — queryable current state projected from ThesisEngine events
  ExecutionRiskEvaluator   — evaluates TradeIntent → IntentTradePlan (stateless per call)
  OrderRouter              — submits PlannedOrders to a broker, returns acks
  PositionService          — reconciles and serves authoritative position state
  IdempotencyStore         — ensures one command → at most one broker submission
  IntentJournal            — append-only audit log for every intent lifecycle event
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from alpha.engines.execution.models import (
    AccountSnapshot,
    BrokerOrderIdentity,
    ExecutionSnapshot,
    IntentAuditRecord,
    IntentTradePlan,
    MarketContextSnapshot,
    TradeIntent,
)
from alpha.engines.execution.readiness import ExecutionReadiness
from alpha.models.order import Execution, Order


# ── Market state ──────────────────────────────────────────────────────────────

@runtime_checkable
class MarketStateStore(Protocol):
    """
    Queryable current market state per instrument.

    Produced by MarketStateProjector, which subscribes to all ThesisEngine
    output events (BarEvent, SetupEvent, MarketStateEvent, etc.) and folds
    them into the latest MarketContextSnapshot per symbol.

    snapshot() must always return the LATEST complete snapshot — never stale.
    If no snapshot is available yet (pre-warm-up), returns None.
    Callers must treat None as market_data_fresh=False.
    """

    def snapshot(self, instrument_id: str) -> MarketContextSnapshot | None: ...
    def latest_snapshot_id(self, instrument_id: str) -> str | None: ...


# ── Risk evaluation ───────────────────────────────────────────────────────────

@runtime_checkable
class ExecutionRiskEvaluator(Protocol):
    """
    Evaluates a TradeIntent against immutable snapshots.

    evaluate() is a pure function: given fixed inputs, always returns the same
    IntentTradePlan. It does not mutate state, does not call the broker, and
    does not read live market feeds — it reads only the snapshots provided.

    This means the same inputs can be replayed in a test or audit context to
    reproduce any historical decision exactly.

    The plan returned always references snapshot IDs (Invariant 9). If snapshot
    IDs are missing, the implementation must raise before returning.
    """

    def evaluate(
        self,
        intent: TradeIntent,
        market: MarketContextSnapshot,
        account: AccountSnapshot,
        execution: ExecutionSnapshot,
    ) -> IntentTradePlan: ...


# ── Order routing ─────────────────────────────────────────────────────────────

@runtime_checkable
class OrderRouter(Protocol):
    """
    Broker-agnostic order submission and management.

    submit_bracket(): submits entry + stop (+ optional target) as an OCO bracket.
    The broker must guarantee that when the entry fills, stop and target become
    active, and when one of stop/target fills, the other is cancelled.

    cancel(): requests cancellation of a working order. The order transitions to
    CANCEL_PENDING locally; the broker callback confirms CANCELLED or returns the
    order to its prior state if already filled.

    reconcile(): queries the broker for all open orders and recent executions.
    Returns orders not found in local state (UNKNOWN or UNMANAGED).
    Called on startup and after any API timeout.

    All implementations must:
    - Write runtime_order_id into the broker's order reference field (IBKR: orderRef)
    - Never submit the same runtime_order_id twice (Invariant 2)
    - Propagate all fills and status changes via registered callbacks
    """

    async def submit_bracket(
        self,
        plan: IntentTradePlan,
    ) -> list[BrokerOrderIdentity]: ...

    async def cancel(self, runtime_order_id: str) -> None: ...

    async def flatten(self, instrument_id: str) -> list[BrokerOrderIdentity]: ...

    async def reconcile(self) -> "ReconciliationResult": ...

    def register_order_callback(self, handler: "OrderCallbackT") -> None: ...
    def register_fill_callback(self, handler: "FillCallbackT") -> None: ...

    @property
    def is_connected(self) -> bool: ...

    @property
    def is_paper(self) -> bool: ...


# ── Position service ──────────────────────────────────────────────────────────

@runtime_checkable
class PositionService(Protocol):
    """
    Authoritative position state, reconciled against the broker on startup.

    reconcile(): fetches current positions from broker and loads them.
    Must complete before ExecutionReadiness.positions_reconciled = True.

    snapshot(): returns an immutable ExecutionSnapshot for risk evaluation.
    The snapshot includes filled positions, working orders, and submitting intents.

    on_fill(): called by the order router on every fill event.
    Updates filled position and recalculates unrealized P&L.
    """

    async def reconcile(self) -> None: ...

    def snapshot(self) -> ExecutionSnapshot: ...

    def on_fill(self, fill: Execution) -> None: ...

    def positions(self) -> dict[str, float]: ...


# ── Idempotency ───────────────────────────────────────────────────────────────

@runtime_checkable
class IdempotencyStore(Protocol):
    """
    Ensures one frontend input event → at most one broker submission.

    check_and_register(): atomically checks if idempotency_key exists and
    registers it if not. Returns True if this is a new key (proceed), False if
    it already exists (return original response).

    The store is in-memory within a session. A process restart generates new
    frontend session IDs, so cross-session deduplication is not needed.
    """

    def check_and_register(self, idempotency_key: str, intent_id: str) -> bool: ...

    def get_intent_id(self, idempotency_key: str) -> str | None: ...


# ── Intent journal ────────────────────────────────────────────────────────────

@runtime_checkable
class IntentJournal(Protocol):
    """
    Append-only audit log for every intent lifecycle event.

    Every record captures what the user pressed, what the runtime saw
    (snapshot IDs), what decision was made, and what happened.

    This data answers: did discretionary overrides outperform runtime-approved
    trades? In what regime does the user have edge, and where should they defer?
    """

    async def record(self, entry: IntentAuditRecord) -> None: ...

    async def get_intent_history(self, intent_id: str) -> list[IntentAuditRecord]: ...


# ── Callback types ────────────────────────────────────────────────────────────

from typing import Callable, Coroutine, Any

OrderCallbackT = Callable[[Order], Coroutine[Any, Any, None]]
FillCallbackT = Callable[[Execution], Coroutine[Any, Any, None]]


# ── Reconciliation result ─────────────────────────────────────────────────────

from dataclasses import dataclass, field


@dataclass
class ReconciliationResult:
    """
    Output of OrderRouter.reconcile().

    unmanaged_orders: orders found at broker not in local state.
      These may be manual orders entered directly in TWS, or orders from a
      previous session that were not persisted. Each must be reviewed before
      new intents are allowed.

    unknown_resolved: local orders that were in UNKNOWN state and whose
      broker status has now been confirmed.

    positions_changed: True if broker-side positions differ from local state.
    """
    unmanaged_orders: list[BrokerOrderIdentity] = field(default_factory=list)
    unknown_resolved: list[tuple[str, str]] = field(default_factory=list)  # (runtime_order_id, resolved_status)
    positions_changed: bool = False
    reconciled_at: str = ""  # ISO datetime string
