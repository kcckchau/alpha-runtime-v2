"""
Execution subsystem invariants.

These 10 invariants must hold at all times. They are the non-negotiable safety
rules of the execution subsystem. Any violation raises ExecutionInvariantViolation
and must halt the offending operation — never silently continue.

Invariants are enforced as explicit guard functions so they can be called at every
critical decision point AND unit-tested against known failure scenarios.

The 10 invariants:

  1. Runtime not READY → no intent may reach broker.
  2. Every broker submission has exactly one stable runtime_order_id.
  3. Retrying the same command never creates a second broker order.
  4. Unknown broker outcome never triggers blind resubmission.
  5. Effective exposure includes filled AND pending quantity.
  6. No live position may exist without known protection status, unless
     explicitly marked UNPROTECTED and trading is halted.
  7. Only broker acknowledgements/fills mutate authoritative order state.
  8. Frontend quote is never authoritative.
  9. Every risk decision references immutable market/account snapshots.
 10. Hard risk cannot be bypassed by UI override.
"""

from __future__ import annotations

from decimal import Decimal

from alpha.engines.execution.models import (
    AccountSnapshot,
    ExecutionSnapshot,
    IntentTradePlan,
    MarketContextSnapshot,
    TradeIntent,
)
from alpha.engines.execution.readiness import ExecutionReadiness
from alpha.models.enums import (
    IntentStatus,
    KillSwitchReason,
    OrderStatus,
    TradeAction,
)


class ExecutionInvariantViolation(Exception):
    """Raised when a hard execution invariant is violated. Must never be caught silently."""


class DuplicateIntentError(Exception):
    """
    Raised when an idempotency_key has already been processed.
    Caller should return the original response, not create a new intent.
    """
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(f"Duplicate idempotency_key: {idempotency_key!r}")
        self.idempotency_key = idempotency_key


class UnknownBrokerOutcomeError(Exception):
    """
    Raised when attempting to act on an order in UNKNOWN status.
    Caller must reconcile with broker before any further action.
    """
    def __init__(self, runtime_order_id: str) -> None:
        super().__init__(
            f"Order {runtime_order_id!r} is UNKNOWN — reconcile before retry or cancel"
        )
        self.runtime_order_id = runtime_order_id


class UnprotectedPositionError(Exception):
    """
    Raised when a live position has no known protective stop and is not
    explicitly marked UNPROTECTED with trading halted.
    """
    def __init__(self, instrument_id: str, position: Decimal) -> None:
        super().__init__(
            f"Position {position} in {instrument_id!r} has no protective stop and trading is not halted"
        )
        self.instrument_id = instrument_id
        self.position = position


# ── Invariant 1 ───────────────────────────────────────────────────────────────

def assert_runtime_ready(readiness: ExecutionReadiness) -> None:
    """
    Invariant 1: Runtime not READY → no intent may reach broker.

    Call this at the top of every public method that could trigger a broker submission.
    This is the hard gate — no exceptions, no bypasses.
    """
    if not readiness.ready:
        raise ExecutionInvariantViolation(
            f"Execution subsystem is not READY. Blocking reasons: {readiness.blocking_reasons}"
        )


# ── Invariant 2 ───────────────────────────────────────────────────────────────

def assert_unique_runtime_order_id(
    runtime_order_id: str,
    existing_ids: set[str],
) -> None:
    """
    Invariant 2: Every broker submission has exactly one stable runtime_order_id.

    The registry of submitted runtime_order_ids must be consulted before every
    new submission. A collision means a programming error — two paths tried to
    submit under the same ID.
    """
    if runtime_order_id in existing_ids:
        raise ExecutionInvariantViolation(
            f"runtime_order_id collision: {runtime_order_id!r} already exists in submitted order registry"
        )


# ── Invariant 3 ───────────────────────────────────────────────────────────────

def assert_not_duplicate_intent(
    idempotency_key: str,
    processed_keys: set[str],
) -> None:
    """
    Invariant 3: Retrying the same command never creates a second broker order.

    idempotency_key comes from the frontend input event (e.g. "session-abc:event-42").
    If the key is already in processed_keys, the caller must return the original
    response rather than creating a new intent.

    This is SEPARATE from:
    - UI debounce (keyboard event.repeat guard — frontend concern)
    - Duplicate trade policy (scale-in limits — risk engine concern)
    """
    if idempotency_key in processed_keys:
        raise DuplicateIntentError(idempotency_key)


# ── Invariant 4 ───────────────────────────────────────────────────────────────

def assert_no_blind_retry_on_unknown(
    runtime_order_id: str,
    order_status: OrderStatus,
) -> None:
    """
    Invariant 4: Unknown broker outcome never triggers blind resubmission.

    A timeout on a broker API call does NOT mean the order was rejected.
    IBKR may have received and accepted it. Any retry without first querying
    the broker by orderRef/permId risks creating a duplicate position.
    """
    if order_status == OrderStatus.UNKNOWN:
        raise UnknownBrokerOutcomeError(runtime_order_id)


# ── Invariant 5 ───────────────────────────────────────────────────────────────

def effective_exposure(
    instrument_id: str,
    execution_snapshot: ExecutionSnapshot,
) -> Decimal:
    """
    Invariant 5: Effective exposure = filled position + pending quantity.

    Never check only the filled position when evaluating a new intent.
    A working buy order that hasn't filled yet still consumes exposure.
    """
    return execution_snapshot.effective_exposure(instrument_id)


def assert_exposure_within_limit(
    instrument_id: str,
    proposed_additional: Decimal,
    execution_snapshot: ExecutionSnapshot,
    max_net_contracts: Decimal,
) -> None:
    """Raises if the proposed additional quantity would breach the max net exposure."""
    current = effective_exposure(instrument_id, execution_snapshot)
    projected = abs(current + proposed_additional)
    if projected > max_net_contracts:
        raise ExecutionInvariantViolation(
            f"Exposure limit breach: current={current}, proposed_additional={proposed_additional}, "
            f"projected={projected} > max={max_net_contracts}"
        )


# ── Invariant 6 ───────────────────────────────────────────────────────────────

def assert_position_protected(
    instrument_id: str,
    execution_snapshot: ExecutionSnapshot,
    protected_instrument_ids: set[str],
    trading_halted: bool,
) -> None:
    """
    Invariant 6: No live position without known protection, unless halted.

    A "protected" position has a working stop order at the broker.
    If a position is found without protection and trading is not halted,
    this is an unmanaged risk — the runtime must halt immediately.
    """
    from alpha.models.enums import OrderRole
    position = execution_snapshot.positions.get(instrument_id, Decimal("0"))
    if position == 0:
        return
    has_stop = instrument_id in protected_instrument_ids
    if not has_stop and not trading_halted:
        raise UnprotectedPositionError(instrument_id, position)


# ── Invariant 7 ───────────────────────────────────────────────────────────────

def assert_only_broker_mutates_order_state(
    current_status: OrderStatus,
    proposed_status: OrderStatus,
    mutation_source: str,
) -> None:
    """
    Invariant 7: Only broker acknowledgements/fills mutate authoritative order state.

    Local code may set PENDING, SUBMITTING, CANCEL_PENDING, REPLACE_PENDING.
    All other transitions must originate from a broker callback, not internal logic.
    """
    broker_only_states = {
        OrderStatus.ACCEPTED,
        OrderStatus.PARTIAL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REPLACED,
        OrderStatus.BROKER_REJECTED,
        OrderStatus.EXPIRED,
        OrderStatus.UNKNOWN,
    }
    local_allowed_states = {
        OrderStatus.PENDING,
        OrderStatus.SUBMITTED,
        OrderStatus.CANCEL_PENDING,
        OrderStatus.REPLACE_PENDING,
        OrderStatus.REJECTED,  # local validation rejection
    }
    if proposed_status in broker_only_states and mutation_source != "broker_callback":
        raise ExecutionInvariantViolation(
            f"Only broker callbacks may set order status to {proposed_status!r}. "
            f"Attempted by: {mutation_source!r}"
        )


# ── Invariant 8 ───────────────────────────────────────────────────────────────

def assert_price_from_backend(
    price_source: str,
) -> None:
    """
    Invariant 8: Frontend quote is never authoritative.

    The final limit price on any PlannedOrder must be derived from the
    backend's MarketContextSnapshot, not the client_quote in TradeIntent.
    """
    if price_source == "client_quote":
        raise ExecutionInvariantViolation(
            "Order price must be resolved from backend MarketContextSnapshot, "
            "not from the frontend client_quote."
        )


def check_client_quote_drift(
    intent: TradeIntent,
    server_snapshot: MarketContextSnapshot,
    max_drift_ticks: Decimal,
    max_age_ms: int = 500,
) -> list[str]:
    """
    Enforce Invariant 8 by flagging stale or drifted client quotes.

    Returns a list of warning strings (empty = quote is acceptable context).
    A non-empty list does not block the trade but is logged and included in
    the audit record. Use reject_on_drift=True in hard risk checks if needed.
    """
    warnings: list[str] = []
    if intent.client_quote is None:
        return warnings

    quote = intent.client_quote
    age_ms = int((server_snapshot.as_of - quote.timestamp).total_seconds() * 1000)
    if age_ms > max_age_ms:
        warnings.append(f"client_quote age {age_ms}ms exceeds {max_age_ms}ms threshold")

    drift = abs(quote.bid - server_snapshot.bid)
    if drift > max_drift_ticks:
        warnings.append(
            f"client bid {quote.bid} differs from server bid {server_snapshot.bid} "
            f"by {drift} (max allowed: {max_drift_ticks} ticks)"
        )

    return warnings


# ── Invariant 9 ───────────────────────────────────────────────────────────────

def assert_plan_has_snapshot_ids(plan: IntentTradePlan) -> None:
    """
    Invariant 9: Every risk decision references immutable snapshots.

    Without snapshot IDs, the audit log cannot reconstruct what the engine saw.
    """
    if not plan.market_context_snapshot_id:
        raise ExecutionInvariantViolation(
            f"IntentTradePlan {plan.plan_id} is missing market_context_snapshot_id"
        )
    if not plan.account_snapshot_id:
        raise ExecutionInvariantViolation(
            f"IntentTradePlan {plan.plan_id} is missing account_snapshot_id"
        )
    if not plan.risk_policy_version:
        raise ExecutionInvariantViolation(
            f"IntentTradePlan {plan.plan_id} is missing risk_policy_version"
        )


# ── Invariant 10 ──────────────────────────────────────────────────────────────

HARD_RISK_REASONS: frozenset[str] = frozenset({
    "daily_loss_limit_breached",
    "kill_switch_active",
    "broker_not_connected",
    "positions_not_reconciled",
    "orders_not_reconciled",
    "market_data_stale",
    "market_halted",
    "max_contracts_exceeded",
    "unmanaged_position",
    "duplicate_intent",
    "unknown_broker_order",
})


def assert_no_hard_risk_override(
    risk_flags: tuple[str, ...],
    override_requested: bool,
) -> None:
    """
    Invariant 10: Hard risk cannot be bypassed by UI override.

    UI double-press or explicit override key (Ctrl+Shift+Q) may bypass
    SOFT contextual risk warnings (regime mismatch, low RVOL, etc.).
    Hard risk reasons (kill switch, limit breach, disconnected) can never
    be overridden — regardless of what the user presses.
    """
    if not override_requested:
        return
    hard_flags = frozenset(risk_flags) & HARD_RISK_REASONS
    if hard_flags:
        raise ExecutionInvariantViolation(
            f"Hard risk cannot be bypassed by UI override. Active hard risk flags: {hard_flags}"
        )
