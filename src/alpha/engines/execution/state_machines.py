"""
Execution subsystem state machines.

IntentStateMachine: tracks a TradeIntent from RECEIVED to terminal state.
OrderStateMachine:  tracks a single broker order from PENDING to terminal state.

State transitions are explicit tables — no implicit jumps.
Invalid transitions raise StateMachineError, not silently succeed.

IntentStateMachine transitions:
─────────────────────────────
                   ┌────────────────────────────────────┐
  RECEIVED ────────► VALIDATING ──────────────────────► REJECTED (validation error)
                         │
                         ├── (hard risk clear, no warnings) ──► PLANNING
                         │
                         └── (contextual warnings) ──────────► AWAITING_CONFIRMATION
                                      │
                                      ├── (user confirms within 10s) ──► CONFIRMED ──► PLANNING
                                      │
                                      └── (timeout) ──────────────────► EXPIRED

  PLANNING ────────► PLAN_READY
                         │
                         ├── (decision = APPROVED) ─────────────────────► SUBMITTING
                         └── (decision = REJECTED) ─────────────────────► REJECTED
                         └── (decision = APPROVED_WITH_MODIFICATIONS) ──► AWAITING_CONFIRMATION

  SUBMITTING ──────► ACTIVE (first child order acknowledged by broker)
                         │
                         ├── (all orders terminal) ──► COMPLETED
                         └── (protective stop rejected after fill) ──► FAILED

  ACTIVE ──────────► CANCELLED (user cancels, all working orders cancelled successfully)
  ACTIVE ──────────► FAILED    (unrecoverable: stop rejected after fill, reconciliation failure)
  ACTIVE ──────────► COMPLETED (all orders terminal, position zero or protected)

OrderStateMachine transitions:
──────────────────────────────
  PENDING ─────────► SUBMITTED   (API call initiated)
  SUBMITTED ───────► ACCEPTED    (broker acked with orderId/permId)    [broker callback]
  SUBMITTED ───────► UNKNOWN     (API timeout — trigger reconciliation)
  SUBMITTED ───────► BROKER_REJECTED  [broker callback]
  ACCEPTED ────────► PARTIAL     (first partial fill)                  [broker callback]
  ACCEPTED ────────► FILLED      (full fill)                           [broker callback]
  ACCEPTED ────────► CANCEL_PENDING   (cancel requested locally)
  ACCEPTED ────────► REPLACE_PENDING  (replace requested locally)
  PARTIAL ─────────► PARTIAL     (additional partial fill)             [broker callback]
  PARTIAL ─────────► FILLED      (remaining qty filled)                [broker callback]
  PARTIAL ─────────► CANCEL_PENDING
  CANCEL_PENDING ──► CANCELLED   (cancel confirmed by broker)          [broker callback]
  CANCEL_PENDING ──► PARTIAL     (cancel rejected — already partially filled) [broker callback]
  CANCEL_PENDING ──► FILLED      (cancel rejected — already fully filled)     [broker callback]
  REPLACE_PENDING ► REPLACED     (old order superseded)                [broker callback]
  REPLACE_PENDING ► ACCEPTED     (replace rejected — remains active)   [broker callback]
  UNKNOWN ─────────► (any state after reconciliation confirms broker status)
  REJECTED ────────► (terminal)
  BROKER_REJECTED ► (terminal)
  FILLED ──────────► (terminal)
  CANCELLED ───────► (terminal)
  REPLACED ────────► (terminal)
  EXPIRED ─────────► (terminal)
"""

from __future__ import annotations

from alpha.models.enums import IntentStatus, OrderStatus


class StateMachineError(Exception):
    """Raised when an invalid state transition is attempted."""


# ── Intent state machine ──────────────────────────────────────────────────────

# Maps current state → set of valid next states
_INTENT_TRANSITIONS: dict[IntentStatus, frozenset[IntentStatus]] = {
    IntentStatus.RECEIVED: frozenset({
        IntentStatus.VALIDATING,
    }),
    IntentStatus.VALIDATING: frozenset({
        IntentStatus.REJECTED,          # validation or hard risk failure
        IntentStatus.AWAITING_CONFIRMATION,  # contextual warnings
        IntentStatus.PLANNING,          # clean approval path
    }),
    IntentStatus.AWAITING_CONFIRMATION: frozenset({
        IntentStatus.CONFIRMED,         # user confirmed
        IntentStatus.EXPIRED,           # 10s timeout
        IntentStatus.CANCELLED,         # user pressed Esc
    }),
    IntentStatus.CONFIRMED: frozenset({
        IntentStatus.PLANNING,          # re-enter risk evaluation
    }),
    IntentStatus.PLANNING: frozenset({
        IntentStatus.PLAN_READY,
        IntentStatus.REJECTED,          # risk engine hard-rejects
    }),
    IntentStatus.PLAN_READY: frozenset({
        IntentStatus.SUBMITTING,        # APPROVED → go straight to broker
        IntentStatus.AWAITING_CONFIRMATION,  # APPROVED_WITH_MODIFICATIONS → second confirmation
        IntentStatus.REJECTED,          # REJECTED
    }),
    IntentStatus.SUBMITTING: frozenset({
        IntentStatus.ACTIVE,            # at least one broker ack received
        IntentStatus.FAILED,            # all submissions failed immediately
        IntentStatus.CANCELLED,         # user cancelled before any ack
    }),
    IntentStatus.ACTIVE: frozenset({
        IntentStatus.COMPLETED,
        IntentStatus.CANCELLED,
        IntentStatus.FAILED,
    }),
    # Terminal states — no outbound transitions
    IntentStatus.COMPLETED: frozenset(),
    IntentStatus.CANCELLED: frozenset(),
    IntentStatus.EXPIRED: frozenset(),
    IntentStatus.REJECTED: frozenset(),
    IntentStatus.FAILED: frozenset(),
}

INTENT_TERMINAL_STATES: frozenset[IntentStatus] = frozenset({
    IntentStatus.COMPLETED,
    IntentStatus.CANCELLED,
    IntentStatus.EXPIRED,
    IntentStatus.REJECTED,
    IntentStatus.FAILED,
})


def assert_valid_intent_transition(
    current: IntentStatus,
    next_: IntentStatus,
) -> None:
    """Raise StateMachineError if the transition is not in the allowed table."""
    allowed = _INTENT_TRANSITIONS.get(current, frozenset())
    if next_ not in allowed:
        raise StateMachineError(
            f"Invalid intent transition: {current!r} → {next_!r}. "
            f"Allowed: {sorted(s.value for s in allowed)}"
        )


def is_intent_terminal(status: IntentStatus) -> bool:
    return status in INTENT_TERMINAL_STATES


# ── Order state machine ───────────────────────────────────────────────────────

_ORDER_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({
        OrderStatus.SUBMITTED,
        OrderStatus.REJECTED,           # local pre-flight rejection
    }),
    OrderStatus.SUBMITTED: frozenset({
        OrderStatus.ACCEPTED,           # broker ack                [broker callback]
        OrderStatus.UNKNOWN,            # API timeout — must reconcile
        OrderStatus.BROKER_REJECTED,    # broker refused            [broker callback]
        OrderStatus.CANCELLED,          # broker cancelled before ack (edge case)
    }),
    OrderStatus.ACCEPTED: frozenset({
        OrderStatus.PARTIAL,            # first fill                [broker callback]
        OrderStatus.FILLED,             # immediate full fill       [broker callback]
        OrderStatus.CANCEL_PENDING,     # cancel requested locally
        OrderStatus.REPLACE_PENDING,    # modify requested locally
        OrderStatus.BROKER_REJECTED,    # conditional rejection     [broker callback]
        OrderStatus.EXPIRED,            # TIF lapsed               [broker callback]
    }),
    OrderStatus.PARTIAL: frozenset({
        OrderStatus.PARTIAL,            # additional fill           [broker callback]
        OrderStatus.FILLED,             # completion fill           [broker callback]
        OrderStatus.CANCEL_PENDING,
    }),
    OrderStatus.CANCEL_PENDING: frozenset({
        OrderStatus.CANCELLED,          # cancel confirmed          [broker callback]
        OrderStatus.PARTIAL,            # cancel rejected — more fills came in
        OrderStatus.FILLED,             # cancel rejected — already filled
    }),
    OrderStatus.REPLACE_PENDING: frozenset({
        OrderStatus.REPLACED,           # replace confirmed         [broker callback]
        OrderStatus.ACCEPTED,           # replace rejected — original still active
    }),
    # UNKNOWN: broker state is not confirmed. Any status allowed after reconciliation.
    OrderStatus.UNKNOWN: frozenset({
        OrderStatus.ACCEPTED,
        OrderStatus.PARTIAL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.BROKER_REJECTED,
        OrderStatus.EXPIRED,
    }),
    # Terminal states
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.BROKER_REJECTED: frozenset(),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REPLACED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
}

ORDER_TERMINAL_STATES: frozenset[OrderStatus] = frozenset({
    OrderStatus.REJECTED,
    OrderStatus.BROKER_REJECTED,
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REPLACED,
    OrderStatus.EXPIRED,
})

# States that require broker reconciliation before any further local action
ORDER_RECONCILIATION_REQUIRED: frozenset[OrderStatus] = frozenset({
    OrderStatus.UNKNOWN,
})

# States that only broker callbacks may set (Invariant 7)
ORDER_BROKER_ONLY_STATES: frozenset[OrderStatus] = frozenset({
    OrderStatus.ACCEPTED,
    OrderStatus.PARTIAL,
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REPLACED,
    OrderStatus.BROKER_REJECTED,
    OrderStatus.EXPIRED,
    OrderStatus.UNKNOWN,
})


def assert_valid_order_transition(
    current: OrderStatus,
    next_: OrderStatus,
) -> None:
    """Raise StateMachineError if the transition is not in the allowed table."""
    allowed = _ORDER_TRANSITIONS.get(current, frozenset())
    if next_ not in allowed:
        raise StateMachineError(
            f"Invalid order transition: {current!r} → {next_!r}. "
            f"Allowed: {sorted(s.value for s in allowed)}"
        )


def is_order_terminal(status: OrderStatus) -> bool:
    return status in ORDER_TERMINAL_STATES


def requires_reconciliation(status: OrderStatus) -> bool:
    return status in ORDER_RECONCILIATION_REQUIRED
