"""
Execution subsystem domain models.

Design principles:
  - TradeIntent  = what the user *wants*  (from hotkey/API, broker-agnostic)
  - IntentTradePlan = what the runtime *will do* (risk-compiled, immutable, auditable)
  - PlannedOrder = a single broker order within a plan bracket
  - Snapshots (Market/Account/Execution) are immutable at evaluation time so the
    risk journal can replay exactly what the engine saw when it approved/rejected.

Naming note: `IntentTradePlan` is distinct from the existing `alpha.models.risk.TradePlan`
which is setup-centric (setup_id). This plan is intent-centric (intent_id) and comes
from the execution subsystem rather than the thesis→risk path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Mapping
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from alpha.models.enums import (
    DataQualityState,
    DayType,
    IntentSource,
    IntentStatus,
    KillSwitchReason,
    OrderRole,
    OrderSide,
    OrderType,
    PriceMode,
    RiskDecision,
    TimeInForce,
    TradeAction,
    TrendState,
)


# ── Quote / market reference ───────────────────────────────────────────────────

class QuoteSnapshot(BaseModel):
    """
    Browser-captured quote at the moment of keypress.

    CONTEXTUAL ONLY — never used as the authoritative price source.
    The backend always resolves the final price from its own market snapshot.
    Used by the backend to detect client/server drift and stale UI.
    """
    model_config = ConfigDict(frozen=True)

    instrument_id: str
    bid: Decimal
    ask: Decimal
    last: Decimal
    timestamp: datetime

    @property
    def spread_ticks(self) -> Decimal:
        return self.ask - self.bid


class MarketContextSnapshot(BaseModel):
    """
    Immutable snapshot of market state at intent evaluation time.

    Produced by MarketStateProjector (subscribes to ThesisEngine events).
    Stored alongside every risk decision so the audit log can replay exactly
    what the engine saw — independent of how market conditions changed after.

    snapshot_id: deterministic version counter or content hash, monotonically
    increasing within a session. Referenced in IntentTradePlan for traceability.
    """
    model_config = ConfigDict(frozen=True)

    instrument_id: str
    snapshot_id: str                       # monotonic version within session
    as_of: datetime
    bar_timestamp: datetime

    # Regime
    regime: TrendState
    day_type: DayType

    # Price
    last_price: Decimal
    bid: Decimal
    ask: Decimal
    vwap: Decimal

    # Structure
    above_vwap: bool
    bars_since_vwap_cross: int
    ema9: Decimal | None
    ema21: Decimal | None
    opening_range_high: Decimal | None
    opening_range_low: Decimal | None

    # Volume / momentum
    rvol: float | None                     # relative volume vs average
    signal_freshness_ms: int               # ms since last ThesisEngine event for this symbol

    # Feed quality
    data_quality: DataQualityState

    # Setup scores (setup_type → score 0.0–1.0)
    setup_scores: dict[str, float] = Field(default_factory=dict)

    @property
    def is_data_reliable(self) -> bool:
        return self.data_quality in {DataQualityState.CLEAN, DataQualityState.RECOVERING}

    @property
    def spread_ticks(self) -> Decimal:
        return self.ask - self.bid


class AccountSnapshot(BaseModel):
    """
    Immutable snapshot of account state at intent evaluation time.

    snapshot_id monotonically increases on every P&L event, fill, or
    reconciliation. Stored in IntentTradePlan for full audit traceability.
    """
    model_config = ConfigDict(frozen=True)

    snapshot_id: str
    as_of: datetime
    account_id: str

    net_liquidation: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    daily_loss_limit: Decimal
    session_high_pnl: Decimal              # high-water mark for profit protection

    is_halted: bool
    halt_reason: KillSwitchReason | None = None

    @property
    def current_pnl(self) -> Decimal:
        return self.realized_pnl + self.unrealized_pnl

    @property
    def remaining_daily_risk(self) -> Decimal:
        return max(Decimal("0"), self.daily_loss_limit + self.realized_pnl)


@dataclass(frozen=True)
class WorkingOrder:
    """A broker order that is not yet terminal."""
    runtime_order_id: str
    instrument_id: str
    side: OrderSide
    quantity: Decimal
    role: OrderRole


class ExecutionSnapshot(BaseModel):
    """
    Immutable snapshot of the execution subsystem state at intent evaluation time.

    effective_long_exposure and effective_short_exposure are computed properties
    that include BOTH filled positions AND pending/working orders — the only correct
    way to measure exposure before fills arrive.
    """
    model_config = ConfigDict(frozen=True)

    snapshot_id: str
    as_of: datetime

    # Broker-reconciled positions (instrument_id → signed quantity; positive=long)
    positions: dict[str, Decimal] = Field(default_factory=dict)

    # Working broker orders not yet terminal
    working_orders: list[WorkingOrder] = Field(default_factory=list)

    # Intent IDs currently in SUBMITTING state (API call in flight, no broker ack yet)
    submitting_intent_ids: list[str] = Field(default_factory=list)

    session_trade_count: int = 0
    consecutive_losses: int = 0

    def effective_exposure(self, instrument_id: str) -> Decimal:
        """
        Signed net exposure including filled position and all pending quantity.

        Invariant 5: effective exposure = filled + pending.
        Positive = net long, negative = net short.
        """
        filled = self.positions.get(instrument_id, Decimal("0"))
        pending = sum(
            (o.quantity if o.side == OrderSide.BUY else -o.quantity)
            for o in self.working_orders
            if o.instrument_id == instrument_id and o.role == OrderRole.ENTRY
        )
        return filled + pending


# ── Execution policy ──────────────────────────────────────────────────────────

class ExecutionPolicy(BaseModel):
    """
    Broker-agnostic price resolution policy.

    The backend resolves `price_mode` against its authoritative MarketContextSnapshot
    to produce a concrete limit price. The frontend never dictates the final price.
    """
    model_config = ConfigDict(frozen=True)

    price_mode: PriceMode = PriceMode.MARKETABLE_LIMIT
    max_slippage_ticks: int = 3
    time_in_force: TimeInForce = TimeInForce.DAY


class ProtectionPolicy(BaseModel):
    """
    Defines the bracket protection structure for a trade.

    `use_broker_bracket` must be True in V1 — protective orders must live at the
    broker, not just in runtime memory. If the process crashes, the stop must survive.

    `stop_retry_attempts`: how many times to retry a rejected stop child before
    triggering emergency flatten + kill switch. Default 1 (retry once, then flatten).
    This is the first-class failure path: entry fills, stop rejected.
    """
    model_config = ConfigDict(frozen=True)

    stop_distance_ticks: int
    target_distance_ticks: int | None = None    # None = no take-profit order
    use_broker_bracket: bool = True             # MUST be True in V1
    stop_retry_attempts: int = 1


# ── Trade intent ──────────────────────────────────────────────────────────────

class TradeIntent(BaseModel):
    """
    User-initiated trade intent from a hotkey or API call.

    This is NOT an order — it is a structured expression of what the user wants
    to do. The ExecutionCoordinator validates it, runs it through the risk engine,
    and compiles it into an IntentTradePlan before anything reaches a broker.

    idempotency_key:
        Generated by the frontend from the specific input event
        (e.g. "session-abc:keydown-event-42"). Used by IdempotencyStore to
        ensure that retrying the same HTTP request never creates a second intent.
        This is DISTINCT from temporal duplicate detection (UI debounce) and
        from duplicate trade policy (scale-in limit).

    client_quote:
        CONTEXTUAL only. Never used as the final price. Backend uses it to
        detect stale UI (reject if client bid differs from server bid by >2 ticks,
        or if quote age > 500ms).
    """
    model_config = ConfigDict(frozen=True)

    intent_id: UUID = Field(default_factory=uuid4)
    idempotency_key: str                        # from frontend input event ID
    created_at: datetime

    instrument_id: str
    action: TradeAction
    quantity: Decimal

    execution_policy: ExecutionPolicy
    protection_policy: ProtectionPolicy

    source: IntentSource
    client_quote: QuoteSnapshot | None = None

    # Mutable via model_copy — tracks progress through IntentStateMachine
    status: IntentStatus = IntentStatus.RECEIVED


# ── Planned order (output of risk compilation) ────────────────────────────────

class PlannedOrder(BaseModel):
    """
    A single broker order within an IntentTradePlan bracket.

    runtime_order_id is the stable identifier that flows all the way to the
    broker (written into IBKR orderRef). It is assigned here, before submission,
    so the entire lifecycle can be traced by this single string.
    """
    model_config = ConfigDict(frozen=True)

    runtime_order_id: str = Field(default_factory=lambda: f"ro-{uuid4().hex[:12]}")
    role: OrderRole
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY


# ── Intent trade plan (output of risk engine for intent path) ─────────────────

class IntentTradePlan(BaseModel):
    """
    Risk-compiled, immutable execution plan for a TradeIntent.

    Distinct from `alpha.models.risk.TradePlan` (setup-centric).
    This plan is produced by the ExecutionRiskEvaluator in the execution subsystem.

    Snapshot IDs (market_context_snapshot_id, account_snapshot_id, risk_policy_version)
    are stored so the audit log can reconstruct exactly what state the engine saw at
    evaluation time — independent of what changed afterward.

    requires_confirmation: True when decision == APPROVED_WITH_MODIFICATIONS.
    The IntentStateMachine transitions to AWAITING_CONFIRMATION rather than
    SUBMITTING. At confirmation time, the backend revalidates quote freshness,
    risk limits, and kill-switch state before submitting.
    """
    model_config = ConfigDict(frozen=True)

    plan_id: UUID = Field(default_factory=uuid4)
    intent_id: UUID

    decision: RiskDecision

    # Bracket orders (None if decision == REJECTED)
    entry_order: PlannedOrder | None = None
    stop_order: PlannedOrder | None = None
    target_orders: tuple[PlannedOrder, ...] = ()

    # Risk metrics
    calculated_risk_usd: Decimal = Decimal("0")
    max_loss_usd: Decimal = Decimal("0")
    reward_risk_ratio: float = 0.0

    # Audit trail — the exact inputs used to produce this decision
    market_context_snapshot_id: str
    account_snapshot_id: str
    risk_policy_version: str
    evaluated_at: datetime

    # Human-readable decision context
    risk_flags: tuple[str, ...] = ()        # warnings that did NOT block approval
    explanation: tuple[str, ...] = ()       # full rationale (for journal + UI)

    @property
    def requires_confirmation(self) -> bool:
        return self.decision == RiskDecision.APPROVED_WITH_MODIFICATIONS

    @property
    def all_orders(self) -> list[PlannedOrder]:
        orders = []
        if self.entry_order:
            orders.append(self.entry_order)
        if self.stop_order:
            orders.append(self.stop_order)
        orders.extend(self.target_orders)
        return orders


# ── Broker order identity ─────────────────────────────────────────────────────

@dataclass
class BrokerOrderIdentity:
    """
    Maps a runtime_order_id to all broker-native identifiers.

    runtime_order_id: assigned by execution subsystem, written into IBKR orderRef.
    broker_order_id: IBKR orderId — session-scoped, reused across connections.
    broker_perm_id: IBKR permId — persistent across sessions, better for reconciliation.

    On reconciliation: query IBKR by orderRef prefix to find orders submitted by
    this runtime, then match broker_perm_id back to local state. Use broker_order_id
    only for within-session operations (cancel, replace).

    UNKNOWN broker outcome: do NOT retry submission. Query the broker using
    runtime_order_id (via orderRef) to determine actual status first.
    """
    runtime_order_id: str
    broker: str                       # "ibkr", "paper", "sim"
    broker_order_ref: str             # what we write into IBKR orderRef
    broker_client_id: int | None = None
    broker_order_id: int | None = None    # session-scoped
    broker_perm_id: int | None = None     # persistent across sessions


# ── Audit record ──────────────────────────────────────────────────────────────

@dataclass
class IntentAuditRecord:
    """
    Append-only journal entry for every significant intent lifecycle event.

    Stored permanently so you can answer: what did the user intend, what did
    the runtime see, what did it decide, did the user override, what happened.
    """
    intent_id: str
    event: str                         # e.g. "received", "approved", "override_requested"
    timestamp: datetime
    market_snapshot_id: str | None = None
    account_snapshot_id: str | None = None
    detail: str = ""
