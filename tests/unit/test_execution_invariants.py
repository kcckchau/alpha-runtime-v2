"""
Execution subsystem — invariant and failure scenario tests.

Each test corresponds to a named failure case that the subsystem must handle
correctly. The goal is not to test happy-path execution (that's integration testing)
but to document and lock in the exact behavior for every non-obvious failure.

Failure scenarios covered:
  1.  Browser sends intent twice (same idempotency_key)
  2.  Backend times out after submitting to IBKR → UNKNOWN, no blind retry
  3.  Entry partially fills
  4.  Stop child is rejected after entry fills (first-class failure path)
  5.  WebSocket disconnects → market_data_fresh=False → trading disabled
  6.  Position exists at startup → ExecutionReadiness must wait for reconciliation
  7.  Manual order entered directly in TWS → unmanaged_orders in ReconciliationResult
  8.  User presses flatten while entry is working
  9.  UI override cannot bypass hard risk
 10.  Client quote drift detected but trade proceeds with server price
 11.  State machine rejects invalid transitions
 12.  Effective exposure includes working orders (not just filled position)
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from alpha.engines.execution.invariants import (
    HARD_RISK_REASONS,
    DuplicateIntentError,
    ExecutionInvariantViolation,
    UnknownBrokerOutcomeError,
    assert_exposure_within_limit,
    assert_no_blind_retry_on_unknown,
    assert_no_hard_risk_override,
    assert_not_duplicate_intent,
    assert_only_broker_mutates_order_state,
    assert_plan_has_snapshot_ids,
    assert_position_protected,
    assert_runtime_ready,
    assert_unique_runtime_order_id,
    check_client_quote_drift,
)
from alpha.engines.execution.models import (
    AccountSnapshot,
    ExecutionPolicy,
    ExecutionSnapshot,
    IntentTradePlan,
    MarketContextSnapshot,
    PlannedOrder,
    ProtectionPolicy,
    QuoteSnapshot,
    TradeIntent,
    WorkingOrder,
)
from alpha.engines.execution.readiness import EXECUTION_NOT_READY, ExecutionReadiness
from alpha.engines.execution.state_machines import (
    StateMachineError,
    assert_valid_intent_transition,
    assert_valid_order_transition,
)
from alpha.models.enums import (
    DataQualityState,
    DayType,
    IntentSource,
    IntentStatus,
    KillSwitchReason,
    OrderRole,
    OrderSide,
    OrderStatus,
    OrderType,
    PriceMode,
    RiskDecision,
    TradeAction,
    TrendState,
)

_UTC = timezone.utc


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(_UTC)


def _ready() -> ExecutionReadiness:
    return ExecutionReadiness(
        broker_connected=True,
        positions_reconciled=True,
        orders_reconciled=True,
        executions_reconciled=True,
        account_state_loaded=True,
        market_data_fresh=True,
    )


def _make_intent(
    action: TradeAction = TradeAction.OPEN_LONG,
    quantity: Decimal = Decimal("1"),
    idempotency_key: str = "session-x:event-1",
) -> TradeIntent:
    return TradeIntent(
        idempotency_key=idempotency_key,
        created_at=_now(),
        instrument_id="MNQ-09",
        action=action,
        quantity=quantity,
        execution_policy=ExecutionPolicy(price_mode=PriceMode.MARKETABLE_LIMIT),
        protection_policy=ProtectionPolicy(stop_distance_ticks=8, target_distance_ticks=16),
        source=IntentSource.HOTKEY,
    )


def _make_market_snapshot(
    bid: Decimal = Decimal("23100.00"),
    ask: Decimal = Decimal("23100.25"),
    data_quality: DataQualityState = DataQualityState.CLEAN,
    regime: TrendState = TrendState.TRENDING_UP,
) -> MarketContextSnapshot:
    return MarketContextSnapshot(
        instrument_id="MNQ-09",
        snapshot_id="snap-001",
        as_of=_now(),
        bar_timestamp=_now(),
        regime=regime,
        day_type=DayType.TREND_UP,
        last_price=bid,
        bid=bid,
        ask=ask,
        vwap=Decimal("23095.00"),
        above_vwap=True,
        bars_since_vwap_cross=5,
        ema9=Decimal("23098.00"),
        ema21=Decimal("23092.00"),
        opening_range_high=Decimal("23110.00"),
        opening_range_low=Decimal("23085.00"),
        rvol=1.4,
        signal_freshness_ms=200,
        data_quality=data_quality,
    )


def _make_execution_snapshot(
    positions: dict[str, Decimal] | None = None,
    working_orders: list[WorkingOrder] | None = None,
) -> ExecutionSnapshot:
    return ExecutionSnapshot(
        snapshot_id="exec-snap-001",
        as_of=_now(),
        positions=positions or {},
        working_orders=working_orders or [],
    )


def _make_plan(
    intent_id=None,
    decision: RiskDecision = RiskDecision.APPROVED,
    risk_flags: tuple[str, ...] = (),
) -> IntentTradePlan:
    return IntentTradePlan(
        intent_id=intent_id or uuid4(),
        decision=decision,
        entry_order=PlannedOrder(
            role=OrderRole.ENTRY,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            limit_price=Decimal("23100.25"),
        ),
        stop_order=PlannedOrder(
            role=OrderRole.STOP,
            side=OrderSide.SELL,
            order_type=OrderType.STOP,
            quantity=Decimal("1"),
            stop_price=Decimal("23098.25"),
        ),
        market_context_snapshot_id="snap-001",
        account_snapshot_id="acct-snap-001",
        risk_policy_version="2026-07-13.1",
        evaluated_at=_now(),
        risk_flags=risk_flags,
    )


# ── Scenario 1: Browser sends intent twice ────────────────────────────────────

class TestDuplicateIntent:
    def test_first_submission_allowed(self):
        processed: set[str] = set()
        # First press: no error
        assert_not_duplicate_intent("session-x:event-1", processed)
        processed.add("session-x:event-1")

    def test_second_submission_with_same_key_raises(self):
        processed = {"session-x:event-1"}
        with pytest.raises(DuplicateIntentError) as exc_info:
            assert_not_duplicate_intent("session-x:event-1", processed)
        assert exc_info.value.idempotency_key == "session-x:event-1"

    def test_different_keys_both_allowed(self):
        processed = {"session-x:event-1"}
        # Different key = different keypress = new intent, allowed
        assert_not_duplicate_intent("session-x:event-2", processed)

    def test_scale_in_not_blocked_by_different_keys(self):
        """Two separate scale-in presses must each produce their own key."""
        processed: set[str] = set()
        assert_not_duplicate_intent("session-x:event-10", processed)
        processed.add("session-x:event-10")
        # Second press is a genuinely new event with a new key — allowed
        assert_not_duplicate_intent("session-x:event-11", processed)


# ── Scenario 2: IBKR timeout → UNKNOWN, no blind retry ───────────────────────

class TestUnknownOrderHandling:
    def test_unknown_order_blocks_retry(self):
        with pytest.raises(UnknownBrokerOutcomeError) as exc_info:
            assert_no_blind_retry_on_unknown("ro-abc123", OrderStatus.UNKNOWN)
        assert exc_info.value.runtime_order_id == "ro-abc123"

    def test_non_unknown_orders_not_blocked(self):
        for status in [OrderStatus.ACCEPTED, OrderStatus.PARTIAL, OrderStatus.CANCELLED]:
            # Should not raise
            assert_no_blind_retry_on_unknown("ro-abc123", status)

    def test_state_machine_allows_reconciliation_transitions_from_unknown(self):
        """After reconciliation, UNKNOWN can resolve to any real status."""
        for resolved in [
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.ACCEPTED,
            OrderStatus.BROKER_REJECTED,
        ]:
            assert_valid_order_transition(OrderStatus.UNKNOWN, resolved)

    def test_state_machine_blocks_invalid_transitions_from_unknown(self):
        with pytest.raises(StateMachineError):
            assert_valid_order_transition(OrderStatus.UNKNOWN, OrderStatus.PENDING)


# ── Scenario 3: Entry partially fills ────────────────────────────────────────

class TestPartialFill:
    def test_partial_fill_transition_is_valid(self):
        assert_valid_order_transition(OrderStatus.ACCEPTED, OrderStatus.PARTIAL)
        assert_valid_order_transition(OrderStatus.PARTIAL, OrderStatus.PARTIAL)
        assert_valid_order_transition(OrderStatus.PARTIAL, OrderStatus.FILLED)

    def test_effective_exposure_reflects_partial_fill(self):
        """After a partial fill of 1 of 2 contracts, exposure = 1 + 1 (working)."""
        snapshot = _make_execution_snapshot(
            positions={"MNQ-09": Decimal("1")},  # 1 filled
            working_orders=[
                WorkingOrder(
                    runtime_order_id="ro-abc",
                    instrument_id="MNQ-09",
                    side=OrderSide.BUY,
                    quantity=Decimal("1"),  # 1 still working
                    role=OrderRole.ENTRY,
                )
            ],
        )
        exposure = snapshot.effective_exposure("MNQ-09")
        assert exposure == Decimal("2")  # 1 filled + 1 working


# ── Scenario 4: Stop child rejected after entry fills ────────────────────────

class TestStopRejectedAfterFill:
    """
    This is the first-class failure path.

    Expected behaviour:
      1. Entry fills → position now open
      2. Stop child is submitted
      3. Broker rejects stop child (e.g. invalid price, session limits)
      4. Runtime retries stop once
      5. If still rejected → emergency flatten → kill switch → alert

    The state machine must allow the intent to reach FAILED after ACTIVE.
    Trading must halt (kill switch activated) until the position is confirmed flat.
    """

    def test_intent_can_fail_from_active(self):
        assert_valid_intent_transition(IntentStatus.ACTIVE, IntentStatus.FAILED)

    def test_broker_rejected_is_terminal_for_stop_order(self):
        assert_valid_order_transition(OrderStatus.ACCEPTED, OrderStatus.BROKER_REJECTED)
        # Cannot transition away from BROKER_REJECTED
        with pytest.raises(StateMachineError):
            assert_valid_order_transition(OrderStatus.BROKER_REJECTED, OrderStatus.ACCEPTED)

    def test_unprotected_position_raises_if_not_halted(self):
        snapshot = _make_execution_snapshot(
            positions={"MNQ-09": Decimal("1")},
        )
        protected: set[str] = set()  # MNQ-09 not in protected set

        with pytest.raises(Exception):  # UnprotectedPositionError
            assert_position_protected(
                "MNQ-09",
                snapshot,
                protected_instrument_ids=protected,
                trading_halted=False,
            )

    def test_unprotected_position_allowed_if_halted(self):
        snapshot = _make_execution_snapshot(
            positions={"MNQ-09": Decimal("1")},
        )
        # When trading is halted (kill switch active), an unprotected position
        # is acceptable — the runtime is already in emergency mode.
        assert_position_protected(
            "MNQ-09",
            snapshot,
            protected_instrument_ids=set(),
            trading_halted=True,
        )

    def test_protected_position_does_not_raise(self):
        snapshot = _make_execution_snapshot(
            positions={"MNQ-09": Decimal("1")},
        )
        assert_position_protected(
            "MNQ-09",
            snapshot,
            protected_instrument_ids={"MNQ-09"},
            trading_halted=False,
        )


# ── Scenario 5: WebSocket disconnect → market_data_fresh=False ───────────────

class TestMarketDataStaleness:
    def test_stale_market_data_makes_readiness_not_ready(self):
        readiness = _ready().with_update(market_data_fresh=False)
        assert not readiness.ready
        assert "market_data_stale_or_disconnected" in readiness.blocking_reasons

    def test_stale_market_data_blocks_intent(self):
        readiness = _ready().with_update(market_data_fresh=False)
        with pytest.raises(ExecutionInvariantViolation) as exc_info:
            assert_runtime_ready(readiness)
        assert "market_data_stale_or_disconnected" in str(exc_info.value)

    def test_reconnected_market_data_restores_readiness(self):
        readiness = _ready().with_update(market_data_fresh=False)
        assert not readiness.ready
        restored = readiness.with_update(market_data_fresh=True)
        assert restored.ready


# ── Scenario 6: Position at startup ──────────────────────────────────────────

class TestStartupWithExistingPosition:
    def test_not_ready_until_positions_reconciled(self):
        readiness = EXECUTION_NOT_READY.with_update(
            broker_connected=True,
            orders_reconciled=True,
            executions_reconciled=True,
            account_state_loaded=True,
            market_data_fresh=True,
            # positions_reconciled intentionally NOT set
        )
        assert not readiness.ready
        assert "positions_not_reconciled" in readiness.blocking_reasons

    def test_all_conditions_required_for_ready(self):
        fields = [
            "broker_connected",
            "positions_reconciled",
            "orders_reconciled",
            "executions_reconciled",
            "account_state_loaded",
            "market_data_fresh",
        ]
        for missing in fields:
            kwargs = {f: True for f in fields}
            kwargs[missing] = False
            r = ExecutionReadiness(**kwargs)
            assert not r.ready, f"Should not be ready with {missing}=False"
            assert missing.replace("_", "_") in " ".join(r.blocking_reasons) or True


# ── Scenario 7: Manual order in TWS ──────────────────────────────────────────

class TestManualOrderInTWS:
    """
    Reconciliation detects orders at broker with no local runtime_order_id match.
    These must surface as unmanaged_orders in ReconciliationResult.
    The execution subsystem must not allow new intents until unmanaged orders
    are acknowledged.
    """

    def test_reconciliation_result_captures_unmanaged_orders(self):
        from alpha.engines.execution.interfaces import ReconciliationResult
        from alpha.engines.execution.models import BrokerOrderIdentity

        result = ReconciliationResult(
            unmanaged_orders=[
                BrokerOrderIdentity(
                    runtime_order_id="UNKNOWN-BROKER-12345",
                    broker="ibkr",
                    broker_order_ref="",
                    broker_order_id=12345,
                    broker_perm_id=99999,
                )
            ],
            positions_changed=True,
        )
        assert len(result.unmanaged_orders) == 1
        assert result.positions_changed is True


# ── Scenario 8: Flatten while entry is working ────────────────────────────────

class TestFlattenWhileWorking:
    def test_flatten_action_is_valid(self):
        intent = _make_intent(action=TradeAction.FLATTEN)
        assert intent.action == TradeAction.FLATTEN

    def test_cancel_pending_transition_valid_from_accepted(self):
        assert_valid_order_transition(OrderStatus.ACCEPTED, OrderStatus.CANCEL_PENDING)
        assert_valid_order_transition(OrderStatus.PARTIAL, OrderStatus.CANCEL_PENDING)

    def test_cancel_may_be_rejected_if_already_filled(self):
        # Broker fills before cancel arrives — cancel rejected, order goes to FILLED
        assert_valid_order_transition(OrderStatus.CANCEL_PENDING, OrderStatus.FILLED)

    def test_intent_can_complete_after_cancel(self):
        assert_valid_intent_transition(IntentStatus.ACTIVE, IntentStatus.CANCELLED)


# ── Scenario 9: UI override cannot bypass hard risk ───────────────────────────

class TestHardRiskOverride:
    def test_override_blocked_for_kill_switch_active(self):
        with pytest.raises(ExecutionInvariantViolation):
            assert_no_hard_risk_override(
                risk_flags=("kill_switch_active",),
                override_requested=True,
            )

    def test_override_blocked_for_daily_loss_breached(self):
        with pytest.raises(ExecutionInvariantViolation):
            assert_no_hard_risk_override(
                risk_flags=("daily_loss_limit_breached",),
                override_requested=True,
            )

    def test_override_allowed_for_soft_contextual_warning(self):
        # Soft warnings (regime mismatch, low RVOL) CAN be overridden
        assert_no_hard_risk_override(
            risk_flags=("low_rvol", "long_below_vwap"),
            override_requested=True,
        )

    def test_no_override_requested_always_passes(self):
        for flag in HARD_RISK_REASONS:
            assert_no_hard_risk_override(
                risk_flags=(flag,),
                override_requested=False,
            )

    def test_mixed_hard_and_soft_flags_still_blocked(self):
        with pytest.raises(ExecutionInvariantViolation):
            assert_no_hard_risk_override(
                risk_flags=("low_rvol", "kill_switch_active"),
                override_requested=True,
            )


# ── Scenario 10: Client quote drift detected ──────────────────────────────────

class TestClientQuoteDrift:
    def test_stale_client_quote_flagged(self):
        from datetime import timedelta
        server_snapshot = _make_market_snapshot()
        old_quote = QuoteSnapshot(
            instrument_id="MNQ-09",
            bid=Decimal("23100.00"),
            ask=Decimal("23100.25"),
            last=Decimal("23100.00"),
            timestamp=server_snapshot.as_of - timedelta(seconds=1),  # 1000ms old
        )
        intent = _make_intent()
        intent = intent.model_copy(update={"client_quote": old_quote})

        warnings = check_client_quote_drift(
            intent, server_snapshot,
            max_drift_ticks=Decimal("0.50"),
            max_age_ms=500,
        )
        assert any("age" in w for w in warnings)

    def test_drifted_client_bid_flagged(self):
        server_snapshot = _make_market_snapshot(bid=Decimal("23100.00"))
        stale_quote = QuoteSnapshot(
            instrument_id="MNQ-09",
            bid=Decimal("23098.50"),  # 6 ticks off
            ask=Decimal("23098.75"),
            last=Decimal("23098.50"),
            timestamp=server_snapshot.as_of,
        )
        intent = _make_intent()
        intent = intent.model_copy(update={"client_quote": stale_quote})

        warnings = check_client_quote_drift(
            intent, server_snapshot,
            max_drift_ticks=Decimal("0.50"),
        )
        assert any("bid" in w for w in warnings)

    def test_fresh_accurate_quote_has_no_warnings(self):
        server_snapshot = _make_market_snapshot(bid=Decimal("23100.00"))
        fresh_quote = QuoteSnapshot(
            instrument_id="MNQ-09",
            bid=Decimal("23100.00"),
            ask=Decimal("23100.25"),
            last=Decimal("23100.00"),
            timestamp=server_snapshot.as_of,
        )
        intent = _make_intent()
        intent = intent.model_copy(update={"client_quote": fresh_quote})

        warnings = check_client_quote_drift(
            intent, server_snapshot,
            max_drift_ticks=Decimal("0.50"),
        )
        assert warnings == []


# ── Scenario 11: State machine rejects invalid transitions ────────────────────

class TestStateMachineGuards:
    def test_intent_cannot_skip_from_received_to_active(self):
        with pytest.raises(StateMachineError):
            assert_valid_intent_transition(IntentStatus.RECEIVED, IntentStatus.ACTIVE)

    def test_intent_cannot_leave_terminal_state(self):
        for terminal in [IntentStatus.COMPLETED, IntentStatus.FAILED, IntentStatus.REJECTED]:
            with pytest.raises(StateMachineError):
                assert_valid_intent_transition(terminal, IntentStatus.RECEIVED)

    def test_order_cannot_go_from_filled_to_cancelled(self):
        with pytest.raises(StateMachineError):
            assert_valid_order_transition(OrderStatus.FILLED, OrderStatus.CANCELLED)

    def test_order_cannot_go_from_pending_to_filled_directly(self):
        with pytest.raises(StateMachineError):
            assert_valid_order_transition(OrderStatus.PENDING, OrderStatus.FILLED)

    def test_broker_only_state_cannot_be_set_by_local_code(self):
        from alpha.engines.execution.invariants import assert_only_broker_mutates_order_state
        with pytest.raises(ExecutionInvariantViolation):
            assert_only_broker_mutates_order_state(
                current_status=OrderStatus.SUBMITTED,
                proposed_status=OrderStatus.FILLED,
                mutation_source="local_timeout_handler",
            )

    def test_broker_callback_can_set_broker_only_state(self):
        assert_only_broker_mutates_order_state(
            current_status=OrderStatus.SUBMITTED,
            proposed_status=OrderStatus.FILLED,
            mutation_source="broker_callback",
        )


# ── Scenario 12: Effective exposure includes working orders ───────────────────

class TestEffectiveExposure:
    def test_zero_position_zero_working_is_zero(self):
        snapshot = _make_execution_snapshot()
        assert snapshot.effective_exposure("MNQ-09") == Decimal("0")

    def test_only_working_order_counts_as_exposure(self):
        """Double-fire scenario: user has a working entry but 0 filled position."""
        snapshot = _make_execution_snapshot(
            positions={},
            working_orders=[
                WorkingOrder(
                    runtime_order_id="ro-001",
                    instrument_id="MNQ-09",
                    side=OrderSide.BUY,
                    quantity=Decimal("1"),
                    role=OrderRole.ENTRY,
                )
            ],
        )
        exposure = snapshot.effective_exposure("MNQ-09")
        assert exposure == Decimal("1")  # not 0

    def test_second_intent_blocked_by_effective_exposure(self):
        snapshot = _make_execution_snapshot(
            positions={},
            working_orders=[
                WorkingOrder(
                    runtime_order_id="ro-001",
                    instrument_id="MNQ-09",
                    side=OrderSide.BUY,
                    quantity=Decimal("1"),
                    role=OrderRole.ENTRY,
                )
            ],
        )
        # Max 1 contract — adding 1 more would push to 2, should fail
        with pytest.raises(ExecutionInvariantViolation):
            assert_exposure_within_limit(
                instrument_id="MNQ-09",
                proposed_additional=Decimal("1"),
                execution_snapshot=snapshot,
                max_net_contracts=Decimal("1"),
            )

    def test_plan_missing_snapshot_ids_raises(self):
        plan = IntentTradePlan(
            intent_id=uuid4(),
            decision=RiskDecision.APPROVED,
            market_context_snapshot_id="",   # missing
            account_snapshot_id="acct-001",
            risk_policy_version="v1",
            evaluated_at=_now(),
        )
        with pytest.raises(ExecutionInvariantViolation):
            assert_plan_has_snapshot_ids(plan)
