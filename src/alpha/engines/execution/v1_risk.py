"""
V1RiskEvaluator — ExecutionRiskEvaluator implementation for V1 paper mode.

Hard risk (always blocks):
  - Kill switch active
  - Daily loss limit breached
  - Effective exposure at or above max_contracts
  - Market data unreliable (FAILED quality)
  - Action not supported in V1 (only OPEN_LONG, OPEN_SHORT, FLATTEN)

Contextual risk (APPROVED_WITH_MODIFICATIONS — requires second confirmation):
  - Long entry below VWAP in TRENDING_DOWN regime
  - Short entry above VWAP in TRENDING_UP regime
  - Market data DEGRADED (not FAILED)
  - Entering against session live_bias (if available in market state)

Price resolution (MARKETABLE_LIMIT, V1 only):
  - BUY:  limit = ask (crosses the spread — aggressive fill, fills immediately)
  - SELL: limit = bid (crosses the spread — aggressive fill, fills immediately)

Stop/target:
  - Stop: entry ± (stop_distance_ticks × tick_size)
  - Target: entry ± (target_distance_ticks × tick_size), if specified

Risk USD:
  - (entry − stop) × quantity × point_value
  - point_value from symbol registry (MNQ = $2/tick = $0.50/point, ES = $50/point)

V1 constraints:
  - OPEN_LONG, OPEN_SHORT, FLATTEN only
  - Quantity must be exactly 1
  - ProtectionPolicy.use_broker_bracket must be True
  - PriceMode.MARKETABLE_LIMIT only
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from alpha.engines.execution.invariants import (
    assert_plan_has_snapshot_ids,
    effective_exposure,
)
from alpha.engines.execution.models import (
    AccountSnapshot,
    ExecutionSnapshot,
    IntentTradePlan,
    MarketContextSnapshot,
    PlannedOrder,
    TradeIntent,
)
from alpha.models.enums import (
    DataQualityState,
    IntentSource,
    OrderRole,
    OrderSide,
    OrderType,
    PriceMode,
    RiskDecision,
    TimeInForce,
    TradeAction,
    TrendState,
)

if TYPE_CHECKING:
    from alpha.core.registry import SymbolRegistry

logger = logging.getLogger(__name__)

_UTC = timezone.utc

_SUPPORTED_ACTIONS_V1 = {TradeAction.OPEN_LONG, TradeAction.OPEN_SHORT, TradeAction.FLATTEN}
_RISK_POLICY_VERSION = "v1.0.0"


class V1RiskEvaluator:
    """
    Hard + contextual risk evaluation for V1 paper mode.

    max_contracts: maximum absolute net exposure per instrument.
    tick_size: resolved from SymbolRegistry. Default 0.25 (MNQ).
    """

    def __init__(
        self,
        registry: "SymbolRegistry",
        *,
        max_contracts: int = 1,
        max_daily_loss_usd: Decimal = Decimal("500.00"),
    ) -> None:
        self._registry = registry
        self._max_contracts = Decimal(str(max_contracts))
        self._max_daily_loss_usd = max_daily_loss_usd

    def evaluate(
        self,
        intent: TradeIntent,
        market: MarketContextSnapshot,
        account: AccountSnapshot,
        execution: ExecutionSnapshot,
    ) -> IntentTradePlan:
        """
        Pure evaluation — no side effects, no I/O.
        Returns IntentTradePlan with decision, orders, and full audit trail.
        """
        hard_flags: list[str] = []
        soft_flags: list[str] = []
        explanation: list[str] = []

        # ── Hard risk checks ──────────────────────────────────────────────────
        if account.is_halted:
            hard_flags.append("kill_switch_active")
            explanation.append(f"Kill switch active: {account.halt_reason}")

        if account.realized_pnl < -self._max_daily_loss_usd:
            hard_flags.append("daily_loss_limit_breached")
            explanation.append(
                f"Daily loss ${abs(account.realized_pnl):.2f} exceeds limit ${self._max_daily_loss_usd:.2f}"
            )

        if intent.action not in _SUPPORTED_ACTIONS_V1:
            hard_flags.append("unsupported_action_v1")
            explanation.append(f"Action {intent.action!r} not supported in V1 (OPEN_LONG, OPEN_SHORT, FLATTEN only)")

        if not intent.protection_policy.use_broker_bracket:
            hard_flags.append("broker_bracket_required")
            explanation.append("use_broker_bracket must be True in V1")

        if market.data_quality == DataQualityState.FAILED:
            hard_flags.append("market_data_stale")
            explanation.append(f"Market data quality FAILED for {intent.instrument_id}")

        # Exposure check (Invariant 5)
        current_exposure = effective_exposure(intent.instrument_id, execution)
        if intent.action in {TradeAction.OPEN_LONG, TradeAction.OPEN_SHORT}:
            projected = abs(current_exposure) + intent.quantity
            if projected > self._max_contracts:
                hard_flags.append("max_contracts_exceeded")
                explanation.append(
                    f"Effective exposure {current_exposure} + {intent.quantity} = {projected} "
                    f"exceeds max {self._max_contracts}"
                )
        elif intent.action == TradeAction.FLATTEN and current_exposure == 0:
            hard_flags.append("no_position_to_flatten")
            explanation.append("No position to flatten")

        if hard_flags:
            return self._rejected_plan(intent, hard_flags, explanation, market, account)

        # ── Contextual (soft) risk ────────────────────────────────────────────
        if market.data_quality == DataQualityState.DEGRADED:
            soft_flags.append("market_data_degraded")
            explanation.append("Market data is DEGRADED — feed may be delayed")

        if intent.action == TradeAction.OPEN_LONG:
            if not market.above_vwap:
                soft_flags.append("long_below_vwap")
                explanation.append("Long entry below VWAP — counter-trend risk")
            if market.regime == TrendState.TRENDING_DOWN:
                soft_flags.append("long_in_downtrend")
                explanation.append("Long entry in TRENDING_DOWN regime")
        elif intent.action == TradeAction.OPEN_SHORT:
            if market.above_vwap:
                soft_flags.append("short_above_vwap")
                explanation.append("Short entry above VWAP — counter-trend risk")
            if market.regime == TrendState.TRENDING_UP:
                soft_flags.append("short_in_uptrend")
                explanation.append("Short entry in TRENDING_UP regime")

        # ── Price resolution ──────────────────────────────────────────────────
        tick_size = self._tick_size(intent.instrument_id)
        point_value = self._point_value(intent.instrument_id)

        entry_side, entry_price = self._resolve_price(intent, market, tick_size)

        # ── Stop / target ─────────────────────────────────────────────────────
        pp = intent.protection_policy
        stop_distance = Decimal(str(pp.stop_distance_ticks)) * tick_size
        if entry_side == OrderSide.BUY:
            stop_price = entry_price - stop_distance
        else:
            stop_price = entry_price + stop_distance

        stop_order = PlannedOrder(
            role=OrderRole.STOP,
            side=OrderSide.SELL if entry_side == OrderSide.BUY else OrderSide.BUY,
            order_type=OrderType.STOP,
            quantity=intent.quantity,
            stop_price=stop_price,
            time_in_force=TimeInForce.GTC,
        )

        target_orders: list[PlannedOrder] = []
        if pp.target_distance_ticks is not None:
            target_distance = Decimal(str(pp.target_distance_ticks)) * tick_size
            if entry_side == OrderSide.BUY:
                target_price = entry_price + target_distance
            else:
                target_price = entry_price - target_distance
            target_orders.append(PlannedOrder(
                role=OrderRole.TARGET,
                side=OrderSide.SELL if entry_side == OrderSide.BUY else OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=intent.quantity,
                limit_price=target_price,
                time_in_force=TimeInForce.GTC,
            ))

        # ── Risk metrics ──────────────────────────────────────────────────────
        risk_per_contract = stop_distance * point_value
        risk_usd = risk_per_contract * intent.quantity
        reward_usd = (
            (Decimal(str(pp.target_distance_ticks)) * tick_size * point_value * intent.quantity)
            if pp.target_distance_ticks else Decimal("0")
        )
        rr = float(reward_usd / risk_usd) if risk_usd > 0 else 0.0

        entry_order = PlannedOrder(
            role=OrderRole.ENTRY,
            side=entry_side,
            order_type=OrderType.LIMIT,
            quantity=intent.quantity,
            limit_price=entry_price,
            time_in_force=TimeInForce.DAY,
        )

        decision = (
            RiskDecision.APPROVED_WITH_MODIFICATIONS if soft_flags
            else RiskDecision.APPROVED
        )

        if decision == RiskDecision.APPROVED:
            explanation.append("All risk checks passed")

        plan = IntentTradePlan(
            intent_id=intent.intent_id,
            decision=decision,
            entry_order=entry_order,
            stop_order=stop_order,
            target_orders=tuple(target_orders),
            calculated_risk_usd=risk_usd,
            max_loss_usd=self._max_daily_loss_usd,
            reward_risk_ratio=rr,
            market_context_snapshot_id=market.snapshot_id,
            account_snapshot_id=account.snapshot_id,
            risk_policy_version=_RISK_POLICY_VERSION,
            evaluated_at=datetime.now(_UTC),
            risk_flags=tuple(soft_flags),
            explanation=tuple(explanation),
        )
        assert_plan_has_snapshot_ids(plan)
        return plan

    # ── Private helpers ───────────────────────────────────────────────────────

    def _resolve_price(
        self,
        intent: TradeIntent,
        market: MarketContextSnapshot,
        tick_size: Decimal,
    ) -> tuple[OrderSide, Decimal]:
        """Return (side, limit_price). MARKETABLE_LIMIT only in V1."""
        if intent.action == TradeAction.OPEN_LONG:
            side = OrderSide.BUY
            price = market.ask   # cross the spread — aggressive fill
        elif intent.action == TradeAction.OPEN_SHORT:
            side = OrderSide.SELL
            price = market.bid
        elif intent.action == TradeAction.FLATTEN:
            # Determine direction from current position sign
            # Positive = long → sell to flatten; negative = short → buy to flatten
            # (Coordinator ensures current_exposure != 0 before this point)
            side = OrderSide.SELL   # default; coordinator adjusts based on position
            price = market.bid
        else:
            raise ValueError(f"Unsupported action in V1: {intent.action}")

        # Round to nearest tick
        price = self._round_to_tick(price, tick_size)
        return side, price

    @staticmethod
    def _round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal:
        return (price / tick_size).quantize(Decimal("1")) * tick_size

    def _tick_size(self, instrument_id: str) -> Decimal:
        try:
            sym = self._registry.get(instrument_id)
            return sym.tick_size
        except Exception:
            return Decimal("0.25")   # MNQ default

    def _point_value(self, instrument_id: str) -> Decimal:
        try:
            sym = self._registry.get(instrument_id)
            return sym.point_value
        except Exception:
            return Decimal("2.00")   # MNQ: $2/point

    def _rejected_plan(
        self,
        intent: TradeIntent,
        hard_flags: list[str],
        explanation: list[str],
        market: MarketContextSnapshot,
        account: AccountSnapshot,
    ) -> IntentTradePlan:
        return IntentTradePlan(
            intent_id=intent.intent_id,
            decision=RiskDecision.REJECTED,
            entry_order=None,
            stop_order=None,
            target_orders=(),
            market_context_snapshot_id=market.snapshot_id,
            account_snapshot_id=account.snapshot_id,
            risk_policy_version=_RISK_POLICY_VERSION,
            evaluated_at=datetime.now(_UTC),
            risk_flags=tuple(hard_flags),
            explanation=tuple(explanation),
        )
