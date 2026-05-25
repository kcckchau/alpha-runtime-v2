"""
Engine 8 — Risk Engine

Responsibilities:
  - Subscribe to SetupEvent (TRIGGERED state)
  - Validate setup against daily risk limits before generating a plan
  - Compute position size based on entry/stop distance and account risk %
  - Calculate stop, target, and risk/reward ratio
  - Emit a TradePlan if the risk check passes
  - Track daily P&L and halt trading if daily loss limit is hit

Input:  SetupEvent (TRIGGERED)
Output: TradePlan → forwarded to OrderEngine (not via EventBus — direct call
        or optional signal event)

The Risk Engine is the last gate before an order is generated.
If it rejects a setup, no order is ever created.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.models.enums import EventType, HealthStatus, OrderSide, SetupState
from alpha.models.events import AnyEvent, SetupEvent
from alpha.models.risk import DailyRiskState, TradePlan
from alpha.models.setup import Setup

logger = logging.getLogger(__name__)


class RiskViolation(Exception):
    pass


class RiskEngine(BaseEngine):
    """
    Validates setups against risk rules and produces sized TradePlans.
    """

    def __init__(self, settings: AlphaSettings, event_bus: EventBus) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._setup_engine: object | None = None
        self._order_engine: object | None = None
        self._daily_state: DailyRiskState | None = None
        self._plans_generated: int = 0
        self._plans_rejected: int = 0

    @property
    def name(self) -> str:
        return "RiskEngine"

    def set_setup_engine(self, engine: object) -> None:
        self._setup_engine = engine

    def set_order_engine(self, engine: object) -> None:
        self._order_engine = engine

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        self._reset_daily_state()

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.SETUP, self._handle_setup)
        self._event_bus.subscribe(EventType.ORDER_UPDATE, self._handle_order_update)

    async def _on_stop(self) -> None:
        pass

    async def _health_check(self) -> EngineHealth:
        halted = self._daily_state.is_halted if self._daily_state else False
        return EngineHealth(
            HealthStatus.DEGRADED if halted else HealthStatus.HEALTHY,
            self.name,
            {
                "plans_generated": self._plans_generated,
                "plans_rejected": self._plans_rejected,
                "daily_halted": halted,
            },
        )

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def daily_state(self) -> DailyRiskState | None:
        return self._daily_state

    @property
    def is_halted(self) -> bool:
        return self._daily_state.is_halted if self._daily_state else False

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _handle_setup(self, event: AnyEvent) -> None:
        if not isinstance(event, SetupEvent):
            return
        if event.setup_state != SetupState.TRIGGERED:
            return
        if self.is_halted:
            logger.warning("Risk halt active — rejecting setup %s", event.setup_id)
            return

        setup = self._get_setup(event.symbol, event.setup_id)
        if setup is None:
            return

        try:
            plan = self._evaluate(setup)
            self._plans_generated += 1
            logger.info(
                "TradePlan generated: %s %s size=%d R:R=%.2f",
                plan.symbol, plan.side, plan.position_size, plan.risk_reward_ratio,
            )
            await self._forward_plan(plan)
        except RiskViolation as exc:
            self._plans_rejected += 1
            logger.info("Setup rejected by risk: %s — %s", event.setup_id, exc)

    async def _handle_order_update(self, event: AnyEvent) -> None:
        # TODO: update realized P&L on fills
        pass

    # ── Plan generation ───────────────────────────────────────────────────────

    def _evaluate(self, setup: Setup) -> TradePlan:
        snap = setup.bar_snapshot
        cfg = self._settings.risk

        entry = setup.entry_trigger
        stop = setup.stop_reference
        target = setup.target_reference

        if entry is None or stop is None or target is None:
            raise RiskViolation("Setup missing entry/stop/target levels")

        side = self._infer_side(setup)
        risk_per_share = abs(entry - stop)
        if risk_per_share == Decimal("0"):
            raise RiskViolation("Zero risk per share")

        dollar_risk = cfg.account_size * Decimal(str(cfg.max_position_risk_pct))
        size = int(dollar_risk / risk_per_share)
        if size < 1:
            raise RiskViolation("Position size rounds to 0")

        self._check_daily_limits(dollar_risk)

        reward = abs(target - entry) * size
        risk = risk_per_share * size
        rr = float(reward / risk) if risk else 0.0

        if rr < 2.0:
            raise RiskViolation(f"Risk/Reward {rr:.2f} below minimum 2.0")

        return TradePlan(
            plan_id=uuid4(),
            setup_id=setup.setup_id,
            symbol=setup.symbol,
            side=side,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            position_size=size,
            risk_amount=risk,
            reward_amount=reward,
            risk_reward_ratio=rr,
            account_size=cfg.account_size,
            account_risk_pct=float(dollar_risk / cfg.account_size),
            created_at=datetime.now(timezone.utc),
        )

    def _check_daily_limits(self, new_risk: Decimal) -> None:
        if self._daily_state is None:
            return
        remaining = self._daily_state.remaining_risk
        if new_risk > remaining:
            raise RiskViolation(
                f"Would exceed daily loss limit: need {new_risk}, have {remaining}"
            )
        max_positions = self._settings.risk.max_open_positions
        if self._daily_state.open_positions >= max_positions:
            raise RiskViolation(
                f"Max open positions ({max_positions}) reached"
            )

    @staticmethod
    def _infer_side(setup: Setup) -> OrderSide:
        from alpha.models.enums import SetupType
        bullish = {
            SetupType.VWAP_RECLAIM, SetupType.ORB_BREAKOUT,
            SetupType.SWEEP_RECLAIM, SetupType.FAKE_BREAKDOWN,
        }
        return OrderSide.BUY if setup.setup_type in bullish else OrderSide.SELL

    def _reset_daily_state(self) -> None:
        today = datetime.now(timezone.utc).date().isoformat()
        cfg = self._settings.risk
        self._daily_state = DailyRiskState(
            date=today,
            daily_loss_limit=cfg.account_size * Decimal(str(cfg.max_daily_loss_pct)),
        )

    def _get_setup(self, symbol: str, setup_id: object) -> Setup | None:
        from alpha.engines.setup.engine import SetupEngine
        if isinstance(self._setup_engine, SetupEngine):
            for s in self._setup_engine.active_setups(symbol):
                if s.setup_id == setup_id:
                    return s
        return None

    async def _forward_plan(self, plan: TradePlan) -> None:
        from alpha.engines.order.engine import OrderEngine
        if isinstance(self._order_engine, OrderEngine):
            await self._order_engine.submit_plan(plan)
