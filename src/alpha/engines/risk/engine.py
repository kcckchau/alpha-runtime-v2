"""
Engine 8 — Risk Engine

Responsibilities:
  - Subscribe to SetupEvent (TRIGGERED state)
  - Route each setup to the correct account based on setup type
  - Validate setup against per-account daily risk limits before generating a plan
  - Compute position size based on entry/stop distance and account risk %
  - Calculate stop, target, and risk/reward ratio
  - Track per-account P&L via OrderUpdateEvent fills
  - Trigger kill switch when either:
      1. Daily loss floor is breached (hard floor)
      2. Giveback from session high-water mark exceeds profit-protect threshold

Input:  SetupEvent (TRIGGERED), OrderUpdateEvent (FILLED)
Output: TradePlan → forwarded to OrderEngine (direct call)

The Risk Engine is the last gate before an order is generated.
If it rejects a setup, no order is ever created.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from alpha.config.settings import AlphaSettings
from alpha.core.engine import BaseEngine, EngineHealth
from alpha.core.event_bus import EventBus
from alpha.models.enums import (
    AccountType,
    EventType,
    HealthStatus,
    KillSwitchReason,
    OrderSide,
    OrderStatus,
    SetupState,
)
from alpha.models.events import AnyEvent, OrderUpdateEvent, SetupEvent
from alpha.models.risk import AccountConfig, DailyRiskState, TradePlan
from alpha.models.setup import Setup

logger = logging.getLogger(__name__)


class RiskViolation(Exception):
    pass


# ── Per-symbol position tracking for P&L computation ─────────────────────────

@dataclass
class _PositionEntry:
    """Tracks the cost basis of an open position."""
    qty: int
    avg_price: Decimal
    side: OrderSide  # ENTRY side: BUY = long, SELL = short


@dataclass
class _AccountRiskContext:
    """Bundles account config + live state + position book."""
    config: AccountConfig
    state: DailyRiskState
    positions: dict[str, _PositionEntry] = field(default_factory=dict)


# ── Engine ────────────────────────────────────────────────────────────────────

class RiskEngine(BaseEngine):
    """
    Validates setups against per-account risk rules and produces sized TradePlans.
    Tracks fills to maintain live P&L and triggers kill switch when limits are hit.
    """

    # How often to sync P&L from the broker (seconds)
    _BROKER_SYNC_INTERVAL = 30

    def __init__(self, settings: AlphaSettings, event_bus: EventBus) -> None:
        super().__init__()
        self._settings = settings
        self._event_bus = event_bus
        self._setup_engine: object | None = None
        self._order_engine: object | None = None
        self._broker_adapter: object | None = None
        # account_id → context
        self._accounts: dict[str, _AccountRiskContext] = {}
        self._plans_generated: int = 0
        self._plans_rejected: int = 0
        self._sync_task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return "RiskEngine"

    def set_setup_engine(self, engine: object) -> None:
        self._setup_engine = engine

    def set_order_engine(self, engine: object) -> None:
        self._order_engine = engine

    def set_broker_adapter(self, adapter: object) -> None:
        """Wire the broker adapter so P&L can be polled directly."""
        self._broker_adapter = adapter

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        self._build_account_contexts()

    async def _on_start(self) -> None:
        self._event_bus.subscribe(EventType.SETUP, self._handle_setup)
        self._event_bus.subscribe(EventType.ORDER_UPDATE, self._handle_order_update)
        # Best-effort seed on startup; don't let a connection delay block the engine.
        try:
            await self._sync_broker_pnl()
        except Exception:
            logger.warning("Initial broker P&L seed failed — will retry in sync loop", exc_info=True)
        self._sync_task = asyncio.create_task(self._broker_sync_loop())

    async def _on_stop(self) -> None:
        if self._sync_task:
            self._sync_task.cancel()

    async def _health_check(self) -> EngineHealth:
        halted_accounts = [
            aid for aid, ctx in self._accounts.items() if ctx.state.is_halted
        ]
        sync_task_dead = (
            self._sync_task is not None and self._sync_task.done()
        )
        if sync_task_dead:
            exc = self._sync_task.exception() if not self._sync_task.cancelled() else None
            logger.error("Broker sync task has died: %s", exc)
        status = (
            HealthStatus.DEGRADED
            if (halted_accounts or sync_task_dead)
            else HealthStatus.HEALTHY
        )
        return EngineHealth(
            status,
            self.name,
            {
                "plans_generated": self._plans_generated,
                "plans_rejected": self._plans_rejected,
                "halted_accounts": halted_accounts,
                "broker_sync_alive": not sync_task_dead,
                "account_states": {
                    aid: {
                        "type": ctx.config.account_type,
                        "realized_pnl": float(ctx.state.realized_pnl),
                        "session_high_pnl": float(ctx.state.session_high_pnl),
                        "daily_loss_limit": float(ctx.state.daily_loss_limit),
                        "is_halted": ctx.state.is_halted,
                        "halt_reason": ctx.state.halt_reason,
                    }
                    for aid, ctx in self._accounts.items()
                },
            },
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def daily_state(self, account_id: str = "default") -> DailyRiskState | None:
        ctx = self._accounts.get(account_id)
        return ctx.state if ctx else None

    def all_daily_states(self) -> dict[str, DailyRiskState]:
        return {aid: ctx.state for aid, ctx in self._accounts.items()}

    @property
    def is_halted(self) -> bool:
        """True if ALL configured accounts are halted."""
        return bool(self._accounts) and all(
            ctx.state.is_halted for ctx in self._accounts.values()
        )

    def is_account_halted(self, account_id: str) -> bool:
        ctx = self._accounts.get(account_id)
        return ctx.state.is_halted if ctx else False

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _handle_setup(self, event: AnyEvent) -> None:
        if not isinstance(event, SetupEvent):
            return
        if event.setup_state != SetupState.TRIGGERED:
            return

        setup = self._get_setup(event.symbol, event.setup_id)
        if setup is None:
            return

        account_id = self._settings.risk.default_account_id
        ctx = self._accounts.get(account_id)
        if ctx is None:
            logger.warning(
                "No account configured for id=%s — rejecting %s",
                account_id, event.setup_id,
            )
            self._plans_rejected += 1
            return

        if ctx.state.is_halted:
            logger.warning(
                "Kill switch active on %s (%s) — rejecting setup %s",
                ctx.config.account_id, ctx.state.halt_reason, event.setup_id,
            )
            self._plans_rejected += 1
            return

        try:
            plan = self._evaluate(setup, ctx)
            self._plans_generated += 1
            logger.info(
                "TradePlan generated: %s %s size=%d R:R=%.2f account=%s",
                plan.symbol, plan.side, plan.position_size,
                plan.risk_reward_ratio, plan.account_id,
            )
            await self._forward_plan(plan)
        except RiskViolation as exc:
            self._plans_rejected += 1
            logger.info("Setup rejected by risk: %s — %s", event.setup_id, exc)

    async def _handle_order_update(self, event: AnyEvent) -> None:
        if not isinstance(event, OrderUpdateEvent):
            return
        if event.order_status != OrderStatus.FILLED:
            return
        if event.avg_fill_price is None or event.side is None:
            return

        ctx = self._accounts.get(event.account_id)
        if ctx is None:
            return

        realized_delta = self._apply_fill(
            ctx,
            symbol=event.symbol,
            side=event.side,
            qty=event.filled_quantity,
            fill_price=event.avg_fill_price,
        )

        if realized_delta != Decimal("0"):
            ctx.state.realized_pnl += realized_delta
            ctx.state.trades_taken += 1
            ctx.state.risk_consumed_pct = float(
                abs(ctx.state.realized_pnl) / ctx.state.daily_loss_limit
            ) if ctx.state.daily_loss_limit else 0.0
            logger.debug(
                "P&L update [%s]: realized_delta=%s total_realized=%s",
                event.account_id, realized_delta, ctx.state.realized_pnl,
            )

        # Update high-water mark
        current_pnl = ctx.state.current_pnl
        if current_pnl > ctx.state.session_high_pnl:
            ctx.state.session_high_pnl = current_pnl

        # Check kill switch triggers
        reason = self._check_kill_switch(ctx)
        if reason is not None and not ctx.state.is_halted:
            await self._trigger_halt(ctx, reason)

    # ── Broker P&L sync ───────────────────────────────────────────────────────

    async def _broker_sync_loop(self) -> None:
        consecutive_errors = 0
        while True:
            await asyncio.sleep(self._BROKER_SYNC_INTERVAL)
            try:
                self._check_day_rollover()
                await self._sync_broker_pnl()
                consecutive_errors = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                consecutive_errors += 1
                logger.exception(
                    "Broker sync loop error (consecutive=%d) — will retry in %ds",
                    consecutive_errors, self._BROKER_SYNC_INTERVAL,
                )
                # Back off progressively but cap at 5 minutes so we recover quickly
                # once the connection is restored.
                backoff = min(300, self._BROKER_SYNC_INTERVAL * consecutive_errors)
                await asyncio.sleep(backoff)

    def _check_day_rollover(self) -> None:
        """Reset each account's daily state when the calendar date changes.

        The engine may run continuously across midnight; without this reset the
        daily loss limit, trade count, and kill-switch state would carry over
        into the next trading session.
        """
        today = datetime.now(timezone.utc).date().isoformat()
        for account_id, ctx in self._accounts.items():
            if ctx.state.date == today:
                continue
            logger.info(
                "Day rollover detected for account %s (%s → %s) — resetting daily state",
                account_id, ctx.state.date, today,
            )
            ctx.state.date = today
            ctx.state.realized_pnl = Decimal("0")
            ctx.state.unrealized_pnl = Decimal("0")
            ctx.state.session_high_pnl = Decimal("0")
            ctx.state.max_drawdown = Decimal("0")
            ctx.state.trades_taken = 0
            ctx.state.open_positions = 0
            ctx.state.risk_consumed_pct = 0.0
            ctx.state.is_halted = False
            ctx.state.halt_reason = None
            ctx.state.halt_time = None
            # Recompute limit in case account_size changed between sessions
            ctx.state.daily_loss_limit = ctx.config.account_size * Decimal(
                str(ctx.config.max_daily_loss_pct)
            )
            # Positions are stale from yesterday; clear the internal book.
            # The next broker sync will repopulate P&L from fresh broker data.
            ctx.positions.clear()

    async def _sync_broker_pnl(self) -> None:
        """
        Pull realized + unrealized P&L directly from the broker for each account.
        This ensures positions opened manually or before the engine started are
        reflected correctly, without depending on fill events flowing through us.
        """
        from alpha.engines.order.adapters.base import BrokerAdapter
        adapter = self._broker_adapter
        if not isinstance(adapter, BrokerAdapter):
            return
        if not adapter.is_connected:
            return

        for account_id, ctx in self._accounts.items():
            try:
                realized, unrealized = await adapter.get_daily_pnl(account_id)
                ctx.state.realized_pnl = Decimal(str(round(realized, 2)))
                ctx.state.unrealized_pnl = Decimal(str(round(unrealized, 2)))
                ctx.state.risk_consumed_pct = float(
                    abs(ctx.state.realized_pnl) / ctx.state.daily_loss_limit
                ) if ctx.state.daily_loss_limit else 0.0

                # Keep high-water mark updated
                current_pnl = ctx.state.current_pnl
                if current_pnl > ctx.state.session_high_pnl:
                    ctx.state.session_high_pnl = current_pnl

                # Pull account equity / cash / leverage
                summary = await adapter.get_account_summary(account_id)
                ctx.state.net_liquidation = summary.get("net_liquidation", 0.0)
                ctx.state.cash_balance = summary.get("cash_balance", 0.0)
                ctx.state.gross_position_value = summary.get("gross_position_value", 0.0)
                ctx.state.leverage_ratio = (
                    ctx.state.gross_position_value / ctx.state.net_liquidation
                    if ctx.state.net_liquidation > 0 else 0.0
                )

                logger.debug(
                    "Broker sync [%s]: realized=%s unrealized=%s nlv=%s cash=%s gpv=%s lev=%.2fx",
                    account_id,
                    ctx.state.realized_pnl, ctx.state.unrealized_pnl,
                    ctx.state.net_liquidation, ctx.state.cash_balance,
                    ctx.state.gross_position_value, ctx.state.leverage_ratio,
                )

                # Check kill switch after each sync
                reason = self._check_kill_switch(ctx)
                if reason is not None and not ctx.state.is_halted:
                    await self._trigger_halt(ctx, reason)

            except Exception:
                logger.warning(
                    "Broker P&L sync failed for account %s", account_id, exc_info=True
                )

    # ── Position book + P&L ───────────────────────────────────────────────────

    def _apply_fill(
        self,
        ctx: _AccountRiskContext,
        symbol: str,
        side: OrderSide,
        qty: int,
        fill_price: Decimal,
    ) -> Decimal:
        """
        Update the position book and return the realized P&L delta for this fill.
        Returns Decimal("0") for pure entry fills.
        """
        pos = ctx.positions.get(symbol)

        if pos is None:
            # Opening a new position
            ctx.positions[symbol] = _PositionEntry(qty=qty, avg_price=fill_price, side=side)
            ctx.state.open_positions += 1
            return Decimal("0")

        # Determine if this fill is closing/reducing an existing position
        is_closing = (pos.side == OrderSide.BUY and side == OrderSide.SELL) or \
                     (pos.side == OrderSide.SELL and side == OrderSide.BUY)

        if is_closing:
            close_qty = min(qty, pos.qty)
            if pos.side == OrderSide.BUY:
                realized = (fill_price - pos.avg_price) * close_qty
            else:
                realized = (pos.avg_price - fill_price) * close_qty

            remaining = pos.qty - close_qty
            if remaining <= 0:
                del ctx.positions[symbol]
                ctx.state.open_positions = max(0, ctx.state.open_positions - 1)
            else:
                ctx.positions[symbol] = _PositionEntry(
                    qty=remaining, avg_price=pos.avg_price, side=pos.side
                )
            return realized

        # Adding to existing position — update average price
        total_cost = pos.avg_price * pos.qty + fill_price * qty
        new_qty = pos.qty + qty
        ctx.positions[symbol] = _PositionEntry(
            qty=new_qty,
            avg_price=total_cost / new_qty,
            side=pos.side,
        )
        return Decimal("0")

    # ── Kill switch ───────────────────────────────────────────────────────────

    def _check_kill_switch(self, ctx: _AccountRiskContext) -> KillSwitchReason | None:
        """
        Returns a KillSwitchReason if either trigger is met, else None.

        Trigger 1 — Hard loss floor:
          current_pnl <= -(daily_loss_limit)

        Trigger 2 — Profit protection (trailing stop on daily P&L):
          Only activates once session_high_pnl >= profit_protect_activation.
          Halts when giveback from peak >= peak * profit_protect_giveback_pct.
        """
        current_pnl = ctx.state.current_pnl

        # Trigger 1: hard floor
        if current_pnl <= -ctx.state.daily_loss_limit:
            return KillSwitchReason.DAILY_LOSS_LIMIT

        # Trigger 2: profit protection
        cfg = ctx.config
        if ctx.state.session_high_pnl >= cfg.profit_protect_activation:
            giveback = ctx.state.session_high_pnl - current_pnl
            max_giveback = ctx.state.session_high_pnl * Decimal(
                str(cfg.profit_protect_giveback_pct)
            )
            if giveback >= max_giveback:
                return KillSwitchReason.PROFIT_PROTECTION

        return None

    async def _trigger_halt(
        self, ctx: _AccountRiskContext, reason: KillSwitchReason
    ) -> None:
        ctx.state.is_halted = True
        ctx.state.halt_reason = reason
        ctx.state.halt_time = datetime.now(timezone.utc)
        logger.warning(
            "KILL SWITCH TRIGGERED — account=%s reason=%s "
            "realized_pnl=%s session_high=%s daily_limit=%s",
            ctx.config.account_id, reason,
            ctx.state.realized_pnl,
            ctx.state.session_high_pnl,
            ctx.state.daily_loss_limit,
        )
        if ctx.config.kill_switch_flatten:
            await self._flatten_account(ctx)

    async def _flatten_account(self, ctx: _AccountRiskContext) -> None:
        """Cancel open orders and close all positions for this account."""
        from alpha.engines.order.engine import OrderEngine
        if not isinstance(self._order_engine, OrderEngine):
            return

        # Cancel all open orders for this account
        for order in self._order_engine.get_open_orders():
            if order.account_id == ctx.config.account_id and order.broker_order_id:
                cancelled = await self._order_engine.cancel_order(order.order_id)
                if cancelled:
                    logger.info("Cancelled order %s during flatten", order.order_id)

        # Close all tracked positions with market orders
        for symbol, pos in list(ctx.positions.items()):
            close_side = OrderSide.SELL if pos.side == OrderSide.BUY else OrderSide.BUY
            await self._order_engine.submit_close_order(
                account_id=ctx.config.account_id,
                symbol=symbol,
                side=close_side,
                quantity=pos.qty,
            )
            logger.info(
                "Flatten: submitted close order %s %s qty=%d account=%s",
                close_side, symbol, pos.qty, ctx.config.account_id,
            )

    # ── Plan generation ───────────────────────────────────────────────────────

    def _evaluate(self, setup: Setup, ctx: _AccountRiskContext) -> TradePlan:
        cfg = ctx.config

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

        self._check_daily_limits(ctx, dollar_risk)

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
            account_id=cfg.account_id,
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

    def _check_daily_limits(self, ctx: _AccountRiskContext, new_risk: Decimal) -> None:
        remaining = ctx.state.remaining_risk
        if new_risk > remaining:
            raise RiskViolation(
                f"Would exceed daily loss limit: need {new_risk}, have {remaining}"
            )
        if ctx.state.open_positions >= ctx.config.max_open_positions:
            raise RiskViolation(
                f"Max open positions ({ctx.config.max_open_positions}) reached"
            )

    @staticmethod
    def _infer_side(setup: Setup) -> OrderSide:
        from alpha.models.enums import SetupType
        bullish = {
            SetupType.VWAP_RECLAIM, SetupType.ORB_BREAKOUT,
            SetupType.SWEEP_RECLAIM, SetupType.FAKE_BREAKDOWN,
        }
        return OrderSide.BUY if setup.setup_type in bullish else OrderSide.SELL

    # ── Account management ────────────────────────────────────────────────────

    def _build_account_contexts(self) -> None:
        from alpha.models.risk import AccountConfig as AC

        cfg = self._settings.risk
        today = datetime.now(timezone.utc).date().isoformat()

        raw_accounts: list[AccountConfig] = [
            a if isinstance(a, AC) else AC(**a)
            for a in cfg.accounts
        ]

        if not raw_accounts:
            # Backward compat: build one account from scalar fields
            raw_accounts = [
                AC(
                    account_id=cfg.default_account_id,
                    account_type=AccountType.DAY,
                    account_size=cfg.account_size,
                    max_daily_loss_pct=cfg.max_daily_loss_pct,
                    max_position_risk_pct=cfg.max_position_risk_pct,
                    max_open_positions=cfg.max_open_positions,
                )
            ]

        for account_cfg in raw_accounts:
            state = DailyRiskState(
                date=today,
                account_id=account_cfg.account_id,
                account_type=account_cfg.account_type,
                daily_loss_limit=account_cfg.account_size * Decimal(
                    str(account_cfg.max_daily_loss_pct)
                ),
            )
            self._accounts[account_cfg.account_id] = _AccountRiskContext(
                config=account_cfg,
                state=state,
            )
            logger.info(
                "Account configured: id=%s type=%s size=%s loss_limit=%s "
                "profit_protect_activation=%s giveback_pct=%.0f%%",
                account_cfg.account_id, account_cfg.account_type,
                account_cfg.account_size, state.daily_loss_limit,
                account_cfg.profit_protect_activation,
                account_cfg.profit_protect_giveback_pct * 100,
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
