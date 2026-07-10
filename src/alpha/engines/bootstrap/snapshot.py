"""
SnapshotMixin — runtime status serialization for BootstrapEngine.

All methods reference ``self`` which is the BootstrapEngine instance.
No __init__ — pure mixin.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from alpha.models.enums import EngineState, SetupState
from alpha.models.events import AnyEvent, SetupEvent
from alpha.runtime_status import write_snapshot

if TYPE_CHECKING:
    from alpha.core.engine import BaseEngine

logger = logging.getLogger(__name__)


def _write_snapshot_sync(settings: Any, payload: dict[str, Any]) -> None:
    """Run write_snapshot in a thread-executor — keeps the event loop free."""
    try:
        write_snapshot(settings, payload)
    except Exception:
        logging.getLogger(__name__).exception("Failed to write runtime snapshot")


class SnapshotMixin:
    """Mixin that provides runtime snapshot serialization to BootstrapEngine."""

    def _write_runtime_snapshot(self) -> None:
        try:
            write_snapshot(self._settings, self._build_runtime_snapshot())
        except Exception:
            logger.exception("Failed to write runtime snapshot")

    def _build_runtime_snapshot(self) -> dict[str, Any]:
        return {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "runtime_state": self.state,
            "mode": self._settings.runtime.mode,
            "symbols": self._settings.runtime.symbols,
            "engines": [self._serialize_engine(engine) for engine in self._engines],
            "quotes": self._serialize_quotes(),
            "bars": self._serialize_bars(),
            "contexts": self._startup_context,
            "market_states": self._serialize_market_states(),
            "setups": self._serialize_setups(),
            "thesis": self._serialize_thesis(),
            "setup_contexts": self._serialize_setup_contexts(),
            "prev_setup_contexts": self._serialize_prev_setup_contexts(),
            "orders": self._serialize_orders(),
            "risk": self._serialize_risk(),
            "pipeline": self._serialize_pipeline_debug(),
            "feed_quality": self._serialize_feed_quality(),
        }

    def _serialize_pipeline_debug(self) -> dict[str, Any]:
        """Per-symbol pipeline timestamps for UI staleness detection."""
        result: dict[str, Any] = {}
        for sym, out in self._last_pipeline_output.items():
            from alpha.models.events import PipelineOutputEvent
            if not isinstance(out, PipelineOutputEvent):
                continue
            ms = out.market_state
            thesis = out.thesis
            result[sym] = {
                "pipeline_ts": out.pipeline_ts.isoformat(),
                "bar_ts": out.timestamp.isoformat(),
                "snapshot_ts": out.bar_snapshot.timestamp.isoformat() if out.bar_snapshot and hasattr(out.bar_snapshot, "timestamp") else None,
                "market_state_ts": ms.timestamp.isoformat() if ms and hasattr(ms, "timestamp") else None,
                "thesis_type": str(thesis.dominant.thesis_type) if thesis else None,
                "flow_available": out.flow_context is not None,
                "active_setup_count": len(out.setups),
                "scored_setup_count": len(out.scored_setups),
            }
        return result

    def _serialize_feed_quality(self) -> dict[str, Any]:
        """Per-symbol data quality state from IngestionMonitor for the dashboard."""
        from alpha.engines.live.monitor import IngestionMonitor
        if not isinstance(self._ingestion_monitor, IngestionMonitor):
            return {}
        return self._ingestion_monitor.summary()

    def _serialize_engine(self, engine: "BaseEngine") -> dict[str, Any]:
        details = self._engine_details(engine)
        if engine.state == EngineState.RUNNING:
            health_status = "healthy"
        elif engine.state == EngineState.ERROR:
            health_status = "unhealthy"
        else:
            health_status = "degraded"
        return {
            "name": engine.name,
            "state": str(engine.state),
            "health": health_status,
            "details": details,
        }

    def _engine_details(self, engine: "BaseEngine") -> dict[str, Any]:
        if engine is self._storage:
            return {
                "queue_depth": self._storage._write_queue.qsize(),  # type: ignore[union-attr]
                "writes_total": self._storage._writes_total,  # type: ignore[union-attr]
            }
        if engine is self._historical:
            return {}
        if engine is self._live:
            return {
                "bars_received": self._live._bars_received,  # type: ignore[union-attr]
                "quotes_received": self._live._quotes_received,  # type: ignore[union-attr]
                "trades_received": self._live._trades_received,  # type: ignore[union-attr]
            }
        if engine is self._feature:
            return {
                "snapshots_emitted": self._feature._snapshots_emitted,  # type: ignore[union-attr]
            }
        if engine is self._market_state:
            return {
                "classifications_total": self._market_state._classifications_total,  # type: ignore[union-attr]
            }
        if engine is self._setup:
            return {
                "active_setups": sum(len(v) for v in self._setup._active.values()),  # type: ignore[union-attr]
                "detected_total": self._setup._setups_detected,  # type: ignore[union-attr]
                "triggered_total": self._setup._setups_triggered,  # type: ignore[union-attr]
            }
        if engine is self._scoring:
            return {}
        if engine is self._risk:
            return {}
        if engine is self._order:
            return {
                "orders_submitted": self._order._orders_submitted,  # type: ignore[union-attr]
                "orders_filled": self._order._orders_filled,  # type: ignore[union-attr]
                "open_orders": len(self._order.get_open_orders()),  # type: ignore[union-attr]
            }
        return {}

    def _serialize_quotes(self) -> dict[str, Any]:
        if self._live is None:
            return {}
        quotes = {}
        for symbol, event in self._live.latest_quotes().items():
            quotes[symbol] = event.model_dump(mode="json")
        return quotes

    def _serialize_bars(self) -> dict[str, Any]:
        if self._live is None:
            return {}
        completed = self._live.latest_bars()
        partial = self._live.latest_partial_bars()
        now = datetime.now(timezone.utc)
        current_minute = now.replace(second=0, microsecond=0)
        bars = {}
        for symbol in set(completed) | set(partial):
            completed_event = completed.get(symbol)
            partial_event = partial.get(symbol)
            if partial_event is not None:
                if partial_event.timestamp < current_minute:
                    partial_event = None
                elif (
                    completed_event is not None
                    and completed_event.timestamp >= partial_event.timestamp
                ):
                    # Completed exchange bar wins over trade-accumulated partial
                    # for the same minute — prevents wick flicker at bar close.
                    partial_event = None
            event = partial_event or completed_event
            if event:
                bars[symbol] = event.model_dump(mode="json")
        return bars

    def _serialize_market_states(self) -> dict[str, Any]:
        if self._market_state is None:
            return {}
        states = {}
        for symbol in self._settings.runtime.symbols:
            state = self._market_state.get_state(symbol)
            if state is not None:
                states[symbol] = state.model_dump(mode="json")
        return states

    async def _on_pipeline_output(self, event: AnyEvent) -> None:
        """Cache the latest PipelineOutputEvent per symbol for status.json."""
        from alpha.models.events import PipelineOutputEvent
        if not isinstance(event, PipelineOutputEvent):
            return
        self._last_pipeline_output[event.symbol] = event

    async def _on_setup_event(self, event: AnyEvent) -> None:
        """Cache terminal setups so they stay visible in status.json for _terminal_ttl bars."""
        if not isinstance(event, SetupEvent):
            return
        terminal_states = {SetupState.FAILED, SetupState.INVALIDATED, SetupState.EXPIRED}
        sym = event.symbol
        sid = str(event.setup_id)
        if event.setup_state in terminal_states:
            if sym not in self._terminal_setups:
                self._terminal_setups[sym] = {}
            # Build a minimal dict for the terminal entry
            self._terminal_setups[sym][sid] = (
                {
                    "setup_id": sid,
                    "symbol": sym,
                    "setup_type": str(event.setup_type),
                    "setup_state": str(event.setup_state),
                    "timestamp": event.timestamp.isoformat(),
                    "_terminal": True,
                },
                self._terminal_setup_bar_count,
            )
        else:
            # Non-terminal transition — remove from terminal cache if present
            if sym in self._terminal_setups:
                self._terminal_setups[sym].pop(sid, None)

    def _serialize_setups(self) -> list[dict[str, Any]]:
        if self._setup is None:
            return []
        setups: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        # Active (FORMING / CONFIRMED / TRIGGERED)
        for symbol in self._settings.runtime.symbols:
            for setup in self._setup.active_setups(symbol):
                d = setup.model_dump(mode="json")
                setups.append(d)
                seen_ids.add(str(setup.setup_id))

        # Terminal with TTL — keep for _terminal_ttl bars after transition
        self._terminal_setup_bar_count = getattr(self._setup, "_bar_counts", {}).get(
            self._settings.runtime.symbols[0] if self._settings.runtime.symbols else "", 0
        )
        for sym, cache in self._terminal_setups.items():
            expired_ids = []
            for sid, (setup_dict, bar_at_terminal) in cache.items():
                age = self._terminal_setup_bar_count - bar_at_terminal
                if age > self._terminal_ttl:
                    expired_ids.append(sid)
                elif sid not in seen_ids:
                    setups.append(setup_dict)
            for sid in expired_ids:
                cache.pop(sid, None)

        return setups

    def _serialize_thesis(self) -> dict[str, Any]:
        if self._thesis is None:
            return {}
        result: dict[str, Any] = {}
        for symbol in self._settings.runtime.symbols:
            active = self._thesis.get_thesis(symbol)
            if active is None:
                continue
            d = active.dominant
            # Derive risk_ratio from entry/stop/target if available
            risk_ratio: float | None = None
            if d.entry and d.stop and d.target:
                risk = abs(float(d.entry - d.stop))
                reward = abs(float(d.target - d.entry))
                risk_ratio = round(reward / risk, 2) if risk > 0 else None
            # Split evidence into positive/negative by weight sign
            ev_pos = [e.text for e in d.evidence if e.weight >= 0]
            ev_neg = [e.text for e in d.evidence if e.weight < 0]
            result[symbol] = {
                "thesis_id": str(d.thesis_id),
                "thesis_type": str(d.thesis_type),
                "state": str(d.state),
                "confidence": d.confidence,
                "bars_alive": d.bars_alive,
                "entry": str(d.entry) if d.entry else None,
                "stop": str(d.stop) if d.stop else None,
                "target": str(d.target) if d.target else None,
                "risk_ratio": risk_ratio,
                "key_level": str(d.key_level) if d.key_level else None,
                "sweep_low": str(d.sweep_low) if d.sweep_low else None,
                "evidence_positive": ev_pos,
                "evidence_negative": ev_neg,
                "commit_conditions": d.commit_conditions,
                "invalidation_conditions": d.invalidation_conditions,
                "invalidation_reason": d.invalidation_reason,
                "flip": {
                    "thesis_type": str(active.flip.thesis_type),
                    "state": str(active.flip.state),
                    "confidence": active.flip.confidence,
                } if active.flip else None,
            }
        return result

    def _serialize_orders(self) -> list[dict[str, Any]]:
        if self._order is None:
            return []
        return [order.model_dump(mode="json") for order in self._order.get_open_orders()]

    def _serialize_risk(self) -> dict[str, Any]:
        if self._risk is None:
            return {}
        result: dict[str, Any] = {}
        for account_id, ctx in self._risk._accounts.items():  # type: ignore[union-attr]
            s = ctx.state
            # All monetary values cast to float so the frontend receives numbers, not strings.
            # Pydantic serialises Decimal as string in JSON mode; we normalise here instead.
            result[account_id] = {
                "account_id": s.account_id,
                "account_type": str(s.account_type),
                "date": s.date,
                "realized_pnl": float(s.realized_pnl),
                "unrealized_pnl": float(s.unrealized_pnl),
                "session_high_pnl": float(s.session_high_pnl),
                "max_drawdown": float(s.max_drawdown),
                "daily_loss_limit": float(s.daily_loss_limit),
                "trades_taken": s.trades_taken,
                "open_positions": s.open_positions,
                "risk_consumed_pct": s.risk_consumed_pct,
                "net_liquidation": s.net_liquidation,
                "cash_balance": s.cash_balance,
                "gross_position_value": s.gross_position_value,
                "leverage_ratio": round(s.leverage_ratio, 3),
                "is_halted": s.is_halted,
                "halt_reason": str(s.halt_reason) if s.halt_reason else None,
                "halt_time": s.halt_time.isoformat() if s.halt_time else None,
                # Config fields the UI needs for threshold bars
                "account_size": float(ctx.config.account_size),
                "profit_protect_activation": float(ctx.config.profit_protect_activation),
                "profit_protect_giveback_pct": ctx.config.profit_protect_giveback_pct,
                "kill_switch_flatten": ctx.config.kill_switch_flatten,
            }
        return result

    def _serialize_setup_contexts(self) -> dict[str, Any]:
        if self._setup is None:
            return {}
        contexts: dict[str, Any] = {}
        for symbol in self._settings.runtime.symbols:
            context = self._setup.session_setup_context(symbol)
            if context is not None:
                contexts[symbol] = context.model_dump(mode="json")
        return contexts

    def _serialize_prev_setup_contexts(self) -> dict[str, Any]:
        if self._setup is None:
            return {}
        contexts: dict[str, Any] = {}
        for symbol in self._settings.runtime.symbols:
            context = self._setup.prev_session_setup_context(symbol)
            if context is not None:
                contexts[symbol] = context.model_dump(mode="json")
        return contexts

    def _log_runtime_summary(self) -> None:
        if self._live is None:
            return
        quote_chunks: list[str] = []
        for symbol in self._settings.runtime.symbols:
            quote = self._live.latest_quotes().get(symbol)
            bar = self._live.latest_bars().get(symbol)
            if quote is None and bar is None:
                continue
            close = f"{bar.close}" if bar is not None else "-"
            bid = f"{quote.bid_price}" if quote is not None else "-"
            ask = f"{quote.ask_price}" if quote is not None else "-"
            quote_chunks.append(f"{symbol} c={close} bid={bid} ask={ask}")
        if quote_chunks:
            logger.info("Runtime summary | %s", " | ".join(quote_chunks))
