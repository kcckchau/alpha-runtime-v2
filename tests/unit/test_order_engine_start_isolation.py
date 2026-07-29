"""
Unit tests for OrderEngine._on_start()'s handling of a broker adapter that
fails to connect.

Reproduces a bug found while diagnosing repeated live-process crashes: a live
run kept dying and restarting every few minutes. Root cause traced to
IBKRConnectionError propagating uncaught out of OrderEngine._on_start() ->
Engine.start() -> BootstrapEngine._on_start()'s plain `for engine in
self._engines: await engine.start()` loop -> main.py's
`asyncio.gather(_engine_task(), server.serve())` -> the whole process exits.
One broker (IBKR/execution, not yet live per CLAUDE.md Phase 3) being
unreachable killed Databento market-data ingestion and Telegram alerts too.

OrderEngine._on_stop() already treats adapter.disconnect() failures as
non-fatal (try/except + log), and _health_check() already reports DEGRADED
when no adapters are connected — the same tolerance was simply missing on
the connect side. These tests pin the fixed behavior: a failing adapter
leaves the engine running (degraded), it does not crash engine.start().
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from alpha.engines.order.adapters.base import BrokerAdapter
from alpha.engines.order.engine import OrderEngine
from alpha.models.enums import HealthStatus


class _FailingAdapter(BrokerAdapter):
    """Broker adapter whose connect() always fails, like IBKR with TWS down."""

    @property
    def source_id(self):
        from alpha.models.enums import DataSourceId
        return DataSourceId.INTERACTIVE_BROKERS

    @property
    def is_paper(self) -> bool:
        return False

    @property
    def is_connected(self) -> bool:
        return False

    async def connect(self) -> None:
        raise ConnectionRefusedError("Connect call failed ('127.0.0.1', 7497)")

    async def disconnect(self) -> None:
        pass

    async def submit_order(self, intent):
        raise NotImplementedError

    async def cancel_order(self, broker_order_id: str) -> bool:
        raise NotImplementedError

    async def get_order(self, broker_order_id: str):
        raise NotImplementedError

    async def get_open_orders(self):
        raise NotImplementedError

    async def get_account_equity(self, account_id: str = "default") -> float:
        raise NotImplementedError

    async def get_positions(self, account_id: str = "default"):
        raise NotImplementedError

    async def get_daily_pnl(self, account_id: str = "default"):
        raise NotImplementedError

    async def get_account_summary(self, account_id: str = "default"):
        raise NotImplementedError

    def on_order_update(self, handler) -> None:
        pass

    def on_execution(self, handler) -> None:
        pass


def _make_engine() -> OrderEngine:
    return OrderEngine(settings=MagicMock(), event_bus=MagicMock(), registry=MagicMock())


@pytest.mark.asyncio
class TestOrderEngineStartIsolation:
    async def test_failing_adapter_does_not_crash_engine_start(self) -> None:
        engine = _make_engine()
        engine.register_adapter(_FailingAdapter())

        await engine.initialize()
        await engine.start()  # must not raise

    async def test_health_check_reports_degraded_after_failed_connect(self) -> None:
        engine = _make_engine()
        engine.register_adapter(_FailingAdapter())

        await engine.initialize()
        await engine.start()

        health = await engine._health_check()
        assert health.status == HealthStatus.DEGRADED
        assert health.details["adapters_connected"] == []
