"""
Unit tests for ConnectionManager._broadcast()'s handling of slow/hung WebSocket clients.

Reproduces a bug found while diagnosing a live "chart not updating" report:
_broadcast() awaited each client's send_json() sequentially with no timeout, so
one hung client (dead connection, stalled browser tab, saturated network) could
stall delivery to every other client subscribed to the same symbol, and — since
ConnectionManager is a live-mode EventBus subscriber running on the same asyncio
loop as market-data ingestion — contribute to loop-wide contention.

These tests pin the fixed behavior: a hung client must not block delivery to
healthy clients, and must be dropped (and closed) within a bounded timeout.
"""
from __future__ import annotations

import asyncio

import pytest

from alpha.api.connection_manager import ConnectionManager


class _FakeWebSocket:
    def __init__(self, *, hang: bool = False) -> None:
        self.hang = hang
        self.sent: list[dict] = []
        self.closed = False

    async def send_json(self, payload: dict) -> None:
        if self.hang:
            await asyncio.sleep(3600)  # never completes within any test's own timeout
        self.sent.append(payload)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        self.closed = True


@pytest.mark.asyncio
class TestConnectionManagerBroadcast:
    async def test_hung_client_does_not_block_healthy_clients(self) -> None:
        manager = ConnectionManager()
        hung = _FakeWebSocket(hang=True)
        healthy = _FakeWebSocket()
        manager._connections["MNQ"].add(hung)
        manager._connections["MNQ"].add(healthy)

        # If the hung client isn't bounded, this call never returns and the
        # outer wait_for trips — proving the healthy client would be starved.
        await asyncio.wait_for(manager._broadcast("MNQ", {"type": "bar"}), timeout=5.0)

        assert healthy.sent == [{"type": "bar"}]

    async def test_hung_client_is_dropped_and_closed(self) -> None:
        manager = ConnectionManager()
        hung = _FakeWebSocket(hang=True)
        manager._connections["MNQ"].add(hung)

        await asyncio.wait_for(manager._broadcast("MNQ", {"type": "bar"}), timeout=5.0)

        assert hung not in manager._connections["MNQ"]
        assert hung.closed

    async def test_healthy_client_receives_payload_and_stays_connected(self) -> None:
        manager = ConnectionManager()
        ws = _FakeWebSocket()
        manager._connections["MNQ"].add(ws)

        await manager._broadcast("MNQ", {"type": "bar", "close": "100"})

        assert ws.sent == [{"type": "bar", "close": "100"}]
        assert ws in manager._connections["MNQ"]
        assert not ws.closed
