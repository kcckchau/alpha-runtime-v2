"""Unit tests for the EventBus."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from alpha.core.event_bus import EventBus
from alpha.models.enums import DataSourceId, EventType
from alpha.models.events import BarEvent, EventMetadata
from tests.conftest import make_bar_event


@pytest.mark.asyncio
class TestEventBus:
    async def test_subscribe_and_receive(self) -> None:
        bus = EventBus()
        await bus.start()

        received: list[BarEvent] = []

        async def handler(event: BarEvent) -> None:
            received.append(event)

        bus.subscribe(EventType.BAR, handler)
        event = make_bar_event()
        await bus.publish(event)
        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert received[0].symbol == "AAPL"
        await bus.stop()

    async def test_symbol_filter(self) -> None:
        bus = EventBus()
        await bus.start()

        aapl_events: list[BarEvent] = []
        all_events: list[BarEvent] = []

        async def aapl_handler(e: BarEvent) -> None:
            aapl_events.append(e)

        async def all_handler(e: BarEvent) -> None:
            all_events.append(e)

        bus.subscribe(EventType.BAR, aapl_handler, symbol="AAPL")
        bus.subscribe(EventType.BAR, all_handler)  # no filter

        await bus.publish(make_bar_event("AAPL"))
        await bus.publish(make_bar_event("TSLA"))
        await asyncio.sleep(0.05)

        assert len(aapl_events) == 1
        assert len(all_events) == 2
        await bus.stop()

    async def test_unsubscribe(self) -> None:
        bus = EventBus()
        await bus.start()

        received: list[BarEvent] = []

        async def handler(e: BarEvent) -> None:
            received.append(e)

        sub = bus.subscribe(EventType.BAR, handler)
        await bus.publish(make_bar_event())
        await asyncio.sleep(0.05)
        assert len(received) == 1

        bus.unsubscribe(sub)
        await bus.publish(make_bar_event())
        await asyncio.sleep(0.05)
        assert len(received) == 1  # no new events

        await bus.stop()
