"""
Shared pytest fixtures.

Fixtures here are available to all tests without explicit import.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from alpha.config.settings import AlphaSettings, RuntimeSettings
from alpha.core.clock import ReplayClock, WallClock
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.models.enums import (
    AssetClass,
    BarTimeframe,
    DataSourceId,
    RuntimeMode,
)
from alpha.models.events import BarEvent, EventMetadata
from alpha.models.symbol import Symbol


# ── Settings ──────────────────────────────────────────────────────────────────


@pytest.fixture
def test_settings() -> AlphaSettings:
    return AlphaSettings(
        runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["AAPL", "SPY"]),
    )


# ── Core infrastructure ───────────────────────────────────────────────────────


@pytest.fixture
def wall_clock() -> WallClock:
    return WallClock()


@pytest.fixture
def replay_clock() -> ReplayClock:
    return ReplayClock(
        start_time=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
        speed=100.0,
    )


@pytest.fixture
async def event_bus() -> EventBus:
    bus = EventBus()
    await bus.start()
    yield bus
    await bus.stop()


@pytest.fixture
def registry() -> SymbolRegistry:
    reg = SymbolRegistry()
    reg.register(Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.EQUITY))
    reg.register(Symbol(ticker="SPY", exchange="ARCA", asset_class=AssetClass.ETF))
    return reg


# ── Model factories ───────────────────────────────────────────────────────────


def make_bar_event(
    symbol: str = "AAPL",
    timestamp: datetime | None = None,
    open: float = 180.0,
    high: float = 181.0,
    low: float = 179.5,
    close: float = 180.5,
    volume: int = 100_000,
    timeframe: BarTimeframe = BarTimeframe.M1,
) -> BarEvent:
    ts = timestamp or datetime(2024, 1, 2, 14, 31, tzinfo=timezone.utc)
    return BarEvent(
        symbol=symbol,
        timestamp=ts,
        metadata=EventMetadata(
            source=DataSourceId.SYNTHETIC,
            received_at=datetime.now(timezone.utc),
            is_replay=True,
        ),
        timeframe=timeframe,
        open=Decimal(str(open)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=volume,
    )


@pytest.fixture
def sample_bar_event() -> BarEvent:
    return make_bar_event()
