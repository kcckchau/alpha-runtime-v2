from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from alpha.config.settings import AlphaSettings, RuntimeSettings
from alpha.core.event_bus import EventBus
from alpha.core.registry import SymbolRegistry
from alpha.engines.setup.engine import SetupEngine
from alpha.instruments import resolve_symbol
from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, ORBState, RuntimeMode, SessionPhase, SetupType
from alpha.models.market_state import MarketState
from alpha.models.setup import Setup
from alpha.models.snapshot import BarSnapshot


def _snapshot(timestamp: datetime) -> BarSnapshot:
    bar = Bar(
        symbol="MNQ",
        timeframe=BarTimeframe.M1,
        timestamp=timestamp,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=100,
    )
    return BarSnapshot(
        symbol="MNQ",
        timestamp=timestamp,
        timeframe=BarTimeframe.M1,
        bar=bar,
        vwap=Decimal("100"),
        orb_state=ORBState.INSIDE,
        session_phase=SessionPhase.EARLY,
        is_above_vwap=True,
    )


def _setup(timestamp: datetime) -> Setup:
    snapshot = _snapshot(timestamp)
    return Setup(
        symbol="MNQ",
        setup_type=SetupType.HOD_BREAKOUT,
        detected_at=timestamp,
        updated_at=timestamp,
        market_state=MarketState(symbol="MNQ", timestamp=timestamp),
        bar_snapshot=snapshot,
    )


@pytest.mark.asyncio
async def test_setup_context_rolls_with_futures_session() -> None:
    registry = SymbolRegistry()
    registry.register(resolve_symbol("MNQ"))
    settings = AlphaSettings(runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["MNQ"]))
    engine = SetupEngine(settings, EventBus(), registry)
    await engine._on_initialize()

    first_timestamp = datetime(2026, 5, 26, 16, 0, tzinfo=timezone.utc)
    engine._roll_session_if_needed("MNQ", first_timestamp)
    engine._record_setup("MNQ", _setup(first_timestamp))

    first_context = engine.session_setup_context("MNQ")
    assert first_context is not None
    assert first_context.session_key == "2026-05-26"
    assert first_context.counts["detected_total"] == 1

    next_session_timestamp = datetime(2026, 5, 26, 22, 30, tzinfo=timezone.utc)
    engine._roll_session_if_needed("MNQ", next_session_timestamp)

    next_context = engine.session_setup_context("MNQ")
    assert next_context is not None
    assert next_context.session_key == "2026-05-27"
    assert next_context.setups == []
    assert next_context.last_setup is None
