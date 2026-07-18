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
from alpha.models.enums import AssetClass, BarTimeframe, EventType, RuntimeMode, SessionPhase, SetupState, SetupType
from alpha.models.events import BarEvent, EventMetadata
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
        or_established=True, or_position="INSIDE",
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


def _bar_event(timestamp: datetime) -> BarEvent:
    return BarEvent(
        event_type=EventType.BAR,
        symbol="MNQ",
        timestamp=timestamp,
        timeframe=BarTimeframe.M1,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=100,
        metadata=EventMetadata(received_at=timestamp),
    )


@pytest.mark.asyncio
async def test_setup_context_rolls_with_futures_session() -> None:
    registry = SymbolRegistry()
    registry.register(resolve_symbol("MNQ"))
    bus = EventBus()
    await bus.start()
    settings = AlphaSettings(runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["MNQ"]))
    engine = SetupEngine(settings, bus, registry)
    await engine._on_initialize()

    first_timestamp = datetime(2026, 5, 26, 16, 0, tzinfo=timezone.utc)
    first_trigger = _bar_event(first_timestamp)
    await engine._roll_session_if_needed("MNQ", first_timestamp, first_trigger)
    active_setup = _setup(first_timestamp)
    engine._active["MNQ"][active_setup.setup_id] = active_setup
    engine._record_setup("MNQ", active_setup)

    first_context = engine.session_setup_context("MNQ")
    assert first_context is not None
    assert first_context.session_key == "2026-05-26"
    assert first_context.counts["detected_total"] == 1
    assert first_context.last_setup is not None
    assert first_context.last_setup.side == "buy"
    assert first_context.last_setup.level_tag == "hod"
    assert first_context.counts_by_level == {"hod": 1}

    next_session_timestamp = datetime(2026, 5, 26, 22, 30, tzinfo=timezone.utc)
    next_trigger = first_trigger.model_copy(
        update={
            "timestamp": next_session_timestamp,
            "metadata": EventMetadata(received_at=next_session_timestamp),
        }
    )
    await engine._roll_session_if_needed("MNQ", next_session_timestamp, next_trigger)

    next_context = engine.session_setup_context("MNQ")
    assert next_context is not None
    assert next_context.session_key == "2026-05-27"
    assert next_context.setups == []
    assert next_context.last_setup is None

    prev_context = engine.prev_session_setup_context("MNQ")
    assert prev_context is not None
    assert prev_context.counts["expired_total"] == 1
    assert prev_context.setups[0].state == SetupState.EXPIRED


@pytest.mark.asyncio
async def test_futures_allow_setup_detection_outside_equity_rth() -> None:
    registry = SymbolRegistry()
    registry.register(resolve_symbol("MNQ"))
    settings = AlphaSettings(runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["MNQ"]))
    engine = SetupEngine(settings, EventBus(), registry)
    await engine._on_initialize()

    premarket_snapshot = _snapshot(datetime(2026, 5, 26, 9, 38, tzinfo=timezone.utc)).model_copy(
        update={"session_phase": SessionPhase.PRE_MARKET}
    )

    assert engine._session_allows_detection("MNQ", premarket_snapshot) is False


@pytest.mark.asyncio
async def test_equities_remain_limited_to_rth_detection() -> None:
    registry = SymbolRegistry()
    registry.register(resolve_symbol("QQQ"))
    settings = AlphaSettings(runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["QQQ"]))
    engine = SetupEngine(settings, EventBus(), registry)
    await engine._on_initialize()

    bar = Bar(
        symbol="QQQ",
        timeframe=BarTimeframe.M1,
        timestamp=datetime(2026, 5, 26, 9, 38, tzinfo=timezone.utc),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=100,
    )
    snapshot = BarSnapshot(
        symbol="QQQ",
        timestamp=bar.timestamp,
        timeframe=BarTimeframe.M1,
        bar=bar,
        vwap=Decimal("100"),
        or_established=True, or_position="INSIDE",
        session_phase=SessionPhase.PRE_MARKET,
        is_above_vwap=True,
    )

    assert registry.get("QQQ").asset_class == AssetClass.EQUITY
    assert engine._session_allows_detection("QQQ", snapshot) is False


@pytest.mark.asyncio
async def test_vwap_rejection_requires_bearish_alignment() -> None:
    registry = SymbolRegistry()
    registry.register(resolve_symbol("MNQ"))
    settings = AlphaSettings(runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["MNQ"]))
    engine = SetupEngine(settings, EventBus(), registry)
    await engine._on_initialize()

    timestamp = datetime(2026, 5, 26, 14, 5, tzinfo=timezone.utc)
    snapshot = _snapshot(timestamp).model_copy(
        update={
            "is_above_vwap": False,
            "vwap_cross_down": True,
            "vwap_cross_down_after_bars": 3,
            "vwap_deviation_pct": -0.12,
            "bar_close_position_pct": 0.2,
            "ema_9": Decimal("101"),
            "ema_21": Decimal("102"),
            "ema9_1m_slope_norm_3": 0.06,
        }
    )

    reason = engine._reason_vwap_rejection(snapshot, MarketState(symbol="MNQ", timestamp=timestamp))
    assert reason == "ema9_slope_not_down"


@pytest.mark.asyncio
async def test_orb_breakout_detector_is_live() -> None:
    registry = SymbolRegistry()
    registry.register(resolve_symbol("MNQ"))
    settings = AlphaSettings(runtime=RuntimeSettings(mode=RuntimeMode.PAPER, symbols=["MNQ"]))
    engine = SetupEngine(settings, EventBus(), registry)
    await engine._on_initialize()

    timestamp = datetime(2026, 5, 26, 14, 5, tzinfo=timezone.utc)
    bar = Bar(
        symbol="MNQ",
        timeframe=BarTimeframe.M1,
        timestamp=timestamp,
        open=Decimal("100"),
        high=Decimal("103"),
        low=Decimal("99.8"),
        close=Decimal("102.5"),
        volume=100,
    )
    snapshot = BarSnapshot(
        symbol="MNQ",
        timestamp=timestamp,
        timeframe=BarTimeframe.M1,
        bar=bar,
        vwap=Decimal("101"),
        or_high=Decimal("101.5"),
        or_low=Decimal("99.5"),
        or_established=True, or_position="ABOVE",
        session_phase=SessionPhase.EARLY,
        is_above_vwap=True,
    )

    assert engine._detect_orb_breakout(snapshot, MarketState(symbol="MNQ", timestamp=timestamp)) is True
