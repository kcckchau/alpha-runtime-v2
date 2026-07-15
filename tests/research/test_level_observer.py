"""
Tests for Phase 0 shadow research pipeline.

All tests are deterministic, network-free, and do not start the full engine.
Fixtures build minimal PipelineOutputEvent objects with controlled BarSnapshot values.

Coverage:
  - Geometry: above/below/overlap/cross
  - PriceSide.ON exact-zero semantics
  - VWAP non-tick-aligned distance rounding
  - Missing ORH/ORL graceful skip
  - VWAP drift: each bar preserves the value at its own timestamp
  - Duplicate bar idempotency via deterministic obs_id
  - Session reset refreshes ATR reference path
  - Writer buffer/flush mechanics
  - Writer IO error: retain buffer, no crash
  - Data quality propagation
  - Replay determinism (content equality across two runs)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
import pyarrow.parquet as pq

from alpha.models.bar import Bar
from alpha.models.enums import BarTimeframe, DataSourceId, ORBState, SessionPhase
from alpha.models.events import EventMetadata, PipelineOutputEvent
from alpha.models.snapshot import BarSnapshot
from alpha.research.level_observer import (
    LevelObserver,
    RunMode,
    _distance_ticks,
    _price_side,
    _proximity_band,
)
from alpha.research.models import LevelBarObservation, PriceSide, ReferenceLevelType, make_obs_id
from alpha.research.parquet_writer import LevelObservationWriter


# ── Helpers ───────────────────────────────────────────────────────────────────

TICK = Decimal("0.25")
RUN_ID = "replay-20260101-aabbccdd"
SYM = "MNQ"
SESSION_DATE = "2026-01-02"

_TS_BASE = datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)  # 09:30 ET


def _ts(offset_minutes: int = 0) -> datetime:
    from datetime import timedelta
    return _TS_BASE + timedelta(minutes=offset_minutes)


def _make_bar(
    open: float,
    high: float,
    low: float,
    close: float,
    ts: datetime | None = None,
) -> Bar:
    return Bar(
        symbol=SYM,
        timeframe=BarTimeframe.M1,
        timestamp=ts or _TS_BASE,
        open=Decimal(str(open)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=1000,
    )


def _make_snap(
    bar: Bar,
    vwap: float | None = 5812.00,
    orb_high: float | None = None,
    orb_low: float | None = None,
    atr_14: float | None = 10.0,
    session_phase: SessionPhase = SessionPhase.EARLY,
    orb_state: ORBState = ORBState.NOT_SET,
) -> BarSnapshot:
    return BarSnapshot(
        symbol=SYM,
        timestamp=bar.timestamp,
        timeframe=BarTimeframe.M1,
        bar=bar,
        vwap=Decimal(str(vwap)) if vwap is not None else Decimal("0"),
        orb_high=Decimal(str(orb_high)) if orb_high is not None else None,
        orb_low=Decimal(str(orb_low)) if orb_low is not None else None,
        atr_14=Decimal(str(atr_14)) if atr_14 is not None else None,
        session_phase=session_phase,
        orb_state=orb_state,
    )


def _make_event(snap: BarSnapshot, ts: datetime | None = None) -> PipelineOutputEvent:
    return PipelineOutputEvent(
        symbol=SYM,
        timestamp=ts or snap.timestamp,
        pipeline_ts=datetime.now(timezone.utc),
        bar_snapshot=snap,
        flow_context=None,
        market_state=None,
        thesis=None,
        setups=[],
        scored_setups=[],
        metadata=EventMetadata(
            source=DataSourceId.SYNTHETIC,
            received_at=datetime.now(timezone.utc),
            is_replay=True,
        ),
    )


def _make_observer(tmp_path: Path, run_mode: RunMode = RunMode.REPLAY) -> tuple[LevelObserver, LevelObservationWriter]:
    writer = LevelObservationWriter(research_root=tmp_path, run_id=RUN_ID, max_buffer_rows=500)
    bus = MagicMock()
    registry = MagicMock()
    sym_obj = MagicMock()
    sym_obj.tick_size = TICK
    registry.get.return_value = sym_obj

    # Patch calendar to return fixed session info
    with patch("alpha.research.level_observer.calendar_for_symbol") as mock_cal:
        cal = MagicMock()
        cal.session_key.return_value = SESSION_DATE
        cal.session_date.return_value = type("d", (), {"isoformat": lambda s: SESSION_DATE})()
        mock_cal.return_value = cal

        observer = LevelObserver(
            event_bus=bus,
            registry=registry,
            writer=writer,
            run_mode=run_mode,
        )
        observer._run_id = RUN_ID
        observer._producer_version = "test"
        # Pre-seed session key so reset_session isn't triggered on first bar
        observer._last_session_key[SYM] = SESSION_DATE
    return observer, writer


async def _process(observer: LevelObserver, event: PipelineOutputEvent) -> None:
    with patch("alpha.research.level_observer.calendar_for_symbol") as mock_cal:
        cal = MagicMock()
        cal.session_key.return_value = SESSION_DATE
        cal.session_date.return_value = type("d", (), {"isoformat": lambda s: SESSION_DATE})()
        mock_cal.return_value = cal
        await observer._process(event)


# ── Unit tests: geometry helpers ───────────────────────────────────────────────

def test_price_side_above():
    assert _price_side(4) == PriceSide.ABOVE

def test_price_side_below():
    assert _price_side(-3) == PriceSide.BELOW

def test_price_side_on():
    """PriceSide.ON is only when distance is exactly zero."""
    assert _price_side(0) == PriceSide.ON

def test_price_side_plus_one_is_above_not_on():
    """AT semantics (within 1 tick) must NOT appear in the schema."""
    assert _price_side(1) == PriceSide.ABOVE

def test_distance_ticks_exact_above():
    price = Decimal("5813.00")
    level = Decimal("5812.00")
    assert _distance_ticks(price, level, TICK, vwap=False) == 4

def test_distance_ticks_exact_below():
    price = Decimal("5811.00")
    level = Decimal("5812.00")
    assert _distance_ticks(price, level, TICK, vwap=False) == -4

def test_distance_ticks_vwap_rounds_half_up():
    """VWAP is not tick-aligned; distance must use ROUND_HALF_UP."""
    # price = 5813.00, vwap = 5811.625 → raw = 1.375 / 0.25 = 5.5 → rounds to 6
    price = Decimal("5813.00")
    vwap_level = Decimal("5811.625")
    result = _distance_ticks(price, vwap_level, TICK, vwap=True)
    assert result == 6

def test_distance_ticks_vwap_negative_rounds_half_up():
    # price = 5810.00, vwap = 5811.625 → raw = -1.625 / 0.25 = -6.5
    # ROUND_HALF_UP rounds away from zero → -7
    price = Decimal("5810.00")
    vwap_level = Decimal("5811.625")
    result = _distance_ticks(price, vwap_level, TICK, vwap=True)
    assert result == -7

def test_proximity_band_atr_available():
    atr = Decimal("10.0")   # 10 points / 0.25 = 40 ticks; 20% = 8
    assert _proximity_band(atr, TICK) == 8

def test_proximity_band_atr_none_uses_fallback():
    assert _proximity_band(None, TICK) == 10


# ── Integration tests: observation content ────────────────────────────────────

@pytest.mark.asyncio
async def test_basic_observation_above_level(tmp_path):
    """Bar entirely above ORL produces correct positive tick distances."""
    bar = _make_bar(open=5815.00, high=5817.00, low=5814.00, close=5816.00)
    snap = _make_snap(bar, vwap=None, orb_low=5812.00, orb_state=ORBState.INSIDE)
    event = _make_event(snap)

    observer, writer = _make_observer(tmp_path)
    await _process(observer, event)

    assert writer.pending_count == 1
    key = (SYM, SESSION_DATE)
    row = writer._pending[key][0]
    # ORL = 5812.00; bar entirely above
    assert row["level_type"] == "orl"
    assert row["open_distance_ticks"] == 12    # (5815 - 5812) / 0.25 = 12
    assert row["high_distance_ticks"] == 20
    assert row["low_distance_ticks"] == 8
    assert row["close_distance_ticks"] == 16
    assert row["bar_overlaps_level"] is False
    assert row["open_side"] == "above"
    assert row["close_side"] == "above"


@pytest.mark.asyncio
async def test_overlap_detection(tmp_path):
    """Bar where low < level < high produces bar_overlaps_level=True."""
    bar = _make_bar(open=5813.00, high=5814.00, low=5810.00, close=5813.50)
    snap = _make_snap(bar, vwap=None, orb_low=5812.00, orb_state=ORBState.INSIDE)
    event = _make_event(snap)

    observer, writer = _make_observer(tmp_path)
    await _process(observer, event)

    row = writer._pending[(SYM, SESSION_DATE)][0]
    assert row["bar_overlaps_level"] is True
    assert row["low_distance_ticks"] == -8   # (5810 - 5812) / 0.25 = -8
    assert row["high_distance_ticks"] == 8
    assert row["open_side"] == "above"
    assert row["close_side"] == "above"


@pytest.mark.asyncio
async def test_cross_close_above_open_below(tmp_path):
    """open_side=BELOW, close_side=ABOVE captured correctly."""
    bar = _make_bar(open=5810.00, high=5815.00, low=5809.00, close=5813.00)
    snap = _make_snap(bar, vwap=None, orb_low=5812.00, orb_state=ORBState.INSIDE)
    event = _make_event(snap)

    observer, writer = _make_observer(tmp_path)
    await _process(observer, event)

    row = writer._pending[(SYM, SESSION_DATE)][0]
    assert row["open_side"] == "below"
    assert row["close_side"] == "above"
    assert row["bar_overlaps_level"] is True


@pytest.mark.asyncio
async def test_vwap_uses_value_at_bar_time(tmp_path):
    """Each bar preserves the VWAP value at its own timestamp."""
    bar1 = _make_bar(open=5813.00, high=5815.00, low=5812.00, close=5814.00, ts=_ts(0))
    bar2 = _make_bar(open=5816.00, high=5818.00, low=5815.00, close=5817.00, ts=_ts(45))

    snap1 = _make_snap(bar1, vwap=5810.00, orb_high=None, orb_low=None)
    snap2 = _make_snap(bar2, vwap=5815.50, orb_high=None, orb_low=None)

    observer, writer = _make_observer(tmp_path)
    await _process(observer, _make_event(snap1, ts=_ts(0)))
    await _process(observer, _make_event(snap2, ts=_ts(45)))

    rows = writer._pending[(SYM, SESSION_DATE)]
    assert len(rows) == 2
    # Decimal(str(float)) preserves float repr: str(5810.00) == "5810.0", str(5815.50) == "5815.5"
    assert rows[0]["level_value"] == "5810.0"
    assert rows[1]["level_value"] == "5815.5"


@pytest.mark.asyncio
async def test_missing_orh_skips_gracefully(tmp_path):
    """When ORH is None (opening range not locked), no ORH row is emitted."""
    bar = _make_bar(open=5813.00, high=5815.00, low=5812.00, close=5814.00)
    # orb_high=None, orb_low=None, orb_state=NOT_SET
    snap = _make_snap(bar, vwap=5812.00, orb_high=None, orb_low=None)
    event = _make_event(snap)

    observer, writer = _make_observer(tmp_path)
    await _process(observer, event)

    # Only VWAP row
    assert writer.pending_count == 1
    row = writer._pending[(SYM, SESSION_DATE)][0]
    assert row["level_type"] == "vwap"


@pytest.mark.asyncio
async def test_all_three_levels_emits_three_rows(tmp_path):
    """When VWAP + ORH + ORL are all available, exactly 3 rows are emitted per bar."""
    bar = _make_bar(open=5813.00, high=5820.00, low=5810.00, close=5816.00)
    snap = _make_snap(
        bar, vwap=5812.00, orb_high=5818.00, orb_low=5808.00,
        orb_state=ORBState.INSIDE,
    )
    event = _make_event(snap)

    observer, writer = _make_observer(tmp_path)
    await _process(observer, event)

    assert writer.pending_count == 3
    types = {r["level_type"] for r in writer._pending[(SYM, SESSION_DATE)]}
    assert types == {"vwap", "orh", "orl"}


@pytest.mark.asyncio
async def test_duplicate_bar_idempotent(tmp_path):
    """Feeding the same bar twice produces one set of rows, not two."""
    bar = _make_bar(open=5813.00, high=5815.00, low=5812.00, close=5814.00)
    snap = _make_snap(bar, vwap=5812.00, orb_high=5818.00, orb_low=5808.00,
                      orb_state=ORBState.INSIDE)
    event = _make_event(snap)

    observer, writer = _make_observer(tmp_path)
    await _process(observer, event)
    await _process(observer, event)  # identical bar delivered twice

    assert writer.pending_count == 3  # not 6


@pytest.mark.asyncio
async def test_replay_determinism(tmp_path):
    """Two observers with the same run_id produce identical obs_ids for identical bars."""
    bar = _make_bar(open=5813.00, high=5815.00, low=5812.00, close=5814.00)
    snap = _make_snap(bar, vwap=5812.00)
    event = _make_event(snap)

    obs_a, writer_a = _make_observer(tmp_path / "a")
    obs_b, writer_b = _make_observer(tmp_path / "b")

    await _process(obs_a, event)
    await _process(obs_b, event)

    rows_a = writer_a._pending[(SYM, SESSION_DATE)]
    rows_b = writer_b._pending[(SYM, SESSION_DATE)]

    assert len(rows_a) == len(rows_b)
    for ra, rb in zip(rows_a, rows_b):
        assert ra["obs_id"] == rb["obs_id"]
        assert ra["level_value"] == rb["level_value"]
        assert ra["close_distance_ticks"] == rb["close_distance_ticks"]


def test_writer_buffers_and_flushes(tmp_path):
    """499 rows stay buffered; 500th triggers auto-flush and file appears on disk."""
    writer = LevelObservationWriter(research_root=tmp_path, run_id=RUN_ID, max_buffer_rows=500)

    def _obs(n: int) -> LevelBarObservation:
        # Use unique timestamps: minutes 0–59 in hour 14, then minutes 0–59 in hour 15, etc.
        ts = datetime(2026, 1, 2, 14 + n // 60, n % 60, tzinfo=timezone.utc)
        return LevelBarObservation(
            obs_id=make_obs_id(RUN_ID, SYM, ts, ReferenceLevelType.VWAP),
            run_id=RUN_ID,
            run_mode=RunMode.REPLAY,
            source_bar_id=str(uuid4()),
            symbol=SYM,
            session_date=SESSION_DATE,
            bar_timestamp=ts,
            level_type=ReferenceLevelType.VWAP,
            level_value=Decimal("5812.00"),
            open_distance_ticks=4,
            high_distance_ticks=8,
            low_distance_ticks=0,
            close_distance_ticks=4,
            bar_overlaps_level=True,
            open_side=PriceSide.ABOVE,
            close_side=PriceSide.ABOVE,
            proximity_band_ticks=8,
            volatility_reference=Decimal("10.00"),
            volatility_definition_version="atr14_m1_v1",
            tick_size=TICK,
            session_phase="early",
            orb_state="not_set",
            producer_version="test",
            data_quality="clean",
        )

    for i in range(499):
        writer.add(_obs(i))

    assert writer.pending_count == 499
    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 0, "No flush should happen before 500 rows"

    writer.add(_obs(499))  # triggers auto-flush

    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 1
    assert writer.pending_count == 0


def test_writer_no_crash_on_io_error(tmp_path):
    """Writer retains buffer and does not raise when disk write fails."""
    writer = LevelObservationWriter(research_root=tmp_path, run_id=RUN_ID, max_buffer_rows=500)

    ts = _TS_BASE
    obs = LevelBarObservation(
        obs_id=make_obs_id(RUN_ID, SYM, ts, ReferenceLevelType.VWAP),
        run_id=RUN_ID,
        run_mode=RunMode.REPLAY,
        source_bar_id=None,
        symbol=SYM,
        session_date=SESSION_DATE,
        bar_timestamp=ts,
        level_type=ReferenceLevelType.VWAP,
        level_value=Decimal("5812.00"),
        open_distance_ticks=4,
        high_distance_ticks=8,
        low_distance_ticks=0,
        close_distance_ticks=4,
        bar_overlaps_level=True,
        open_side=PriceSide.ABOVE,
        close_side=PriceSide.ABOVE,
        proximity_band_ticks=8,
        volatility_reference=Decimal("10.00"),
        volatility_definition_version="atr14_m1_v1",
        tick_size=TICK,
        session_phase="early",
        orb_state="not_set",
        producer_version="test",
        data_quality="clean",
    )
    writer.add(obs)
    assert writer.pending_count == 1

    with patch("alpha.research.parquet_writer.pq.write_table", side_effect=OSError("disk full")):
        writer.flush()   # must not raise

    assert writer.pending_count == 1   # row retained
    assert writer.flush_errors == 1    # public property


def test_writer_part_files_use_run_id_in_name(tmp_path):
    """Part files embed run_id so restarts never collide."""
    writer = LevelObservationWriter(research_root=tmp_path, run_id="test-run", max_buffer_rows=2)

    for i in range(2):
        ts = datetime(2026, 1, 2, 14, i, tzinfo=timezone.utc)
        obs = LevelBarObservation(
            obs_id=make_obs_id("test-run", SYM, ts, ReferenceLevelType.VWAP),
            run_id="test-run",
            run_mode=RunMode.REPLAY,
            source_bar_id=None,
            symbol=SYM,
            session_date=SESSION_DATE,
            bar_timestamp=ts,
            level_type=ReferenceLevelType.VWAP,
            level_value=Decimal("5812.00"),
            open_distance_ticks=4, high_distance_ticks=8,
            low_distance_ticks=0, close_distance_ticks=4,
            bar_overlaps_level=True,
            open_side=PriceSide.ABOVE, close_side=PriceSide.ABOVE,
            proximity_band_ticks=8,
            volatility_reference=Decimal("10.00"),
            volatility_definition_version="atr14_m1_v1",
            tick_size=TICK,
            session_phase="early", orb_state="not_set",
            producer_version="test", data_quality="clean",
        )
        writer.add(obs)

    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 1
    assert "test-run" in parquet_files[0].name


@pytest.mark.asyncio
async def test_data_quality_field_propagates(tmp_path):
    """data_quality field reflects the IngestionMonitor state at time of observation."""
    bar = _make_bar(open=5813.00, high=5815.00, low=5812.00, close=5814.00)
    snap = _make_snap(bar, vwap=5812.00)
    event = _make_event(snap)

    writer = LevelObservationWriter(research_root=tmp_path, run_id=RUN_ID)
    bus = MagicMock()
    registry = MagicMock()
    sym_obj = MagicMock()
    sym_obj.tick_size = TICK
    registry.get.return_value = sym_obj

    from alpha.engines.live.monitor import IngestionMonitor
    mock_monitor = MagicMock(spec=IngestionMonitor)
    mock_monitor.summary.return_value = {SYM: {"state": "degraded"}}

    observer = LevelObserver(
        event_bus=bus,
        registry=registry,
        writer=writer,
        run_mode=RunMode.LIVE,
        ingestion_monitor=mock_monitor,
    )
    observer._run_id = RUN_ID
    observer._producer_version = "test"
    observer._last_session_key[SYM] = SESSION_DATE

    with patch("alpha.research.level_observer.calendar_for_symbol") as mock_cal:
        cal = MagicMock()
        cal.session_key.return_value = SESSION_DATE
        cal.session_date.return_value = type("d", (), {"isoformat": lambda s: SESSION_DATE})()
        mock_cal.return_value = cal
        await observer._process(event)

    row = writer._pending[(SYM, SESSION_DATE)][0]
    assert row["data_quality"] == "degraded"


def test_obs_id_deterministic():
    """Same inputs always produce the same obs_id."""
    ts = _TS_BASE
    id1 = make_obs_id(RUN_ID, SYM, ts, ReferenceLevelType.VWAP)
    id2 = make_obs_id(RUN_ID, SYM, ts, ReferenceLevelType.VWAP)
    assert id1 == id2


def test_obs_id_differs_by_level_type():
    ts = _TS_BASE
    vwap_id = make_obs_id(RUN_ID, SYM, ts, ReferenceLevelType.VWAP)
    orh_id  = make_obs_id(RUN_ID, SYM, ts, ReferenceLevelType.ORH)
    assert vwap_id != orh_id


def test_writer_parquet_file_readable(tmp_path):
    """Written Parquet files are readable and contain expected columns."""
    writer = LevelObservationWriter(research_root=tmp_path, run_id=RUN_ID, max_buffer_rows=2)
    for i in range(2):
        ts = datetime(2026, 1, 2, 14, i, tzinfo=timezone.utc)
        obs = LevelBarObservation(
            obs_id=make_obs_id(RUN_ID, SYM, ts, ReferenceLevelType.ORL),
            run_id=RUN_ID,
            run_mode=RunMode.REPLAY,
            source_bar_id=f"src-{i}",
            symbol=SYM,
            session_date=SESSION_DATE,
            bar_timestamp=ts,
            level_type=ReferenceLevelType.ORL,
            level_value=Decimal("5808.00"),
            open_distance_ticks=20, high_distance_ticks=24,
            low_distance_ticks=16, close_distance_ticks=20,
            bar_overlaps_level=False,
            open_side=PriceSide.ABOVE, close_side=PriceSide.ABOVE,
            proximity_band_ticks=8,
            volatility_reference=Decimal("10.00"),
            volatility_definition_version="atr14_m1_v1",
            tick_size=TICK,
            session_phase="early", orb_state="inside",
            producer_version="test", data_quality="clean",
        )
        writer.add(obs)

    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 1
    # Use ParquetFile.read() to avoid Hive partition discovery conflicting with
    # the session_date column stored in the file itself.
    table = pq.ParquetFile(str(files[0])).read()
    assert "obs_id" in table.schema.names
    assert "level_type" in table.schema.names
    assert "close_distance_ticks" in table.schema.names
    assert table.num_rows == 2
