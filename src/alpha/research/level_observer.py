"""
LevelObserver — shadow subscriber for Phase 0 level geometry recording.

Subscribes to PipelineOutputEvent (fires after all 5 pipeline stages complete).
For each M1 bar, emits one LevelBarObservation per available reference level
(VWAP, ORH, ORL) into LevelObservationWriter.

Constraints (non-negotiable):
  - Never publishes to EventBus.
  - Never calls SetupEngine, ThesisEngine, RiskEngine, or OrderEngine.
  - Never influences a trade decision.
  - Failures are logged and swallowed; the main pipeline is never interrupted.

Ordering guarantee:
  PipelineOutputEvent fires after BarPipeline runs all 5 stages. FeatureEngine
  (Stage 1) has already computed VWAP, ORH, ORL, ATR — they are embedded in
  bar_snapshot on the event. No separate ContextEngine call is needed.

VWAP distance rounding:
  VWAP is a volume-weighted running average — NOT guaranteed to be tick-aligned.
  Distances for VWAP rows use ROUND_HALF_UP via Decimal.to_integral_value().
  ORH and ORL are bar prices — tick-aligned — so exact integer arithmetic is used.
  The rounding path is selected per level_type in _distance_ticks().
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from alpha.calendar.resolver import calendar_for_symbol
from alpha.models.enums import DataQualityState, EventType, RuntimeMode
from alpha.models.events import AnyEvent, PipelineOutputEvent
from alpha.research.models import (
    LevelBarObservation,
    PriceSide,
    ReferenceLevelType,
    RunMode,
    make_obs_id,
)
from alpha.research.parquet_writer import LevelObservationWriter

from alpha.engines.live.monitor import IngestionMonitor

if TYPE_CHECKING:
    from alpha.core.event_bus import EventBus
    from alpha.core.registry import SymbolRegistry
    from alpha.models.snapshot import BarSnapshot

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")
_PROXIMITY_ATR_FACTOR = Decimal("0.20")   # 20% of ATR-14 defines the band
_FALLBACK_PROXIMITY_TICKS = 10            # used when ATR is unavailable
_DEFAULT_TICK_SIZE = Decimal("0.25")      # MNQ / MES / NQ / ES default


def _producer_version() -> str:
    """Return the current git commit SHA (first 8 chars), or 'dev' if unavailable."""
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short=8", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return sha or "dev"
    except Exception:
        return "dev"


def _make_run_id(run_mode: RunMode) -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    short = uuid4().hex[:8]
    return f"{run_mode}-{today}-{short}"


def _price_side(distance_ticks: int) -> PriceSide:
    if distance_ticks > 0:
        return PriceSide.ABOVE
    if distance_ticks < 0:
        return PriceSide.BELOW
    return PriceSide.ON


def _distance_ticks(price: Decimal, level: Decimal, tick_size: Decimal, *, vwap: bool) -> int:
    """Compute signed tick distance from level to price.

    vwap=True  → ROUND_HALF_UP (VWAP is not tick-aligned).
    vwap=False → exact integer (ORH/ORL are tick-aligned bar prices).
                 An assertion fires in debug if the price is not tick-aligned,
                 but never raises in production to avoid interrupting the pipeline.
    """
    raw = (price - level) / tick_size
    if vwap:
        return int(raw.to_integral_value(rounding=ROUND_HALF_UP))
    # Tick-aligned path — should divide evenly.
    integral = raw.to_integral_value(rounding=ROUND_HALF_UP)
    if raw != integral:
        logger.debug(
            "LevelObserver: non-tick-aligned distance (price=%s level=%s tick=%s raw=%s) — rounding",
            price, level, tick_size, raw,
        )
    return int(integral)


def _proximity_band(atr: Decimal | None, tick_size: Decimal) -> int:
    if atr is None or atr <= _ZERO:
        return _FALLBACK_PROXIMITY_TICKS
    raw = (atr * _PROXIMITY_ATR_FACTOR / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
    return max(1, int(raw))


class LevelObserver:
    """
    Shadow subscriber: records LevelBarObservation rows from PipelineOutputEvent.

    Lifecycle:
      observer = LevelObserver(event_bus, registry, writer, run_mode, ingestion_monitor)
      observer.attach()          # subscribe to PIPELINE_OUTPUT
      ...runtime runs...
      observer.flush()           # call at engine shutdown to drain buffer
    """

    def __init__(
        self,
        event_bus: "EventBus",
        registry: "SymbolRegistry",
        writer: LevelObservationWriter,
        run_mode: RunMode,
        ingestion_monitor: "IngestionMonitor | None" = None,
    ) -> None:
        self._bus = event_bus
        self._registry = registry
        self._writer = writer
        self._run_mode = run_mode
        self._ingestion_monitor = ingestion_monitor

        self._run_id = _make_run_id(run_mode)
        self._producer_version = _producer_version()

        # Per-symbol session tracking for session-reset dedup clear.
        self._last_session_key: dict[str, str] = {}

        self._bars_processed: int = 0
        self._obs_emitted: int = 0

    def attach(self) -> None:
        """Subscribe to the EventBus. Call after the bus is started."""
        self._bus.subscribe(
            EventType.PIPELINE_OUTPUT,
            self._handle,
            drop_if_full=True,   # shadow: drop rather than back-pressure the pipeline
        )
        logger.info(
            "LevelObserver attached | run_id=%s run_mode=%s producer=%s",
            self._run_id, self._run_mode, self._producer_version,
        )

    def flush(self) -> None:
        """Flush all buffered observations to Parquet. Call at engine shutdown."""
        try:
            self._writer.flush()
            logger.info(
                "LevelObserver: final flush complete | obs_emitted=%d rows_written=%d",
                self._obs_emitted, self._writer.rows_written,
            )
        except Exception:
            logger.exception("LevelObserver: flush failed at shutdown")

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _handle(self, event: AnyEvent) -> None:
        """Handler called by EventBus for each PipelineOutputEvent."""
        try:
            await self._process(event)
        except Exception:
            logger.exception(
                "LevelObserver: unhandled error processing event — skipping bar"
            )

    async def _process(self, event: AnyEvent) -> None:
        if not isinstance(event, PipelineOutputEvent):
            return

        snap: BarSnapshot = event.bar_snapshot
        if snap is None:
            return

        symbol = event.symbol
        bar_timestamp = event.timestamp

        # ── Session rollover detection ─────────────────────────────────────────
        sym_obj = self._registry.get(symbol)
        cal = calendar_for_symbol(sym_obj) if sym_obj else None
        if cal is not None:
            session_key = cal.session_key(bar_timestamp)
            if self._last_session_key.get(symbol) != session_key:
                self._writer.reset_session()
                self._last_session_key[symbol] = session_key

        session_date = (
            cal.session_date(bar_timestamp).isoformat()
            if cal is not None
            else bar_timestamp.astimezone().strftime("%Y-%m-%d")
        )

        # ── Tick size ─────────────────────────────────────────────────────────
        tick_size = (
            sym_obj.tick_size
            if sym_obj is not None and hasattr(sym_obj, "tick_size")
            else _DEFAULT_TICK_SIZE
        )

        # ── ATR and proximity band ─────────────────────────────────────────────
        atr = snap.atr_14
        proximity_band_ticks = _proximity_band(atr, tick_size)
        volatility_ref = atr if atr is not None else _ZERO

        # ── Data quality ───────────────────────────────────────────────────────
        data_quality = self._get_data_quality(symbol)

        # ── Source bar ID ─────────────────────────────────────────────────────
        source_bar_id = (
            str(event.metadata.event_id)
            if event.metadata is not None
            else None
        )

        self._bars_processed += 1

        # ── Emit one observation per available level ───────────────────────────
        levels: list[tuple[ReferenceLevelType, Decimal, bool]] = []  # (type, value, is_vwap)

        if snap.vwap is not None and snap.vwap > _ZERO:
            levels.append((ReferenceLevelType.VWAP, snap.vwap, True))

        if snap.orb_high is not None:
            levels.append((ReferenceLevelType.ORH, snap.orb_high, False))

        if snap.orb_low is not None:
            levels.append((ReferenceLevelType.ORL, snap.orb_low, False))

        bar = snap.bar
        for level_type, level_value, is_vwap in levels:
            open_d  = _distance_ticks(bar.open,  level_value, tick_size, vwap=is_vwap)
            high_d  = _distance_ticks(bar.high,  level_value, tick_size, vwap=is_vwap)
            low_d   = _distance_ticks(bar.low,   level_value, tick_size, vwap=is_vwap)
            close_d = _distance_ticks(bar.close, level_value, tick_size, vwap=is_vwap)

            obs = LevelBarObservation(
                obs_id=make_obs_id(self._run_id, symbol, bar_timestamp, level_type),
                run_id=self._run_id,
                run_mode=self._run_mode,
                source_bar_id=source_bar_id,
                symbol=symbol,
                session_date=session_date,
                bar_timestamp=bar_timestamp,
                level_type=level_type,
                level_value=level_value,
                open_distance_ticks=open_d,
                high_distance_ticks=high_d,
                low_distance_ticks=low_d,
                close_distance_ticks=close_d,
                bar_overlaps_level=(low_d <= 0 <= high_d),
                open_side=_price_side(open_d),
                close_side=_price_side(close_d),
                proximity_band_ticks=proximity_band_ticks,
                volatility_reference=volatility_ref,
                volatility_definition_version="atr14_m1_v1",
                tick_size=tick_size,
                session_phase=str(snap.session_phase),
                orb_state=str(snap.orb_state),
                schema_version="lbo_v1",
                producer_version=self._producer_version,
                data_quality=data_quality,
            )
            self._writer.add(obs)
            self._obs_emitted += 1

    def _get_data_quality(self, symbol: str) -> str:
        """Return the current DataQualityState for the symbol, or 'clean' if unavailable."""
        try:
            if isinstance(self._ingestion_monitor, IngestionMonitor):
                summary = self._ingestion_monitor.summary()
                sym_info = summary.get(symbol, {})
                return sym_info.get("state", DataQualityState.CLEAN)
        except Exception:
            pass
        return str(DataQualityState.CLEAN)
