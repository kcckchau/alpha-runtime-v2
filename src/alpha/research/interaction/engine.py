"""
LevelInteractionEngine: shadow research subscriber.

Subscribes to PIPELINE_OUTPUT with drop_if_full=True (research — must not
back-pressure the main pipeline). Gap detection compensates: if timestamp
gap > max_bar_gap_seconds, active episodes are invalidated with reason
"gap_detected" and is_valid_for_research=False.

Session rollover detected via calendar session_key change per symbol.

Never publishes to EventBus. Never influences trades.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from alpha.models.enums import EventType

if TYPE_CHECKING:
    from alpha.core.event_bus import EventBus
    from alpha.core.registry import SymbolRegistry

from alpha.research.interaction.config import LevelDistanceConfig
from alpha.research.interaction.episode import InteractionEpisodeManager
from alpha.research.interaction.models import InteractionFrame, LevelSnapshot
from alpha.research.interaction.writer import EpisodeWriter

logger = logging.getLogger(__name__)


class LevelInteractionEngine:
    """
    Wires InteractionFrame building, gap detection, session rollover,
    InteractionEpisodeManager and EpisodeWriter.
    """

    def __init__(
        self,
        event_bus: "EventBus",
        registry: "SymbolRegistry",
        research_root: Path,
        config: LevelDistanceConfig | None = None,
        run_id: str | None = None,
    ) -> None:
        self._bus = event_bus
        self._registry = registry
        self._config = config or LevelDistanceConfig()
        self._writer = EpisodeWriter(research_root, run_id=run_id)
        self._manager = InteractionEpisodeManager(
            config=self._config,
            on_episode_complete=self._on_episode_complete,
        )

        # Gap detection: last bar timestamp per symbol
        self._last_bar_ts: dict[str, datetime] = {}

        # Session rollover: last session_key per symbol
        self._last_session_key: dict[str, str] = {}

        self._sub = None
        self._frames_processed = 0
        # Separate counter for periodic flush (episodes_written only increments after flush)
        self._completed_episode_count = 0

    def attach(self) -> None:
        self._sub = self._bus.subscribe(
            EventType.PIPELINE_OUTPUT,
            self._handle,
            queue_size=10_000,
            drop_if_full=True,  # research: never back-pressure pipeline
        )
        logger.info("LevelInteractionEngine attached (shadow research subscriber)")

    def detach(self) -> None:
        if self._sub is not None:
            self._bus.unsubscribe(self._sub)
            self._sub = None

    def flush(self) -> None:
        """Call at shutdown to close remaining open episodes and flush writer."""
        self._manager.flush()
        self._writer.flush()
        logger.info(
            "LevelInteractionEngine flushed | episodes_written=%d",
            self._writer.episodes_written,
        )

    @property
    def episodes_written(self) -> int:
        return self._writer.episodes_written

    async def _handle(self, event) -> None:
        try:
            await self._process(event)
        except Exception:
            logger.exception("LevelInteractionEngine: unhandled error — invalidating active episodes")
            try:
                from alpha.models.events import PipelineOutputEvent
                if isinstance(event, PipelineOutputEvent):
                    symbol = event.symbol
                    bar_ts = event.timestamp
                    snap = event.bar_snapshot
                    if snap is not None and snap.bar is not None:
                        bar_ts = snap.bar.timestamp
                    self._manager.on_processing_error(symbol, bar_ts)
            except Exception:
                logger.exception("LevelInteractionEngine: on_processing_error also failed")

    async def _process(self, event) -> None:
        from alpha.models.events import PipelineOutputEvent
        if not isinstance(event, PipelineOutputEvent):
            return

        snap = event.bar_snapshot
        if snap is None or snap.bar is None:
            return

        symbol = event.symbol
        bar_ts: datetime = snap.bar.timestamp  # canonical market time, not event dispatch time

        # ── Session rollover detection (must run BEFORE gap detection) ────────
        # Normal session-to-session transitions are NOT data gaps — they are
        # expected overnight breaks. Checking rollover first prevents the gap
        # detector from misclassifying them as missing data.
        session_id = self._build_session_id(symbol, snap)
        last_session = self._last_session_key.get(symbol)
        is_session_rollover = last_session is not None and session_id != last_session
        if is_session_rollover:
            logger.info(
                "LevelInteractionEngine: session rollover %s → %s for %s",
                last_session, session_id, symbol,
            )
            self._manager.on_session_end(symbol, last_session, bar_ts)
            self._writer.flush()
        self._last_session_key[symbol] = session_id

        # ── Gap detection (within-session only) ──────────────────────────────
        last_ts = self._last_bar_ts.get(symbol)
        if last_ts is not None and not is_session_rollover:
            gap_seconds = (bar_ts - last_ts).total_seconds()
            if gap_seconds > self._config.max_bar_gap_seconds:
                logger.warning(
                    "LevelInteractionEngine: gap detected for %s — %.0fs gap "
                    "(threshold=%ds) — invalidating active episodes",
                    symbol, gap_seconds, self._config.max_bar_gap_seconds,
                )
                self._manager.on_gap(symbol, session_id, bar_ts)
        self._last_bar_ts[symbol] = bar_ts

        # ── Build InteractionFrame ────────────────────────────────────────────
        levels = self._resolve_levels(symbol, session_id, snap)
        if not levels:
            return

        frame = InteractionFrame(
            bar_timestamp=bar_ts,
            symbol=symbol,
            session_id=session_id,
            sequence_num=event.metadata.sequence_num if event.metadata else None,
            open=snap.bar.open,
            high=snap.bar.high,
            low=snap.bar.low,
            close=snap.bar.close,
            volume=snap.bar.volume,
            atr_14=snap.atr_14,
            session_phase=str(snap.session_phase),
            is_replay=event.metadata.is_replay if event.metadata else False,
            levels=tuple(levels),
        )

        self._manager.process(frame)
        self._frames_processed += 1

    def _build_session_id(self, symbol: str, snap) -> str:
        """Build stable session_id from BarSnapshot session date."""
        # BarSnapshot has session_date via the feature engine calendar
        # Use snap.bar.timestamp date in ET as fallback
        try:
            from alpha.calendar.resolver import calendar_for_symbol
            from alpha.core.registry import SymbolNotFoundError
            try:
                sym_obj = self._registry.get(symbol)
            except SymbolNotFoundError:
                sym_obj = None
            if sym_obj:
                cal = calendar_for_symbol(sym_obj)
                session_date = cal.session_date(snap.bar.timestamp).isoformat()
                return f"{symbol}:{session_date}"
        except Exception:
            pass
        # Fallback: use UTC date from bar timestamp
        return f"{symbol}:{snap.bar.timestamp.date().isoformat()}"

    def _resolve_levels(self, symbol: str, session_id: str, snap) -> list[LevelSnapshot]:
        """Extract registered levels from BarSnapshot for Phase 1: VWAP, ORH, ORL.

        Level ID format: "{symbol}:{level_type}:rth:{session_date}"
        e.g. "MNQ:vwap:rth:2026-07-10"

        ORH/ORL are only registered after the opening range window has closed
        (session_phase != OPENING_RANGE). During the OR window the levels are
        still accumulating and must not be treated as fixed interaction points.
        """
        from alpha.models.enums import ORBState

        tick_size = self._get_tick_size(symbol)
        # session_id = "{symbol}:{session_date}" — extract the date portion
        session_date = session_id.split(":", 1)[1] if ":" in session_id else session_id
        levels: list[LevelSnapshot] = []

        if snap.vwap and snap.vwap > 0:
            levels.append(LevelSnapshot(
                level_id=f"{symbol}:vwap:rth:{session_date}",
                symbol=symbol,
                session_id=session_id,
                level_type="vwap",
                level_value=snap.vwap,
                tick_size=tick_size,
                is_dynamic=True,
                sampling_note="end_of_bar_cumulative_vwap",
            ))

        # ORH/ORL: only after the opening range is fully established (frozen)
        # ORBState.NOT_SET means the ORB window is still accumulating — levels are not yet fixed
        or_frozen = snap.orb_state != ORBState.NOT_SET

        if or_frozen and snap.orb_high is not None:
            levels.append(LevelSnapshot(
                level_id=f"{symbol}:orh:rth:{session_date}",
                symbol=symbol,
                session_id=session_id,
                level_type="orh",
                level_value=snap.orb_high,
                tick_size=tick_size,
                is_dynamic=False,
                sampling_note="fixed_orb_high",
            ))

        if or_frozen and snap.orb_low is not None:
            levels.append(LevelSnapshot(
                level_id=f"{symbol}:orl:rth:{session_date}",
                symbol=symbol,
                session_id=session_id,
                level_type="orl",
                level_value=snap.orb_low,
                tick_size=tick_size,
                is_dynamic=False,
                sampling_note="fixed_orb_low",
            ))

        return levels

    def _get_tick_size(self, symbol: str) -> Decimal:
        try:
            from alpha.core.registry import SymbolNotFoundError
            try:
                sym_obj = self._registry.get(symbol)
            except SymbolNotFoundError:
                sym_obj = None
            if sym_obj and hasattr(sym_obj, "tick_size") and sym_obj.tick_size:
                return Decimal(str(sym_obj.tick_size))
        except Exception:
            pass
        return Decimal("0.25")  # MNQ/NQ default

    def _on_episode_complete(self, summary, bars) -> None:
        self._writer.write(summary, bars)
        self._completed_episode_count += 1
        # Flush every 10 completed episodes.
        # episodes_written only increments after flush(), so it cannot be used
        # here directly — it would flush on episode 1 and then never again.
        if self._completed_episode_count % 10 == 0:
            self._writer.flush()
