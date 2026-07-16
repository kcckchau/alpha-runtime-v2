"""
InteractionEpisodeManager: opens, tracks and closes interaction episodes.

Episode identity: (symbol, session_id, level_id)
One active episode per level per session at any time.

Entry rule:
- Episode opens when the bar's range overlaps the proximity band:
    low_distance_ticks <= proximity_ticks  AND  high_distance_ticks >= -proximity_ticks
  This captures wick-touches and full bars within the band, regardless of close.

Separation logic:
- A bar is "fully separated above" when low_distance_ticks > separation_ticks
  (the ENTIRE bar range is beyond the threshold — wicks included).
- A bar is "fully separated below" when high_distance_ticks < -separation_ticks.
- Consecutive bars must remain on the SAME side. A direction change resets the
  counter to 1 for the new direction. Alternating far-above/far-below never
  accumulates toward closure.
- Any bar that fails both tests (wick re-enters the gap between thresholds)
  resets the counter to 0.
- Episode closes after min_separation_bars consecutive fully-separated bars on
  the same side.

flush() end reason:
- "replay_completed" — called at engine shutdown (research replay or live stop).
- ended_at is set to the last bar's timestamp, NOT started_at.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Callable
from uuid import uuid4

from alpha.research.interaction.config import LevelDistanceConfig
from alpha.research.interaction.geometry import BarLevelGeometry, GEOMETRY_VERSION
from alpha.research.interaction.models import (
    EpisodeBarRecord,
    EpisodeSummary,
    InteractionFrame,
    LevelSnapshot,
)

logger = logging.getLogger(__name__)

# Type alias for episode key
_EpisodeKey = tuple[str, str, str]  # (symbol, session_id, level_id)


@dataclass
class _ActiveEpisode:
    summary: EpisodeSummary
    bar_records: list[EpisodeBarRecord] = field(default_factory=list)
    bars_at_separation: int = 0
    separation_direction: str | None = None  # "above" | "below" | None
    last_bar_ts: datetime | None = None      # timestamp of last bar seen
    atr_at_start_was_none: bool = False
    last_session_phase: str | None = None  # phase of last bar seen; used as end_session_phase


class InteractionEpisodeManager:
    """
    Processes one InteractionFrame at a time.
    For each level in the frame, runs episode open/continue/close logic.
    Completed episodes are handed to the provided callback.
    """

    POLICY_VERSION = "v1"

    def __init__(
        self,
        config: LevelDistanceConfig,
        on_episode_complete: Callable[[EpisodeSummary, list[EpisodeBarRecord]], None],
    ) -> None:
        self._config = config
        self._on_complete = on_episode_complete

        # Active episodes
        self._active: dict[_EpisodeKey, _ActiveEpisode] = {}

        # Last seen geometry per key (for pre_entry fields)
        self._last_geo: dict[_EpisodeKey, tuple[str, int]] = {}  # (close_side, close_distance_ticks)

        # Last completed episode_id per key (for prior_episode_id link)
        self._last_episode_id: dict[_EpisodeKey, str | None] = {}

        # Interaction counter per key
        self._interaction_index: dict[_EpisodeKey, int] = {}

    def process(self, frame: InteractionFrame) -> None:
        for level in frame.levels:
            geo = _compute_geo_for_level(frame, level)
            key: _EpisodeKey = (level.symbol, level.session_id, level.level_id)

            active = self._active.get(key)
            if active is None:
                self._handle_outside(key, frame, level, geo)
            else:
                self._handle_active(key, active, frame, level, geo)

    def on_session_end(self, symbol: str, session_id: str, timestamp: datetime) -> None:
        """Close all active episodes for a symbol+session with end_reason='session_ended'."""
        keys = [k for k in self._active if k[0] == symbol and k[1] == session_id]
        for key in keys:
            self._close(key, timestamp, "session_ended")
        # Clear per-session state
        for key in list(self._interaction_index):
            if key[0] == symbol and key[1] == session_id:
                del self._interaction_index[key]
                self._last_episode_id.pop(key, None)
                self._last_geo.pop(key, None)

    def on_gap(self, symbol: str, session_id: str, timestamp: datetime) -> None:
        """Invalidate all active episodes for a symbol+session due to within-session data gap."""
        keys = [k for k in self._active if k[0] == symbol and k[1] == session_id]
        for key in keys:
            self._close(key, timestamp, "gap_detected")

    def on_processing_error(self, symbol: str, timestamp: datetime) -> None:
        """Invalidate all active episodes for a symbol after an unhandled processing exception."""
        keys = [k for k in self._active if k[0] == symbol]
        for key in keys:
            self._close(key, timestamp, "processing_error")

    def flush(self) -> None:
        """
        Close all remaining active episodes (called at engine shutdown).
        Uses each episode's last_bar_ts as ended_at so duration is non-zero.
        End reason: 'replay_completed'.
        """
        for key in list(self._active.keys()):
            ep = self._active[key]
            ts = ep.last_bar_ts or ep.summary.started_at
            self._close(key, ts, "replay_completed")

    # ── Private ───────────────────────────────────────────────────────────────

    def _handle_outside(
        self,
        key: _EpisodeKey,
        frame: InteractionFrame,
        level: LevelSnapshot,
        geo: BarLevelGeometry,
    ) -> None:
        # Capture pre-entry context from the bar BEFORE this one
        pre_close_side, pre_close_d = self._last_geo.get(key, (None, None))

        # Update last-seen geometry for next bar's pre-entry capture
        self._last_geo[key] = (geo.close_side, geo.close_distance_ticks)

        # Entry: bar range overlaps the proximity band
        # [level - prox*tick, level + prox*tick]
        prox = self._config.proximity_ticks(frame.atr_14, level.tick_size)
        sep = self._config.separation_ticks(frame.atr_14, level.tick_size)
        enters_proximity = (
            geo.low_distance_ticks <= prox
            and geo.high_distance_ticks >= -prox
        )
        if enters_proximity:
            self._open_episode(key, frame, level, geo, pre_close_side, pre_close_d, prox, sep)

    def _handle_active(
        self,
        key: _EpisodeKey,
        active: _ActiveEpisode,
        frame: InteractionFrame,
        level: LevelSnapshot,
        geo: BarLevelGeometry,
    ) -> None:
        s = active.summary

        # Track latest bar timestamp and session phase for flush()
        active.last_bar_ts = frame.bar_timestamp
        active.last_session_phase = frame.session_phase

        # Update last-seen geo
        self._last_geo[key] = (geo.close_side, geo.close_distance_ticks)

        # Compute effective thresholds for this bar
        prox = self._config.proximity_ticks(frame.atr_14, level.tick_size)
        sep = self._config.separation_ticks(frame.atr_14, level.tick_size)

        # Append bar record
        bar_seq = len(active.bar_records)
        session_date = _session_date_from_id(s.session_id)
        active.bar_records.append(EpisodeBarRecord(
            episode_id=s.episode_id,
            bar_seq=bar_seq,
            bar_timestamp=frame.bar_timestamp,
            symbol=s.symbol,
            session_id=s.session_id,
            session_date=session_date,
            open_side=geo.open_side,
            close_side=geo.close_side,
            range_spans_level=geo.range_spans_level,
            high_distance_ticks=geo.high_distance_ticks,
            low_distance_ticks=geo.low_distance_ticks,
            close_distance_ticks=geo.close_distance_ticks,
            level_value_at_timestamp=level.level_value,
            atr_14=frame.atr_14,
            proximity_ticks=prox,
            separation_ticks=sep,
            sequence_num=frame.sequence_num,
        ))

        # Update running aggregates
        s.bar_count += 1
        if geo.range_spans_level:
            s.range_span_count += 1
        s.max_above_ticks = max(s.max_above_ticks, geo.high_distance_ticks)
        s.max_below_ticks = min(s.max_below_ticks, geo.low_distance_ticks)
        s.end_side = geo.close_side

        # Check timeout
        if s.bar_count >= self._config.max_episode_bars:
            self._close(key, frame.bar_timestamp, "timeout")
            return

        # ── Separation logic ─────────────────────────────────────────────────
        # Full bar range must be beyond the separation threshold on the same side
        # for the counter to accumulate. A direction change or wick-return resets.
        fully_separated_above = geo.low_distance_ticks > sep
        fully_separated_below = geo.high_distance_ticks < -sep

        if fully_separated_above:
            this_dir: str | None = "above"
        elif fully_separated_below:
            this_dir = "below"
        else:
            this_dir = None

        if this_dir is not None:
            if this_dir == active.separation_direction:
                active.bars_at_separation += 1
            else:
                # Direction changed — start fresh in new direction
                active.bars_at_separation = 1
                active.separation_direction = this_dir
        else:
            # Wick or close returned within the gap — full reset
            active.bars_at_separation = 0
            active.separation_direction = None

        if active.bars_at_separation >= self._config.min_separation_bars:
            self._close(key, frame.bar_timestamp, "separation")

    def _open_episode(
        self,
        key: _EpisodeKey,
        frame: InteractionFrame,
        level: LevelSnapshot,
        geo: BarLevelGeometry,
        pre_close_side: str | None,
        pre_close_distance_ticks: int | None,
        proximity_ticks: int,
        separation_ticks: int,
    ) -> None:
        symbol, session_id, level_id = key
        session_date = _session_date_from_id(session_id)

        idx = self._interaction_index.get(key, 0) + 1
        self._interaction_index[key] = idx

        prior_id = self._last_episode_id.get(key)

        approach_side = _infer_approach_side(pre_close_side, geo.open_side)

        episode_id = str(uuid4())
        atr_was_none = frame.atr_14 is None

        summary = EpisodeSummary(
            episode_id=episode_id,
            prior_episode_id=prior_id,
            level_id=level_id,
            level_type=level.level_type,
            symbol=symbol,
            session_id=session_id,
            session_date=session_date,
            interaction_index=idx,
            started_at=frame.bar_timestamp,
            ended_at=None,
            pre_entry_close_side=pre_close_side,
            pre_entry_close_distance_ticks=pre_close_distance_ticks,
            approach_side=approach_side,
            bar_count=1,
            range_span_count=1 if geo.range_spans_level else 0,
            max_above_ticks=geo.high_distance_ticks,
            max_below_ticks=geo.low_distance_ticks,
            end_side=geo.close_side,
            end_reason="",  # filled at close
            close_side_flip_count=0,
            max_consecutive_closes_above=0,
            max_consecutive_closes_below=0,
            is_valid_for_research=not atr_was_none,
            policy_version=self.POLICY_VERSION,
            policy_config_hash=self._config.config_hash(),
            geometry_version=GEOMETRY_VERSION,
            session_scope=level.session_scope,
            start_session_phase=frame.session_phase,
            end_session_phase=None,
        )

        bar_record = EpisodeBarRecord(
            episode_id=episode_id,
            bar_seq=0,
            bar_timestamp=frame.bar_timestamp,
            symbol=symbol,
            session_id=session_id,
            session_date=session_date,
            open_side=geo.open_side,
            close_side=geo.close_side,
            range_spans_level=geo.range_spans_level,
            high_distance_ticks=geo.high_distance_ticks,
            low_distance_ticks=geo.low_distance_ticks,
            close_distance_ticks=geo.close_distance_ticks,
            level_value_at_timestamp=level.level_value,
            atr_14=frame.atr_14,
            proximity_ticks=proximity_ticks,
            separation_ticks=separation_ticks,
            sequence_num=frame.sequence_num,
        )

        active = _ActiveEpisode(
            summary=summary,
            bar_records=[bar_record],
            bars_at_separation=0,
            separation_direction=None,
            last_bar_ts=frame.bar_timestamp,
            atr_at_start_was_none=atr_was_none,
            last_session_phase=frame.session_phase,
        )
        self._active[key] = active

        logger.debug(
            "Episode opened | %s %s #%d | approach=%s | prox_entry=low%+d high%+d",
            level.level_type, symbol, idx, approach_side,
            geo.low_distance_ticks, geo.high_distance_ticks,
        )

    def _close(self, key: _EpisodeKey, timestamp: datetime, end_reason: str) -> None:
        active = self._active.pop(key, None)
        if active is None:
            return

        s = active.summary
        s.ended_at = timestamp
        s.end_reason = end_reason
        s.end_session_phase = active.last_session_phase or s.start_session_phase

        # Derive aggregate fields from bar sequence
        s.close_side_flip_count, s.max_consecutive_closes_above, s.max_consecutive_closes_below = \
            _derive_close_sequence(active.bar_records)

        # Mark invalid for data-quality or exception failures
        if end_reason in ("gap_detected", "processing_error"):
            s.is_valid_for_research = False

        self._last_episode_id[key] = s.episode_id

        logger.debug(
            "Episode closed | %s %s #%d | bars=%d spans=%d reason=%s valid=%s",
            s.level_type, s.symbol, s.interaction_index,
            s.bar_count, s.range_span_count, end_reason, s.is_valid_for_research,
        )

        self._on_complete(s, active.bar_records)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _compute_geo_for_level(frame: InteractionFrame, level: LevelSnapshot) -> BarLevelGeometry:
    from alpha.research.interaction.geometry import compute_geometry
    return compute_geometry(
        open_price=frame.open,
        high_price=frame.high,
        low_price=frame.low,
        close_price=frame.close,
        level_value=level.level_value,
        tick_size=level.tick_size,
        atr_14=frame.atr_14,
        is_vwap=(level.level_type == "vwap"),
    )


def _infer_approach_side(pre_close_side: str | None, open_side: str) -> str:
    if pre_close_side == "above":
        return "from_above"
    if pre_close_side == "below":
        return "from_below"
    if open_side in ("above", "below"):
        return f"from_{open_side}"
    return "unknown"


def _session_date_from_id(session_id: str) -> str:
    """Extract session date from "{symbol}:{session_date}" format."""
    parts = session_id.split(":")
    return parts[-1] if parts else session_id


def _derive_close_sequence(
    bar_records: list[EpisodeBarRecord],
) -> tuple[int, int, int]:
    """
    Returns (close_side_flip_count, max_consecutive_above, max_consecutive_below).
    """
    if not bar_records:
        return 0, 0, 0

    flip_count = 0
    max_above = 0
    max_below = 0
    cur_above = 0
    cur_below = 0

    prev_side = None
    for rec in bar_records:
        side = rec.close_side
        if prev_side is not None and side != prev_side and prev_side != "on" and side != "on":
            flip_count += 1

        if side == "above":
            cur_above += 1
            cur_below = 0
        elif side == "below":
            cur_below += 1
            cur_above = 0
        else:  # "on"
            cur_above = 0
            cur_below = 0

        max_above = max(max_above, cur_above)
        max_below = max(max_below, cur_below)
        prev_side = side

    return flip_count, max_above, max_below
