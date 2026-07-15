"""
InteractionEpisodeManager: opens, tracks and closes interaction episodes.

Episode identity: (symbol, session_id, level_id)
One active episode per level per session at any time.

Separation logic:
- bars_at_separation counter increments when |close_distance_ticks| > separation_ticks
- counter RESETS if price returns within separation distance (even one bar)
- episode closes only after min_separation_bars consecutive bars at separation distance

This conservative reset prevents premature closure on noise.
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
    atr_at_start_was_none: bool = False


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

        # Last seen geometry per key (for pre_entry fields and approach_side)
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

            # Update last-seen geometry AFTER episode logic (so pre_entry captures
            # the bar before the episode opened, not the opening bar itself)
            # Actually we update BEFORE episode start check — see _handle_outside

    def on_session_end(self, symbol: str, session_id: str, timestamp: datetime) -> None:
        """Close all active episodes for a symbol+session."""
        keys = [k for k in self._active if k[0] == symbol and k[1] == session_id]
        for key in keys:
            self._close(key, timestamp, "session_ended")
        # Clear interaction index for this session
        for key in list(self._interaction_index):
            if key[0] == symbol and key[1] == session_id:
                del self._interaction_index[key]
                self._last_episode_id.pop(key, None)
                self._last_geo.pop(key, None)

    def on_gap(self, symbol: str, session_id: str, timestamp: datetime) -> None:
        """Invalidate all active episodes for a symbol+session due to data gap."""
        keys = [k for k in self._active if k[0] == symbol and k[1] == session_id]
        for key in keys:
            self._close(key, timestamp, "gap_detected")

    def flush(self) -> None:
        """Close all remaining active episodes (called at engine shutdown)."""
        for key in list(self._active.keys()):
            ep = self._active[key]
            ts = ep.summary.started_at  # best we have
            self._close(key, ts, "session_ended")

    # ── Private ───────────────────────────────────────────────────────────────

    def _handle_outside(
        self,
        key: _EpisodeKey,
        frame: InteractionFrame,
        level: LevelSnapshot,
        geo: BarLevelGeometry,
    ) -> None:
        # Capture pre-entry context BEFORE deciding to open
        pre_close_side, pre_close_d = self._last_geo.get(key, (None, None))

        # Update last-seen geo
        self._last_geo[key] = (geo.close_side, geo.close_distance_ticks)

        # Check if episode should start
        prox = self._config.proximity_ticks(frame.atr_14, level.tick_size)
        dist = abs(geo.close_distance_ticks)

        if dist <= prox or geo.range_spans_level:
            self._open_episode(key, frame, level, geo, pre_close_side, pre_close_d)

    def _handle_active(
        self,
        key: _EpisodeKey,
        active: _ActiveEpisode,
        frame: InteractionFrame,
        level: LevelSnapshot,
        geo: BarLevelGeometry,
    ) -> None:
        s = active.summary

        # Update last-seen geo
        self._last_geo[key] = (geo.close_side, geo.close_distance_ticks)

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
            sequence_num=frame.sequence_num,
        ))

        # Update running aggregates
        s.bar_count += 1
        if geo.range_spans_level:
            s.cross_count += 1
        s.max_above_ticks = max(s.max_above_ticks, geo.high_distance_ticks)
        s.max_below_ticks = min(s.max_below_ticks, geo.low_distance_ticks)
        s.end_side = geo.close_side

        # Check timeout
        if s.bar_count >= self._config.max_episode_bars:
            self._close(key, frame.bar_timestamp, "timeout")
            return

        # Separation logic
        sep = self._config.separation_ticks(frame.atr_14, level.tick_size)
        dist = abs(geo.close_distance_ticks)

        if dist > sep:
            active.bars_at_separation += 1
        else:
            active.bars_at_separation = 0  # reset on ANY return to proximity zone

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
            cross_count=1 if geo.range_spans_level else 0,
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
            sequence_num=frame.sequence_num,
        )

        active = _ActiveEpisode(
            summary=summary,
            bar_records=[bar_record],
            bars_at_separation=0,
            atr_at_start_was_none=atr_was_none,
        )
        self._active[key] = active

        logger.debug(
            "Episode opened | %s %s #%d | approach=%s | prox_entry=dist%+d",
            level.level_type, symbol, idx, approach_side, geo.close_distance_ticks,
        )

    def _close(self, key: _EpisodeKey, timestamp: datetime, end_reason: str) -> None:
        active = self._active.pop(key, None)
        if active is None:
            return

        s = active.summary
        s.ended_at = timestamp
        s.end_reason = end_reason

        # Derive aggregate fields from bar sequence
        s.close_side_flip_count, s.max_consecutive_closes_above, s.max_consecutive_closes_below = \
            _derive_close_sequence(active.bar_records)

        # is_valid_for_research: False for gaps or ATR-unwarm episodes
        if end_reason == "gap_detected":
            s.is_valid_for_research = False

        self._last_episode_id[key] = s.episode_id

        logger.debug(
            "Episode closed | %s %s #%d | bars=%d crosses=%d reason=%s valid=%s",
            s.level_type, s.symbol, s.interaction_index,
            s.bar_count, s.cross_count, end_reason, s.is_valid_for_research,
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
