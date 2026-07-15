from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class LevelSnapshot:
    """Stable semantic identity for one market level at one bar."""
    level_id: str          # "{level_type}:{symbol}:{session_id}" — stable across session
    symbol: str
    session_id: str        # "{symbol}:{session_date_et}"
    level_type: str        # "vwap" | "orh" | "orl"
    level_value: Decimal   # value at this bar timestamp (changes each bar for VWAP)
    tick_size: Decimal
    is_dynamic: bool       # True for VWAP (value changes), False for ORH/ORL
    sampling_note: str     # "end_of_bar_cumulative_vwap" or "fixed_orb_high" etc.


@dataclass(frozen=True)
class InteractionFrame:
    """
    Minimal research input. Contains NO thesis, setups, or scored_setups.
    Built from PipelineOutputEvent by stripping everything that could
    contaminate research sample construction.
    """
    bar_timestamp: datetime
    symbol: str
    session_id: str        # "{symbol}:{session_date_et}"
    sequence_num: int | None   # from EventMetadata.sequence_num, nullable

    # Bar OHLCV — from snap.bar (BarSnapshot does NOT have these directly)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    # Context (no setup opinions)
    atr_14: Decimal | None
    session_phase: str
    is_replay: bool

    # Registered levels for this bar
    levels: tuple[LevelSnapshot, ...]


@dataclass
class EpisodeBarRecord:
    """
    One row per bar within an episode. Ordered by bar_seq.
    Preserves raw bar-to-bar side sequence for later interpretation.
    Repeated symbol/session_id/session_date for direct joins to M1 bar data.
    """
    episode_id: str
    bar_seq: int           # 0-indexed within episode

    bar_timestamp: datetime
    symbol: str
    session_id: str
    session_date: str      # YYYY-MM-DD in ET

    open_side: str         # "above" | "below" | "on"
    close_side: str
    range_spans_level: bool   # low <= level <= high (same as bar_overlaps_level)

    high_distance_ticks: int
    low_distance_ticks: int
    close_distance_ticks: int
    level_value_at_timestamp: Decimal

    sequence_num: int | None   # from EventMetadata.sequence_num, nullable


@dataclass
class EpisodeSummary:
    """
    One row per episode. Aggregates derived from bar sequence at close time.

    max_above_ticks and max_below_ticks describe excursion during the active
    episode window (started_at → ended_at) only. They are NOT forward-looking
    outcome labels and must not be interpreted as post-episode MFE/MAE.
    """
    episode_id: str
    prior_episode_id: str | None   # episode_id of previous episode for same level

    level_id: str
    level_type: str
    symbol: str
    session_id: str
    session_date: str      # YYYY-MM-DD in ET

    interaction_index: int   # 1st, 2nd, 3rd interaction with this level this session

    started_at: datetime
    ended_at: datetime | None

    # Minimal approach context (last bar before episode opened — no approach buffer)
    pre_entry_close_side: str | None
    pre_entry_close_distance_ticks: int | None

    approach_side: str     # "from_above" | "from_below" | "unknown"

    bar_count: int
    cross_count: int       # bars where range_spans_level is True

    # Intra-episode excursion only. NOT outcome labels.
    max_above_ticks: int   # max high_distance_ticks seen (positive = above level)
    max_below_ticks: int   # min low_distance_ticks seen (negative = below level)

    end_side: str | None   # close_side of the last bar
    end_reason: str        # "separation" | "timeout" | "session_ended" | "gap_detected"

    # Derived from bar sequence at close time
    close_side_flip_count: int           # times close_side changed between consecutive bars
    max_consecutive_closes_above: int
    max_consecutive_closes_below: int

    is_valid_for_research: bool   # False if gap_detected or ATR was None at start

    policy_version: str
    policy_config_hash: str
    geometry_version: str
