"""
Thesis models: the mutable internal state of ThesisEngine
and the immutable snapshot emitted as ThesisEvent payload.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

from alpha.models.enums import ThesisState, ThesisType


@dataclass
class EvidenceItem:
    """One piece of evidence supporting or weakening the thesis."""
    text: str
    positive: bool      # True = supports thesis, False = weakens it
    weight: float       # magnitude of confidence change this caused


@dataclass
class LevelMemorySnapshot:
    """
    VWAP interaction memory for a single symbol.
    Produced by FeatureEngine, consumed by ThesisEngine.
    """
    symbol: str
    timestamp: datetime

    vwap_touch_count: int = 0           # how many cross/sweep events today
    last_vwap_outcome: str | None = None  # VWAPEpisodeOutcome value
    bars_since_last_vwap_touch: int = 0
    # "above" or "below" — which side price is currently on
    current_side: str | None = None
    # The sweep low recorded this session (lowest price below VWAP)
    session_sweep_low: Decimal | None = None
    # The rejection high recorded this session (highest price above VWAP after a cross_down)
    session_reject_high: Decimal | None = None


@dataclass
class TickFlowSnapshot:
    """
    Tick-level flow metrics computed from a rolling trade window.
    Produced by ThesisEngine from raw TradeEvents.
    """
    symbol: str
    timestamp: datetime

    tps_10s: float = 0.0           # trades per second, 10s window
    tps_30s: float = 0.0           # trades per second, 30s window
    vps_10s: float = 0.0           # volume per second, 10s window
    vps_30s: float = 0.0           # volume per second, 30s window

    buy_tps_10s: float | None = None   # when taker_side available
    sell_tps_10s: float | None = None
    buy_vps_10s: float | None = None
    sell_vps_10s: float | None = None

    avg_trade_size_30s: float = 0.0    # mean trade size in 30s window
    large_burst_count_10s: int = 0     # trades > 3x avg_trade_size in last 10s

    # vps_10s / vps_30s — > 1.0 means volume accelerating
    volume_acceleration: float = 1.0

    # Rolling session baseline (updated every bar)
    session_avg_tps: float = 0.0


@dataclass
class ThesisCandidate:
    """
    A trading thesis being tracked by ThesisEngine.
    Mutable — updated each bar as evidence accumulates.
    """
    thesis_id: UUID = field(default_factory=uuid4)
    thesis_type: ThesisType = ThesisType.FAKE_BREAKDOWN_RECLAIM_LONG
    symbol: str = ""
    state: ThesisState = ThesisState.WATCHING

    confidence: float = 0.10    # starts at 0.10 when WATCHING; ranges [0.0, 1.0]
    bars_alive: int = 0         # incremented each bar this thesis has been active

    # Entry plan (populated when confidence is high enough)
    entry: Decimal | None = None
    stop: Decimal | None = None
    target: Decimal | None = None

    # Contextual anchors set at detection time
    key_level: Decimal | None = None    # VWAP / ORH / ORL that triggered the thesis
    sweep_low: Decimal | None = None    # for LONG: low of the sweep bar
    rejection_high: Decimal | None = None  # for SHORT: high of the rejection bar

    # Narrative (rebuilt each bar)
    evidence: list[EvidenceItem] = field(default_factory=list)
    commit_conditions: list[str] = field(default_factory=list)
    invalidation_conditions: list[str] = field(default_factory=list)

    # Flip path
    possible_flip: ThesisType | None = None
    invalidation_reason: str | None = None

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ActiveThesis:
    """
    Container for one symbol's active thesis + its potential flip.
    At most one dominant thesis per symbol at a time.
    """
    dominant: ThesisCandidate
    flip: ThesisCandidate | None = None  # Shadow flip thesis tracked in parallel
