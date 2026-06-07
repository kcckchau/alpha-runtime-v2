from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from alpha.models.enums import OrderSide, SessionPhase, SetupGrade, SetupState, SetupType
from alpha.models.market_state import MarketState
from alpha.models.snapshot import BarSnapshot


class Setup(BaseModel):
    """Lifecycle state machine for a detected trade setup."""

    setup_id: UUID = Field(default_factory=uuid4)
    symbol: str
    setup_type: SetupType
    state: SetupState = SetupState.FORMING

    detected_at: datetime
    updated_at: datetime
    triggered_at: datetime | None = None
    invalidated_at: datetime | None = None

    # ── Price levels ──────────────────────────────────────────────────────────
    entry_trigger: Decimal | None = None      # price level that triggers entry
    stop_reference: Decimal | None = None     # initial stop reference
    target_reference: Decimal | None = None   # initial target reference

    # ── Scoring (populated by Scoring Engine) ─────────────────────────────────
    score: float | None = None               # 0.0–100.0
    grade: SetupGrade | None = None

    # ── Conditions ───────────────────────────────────────────────────────────
    conditions_met: list[str] = Field(default_factory=list)
    conditions_missing: list[str] = Field(default_factory=list)
    invalidation_reason: str | None = None

    # ── Context snapshots at detection time ──────────────────────────────────
    market_state: MarketState
    bar_snapshot: BarSnapshot

    # ── Metadata ─────────────────────────────────────────────────────────────
    notes: str = ""
    tags: list[str] = Field(default_factory=list)

    def transition(
        self,
        new_state: SetupState,
        reason: str = "",
        bar_time: "datetime | None" = None,
    ) -> "Setup":
        """Return a new Setup with updated state. Immutable transition.

        Pass bar_time to use the bar's timestamp instead of wall-clock time.
        This is required during replay so that setup timestamps reflect the
        historical bar time rather than the moment the replay runs.
        """
        from datetime import timezone
        t = bar_time if bar_time is not None else datetime.now(timezone.utc)
        updates: dict[str, object] = {"state": new_state, "updated_at": t}
        if new_state == SetupState.TRIGGERED:
            updates["triggered_at"] = t
        if new_state in {SetupState.FAILED, SetupState.INVALIDATED}:
            updates["invalidated_at"] = t
            if reason:
                updates["invalidation_reason"] = reason
        return self.model_copy(update=updates)


class SetupHistoryEntry(BaseModel):
    setup_id: UUID
    setup_type: SetupType
    state: SetupState
    detected_at: datetime
    updated_at: datetime
    resolved_at: datetime | None = None
    side: OrderSide
    level_tag: str
    entry_trigger: Decimal | None = None
    stop_reference: Decimal | None = None
    target_reference: Decimal | None = None
    grade: SetupGrade | None = None
    score: float | None = None
    session_phase: SessionPhase
    invalidation_reason: str | None = None


class SessionSetupContext(BaseModel):
    symbol: str
    session_key: str
    session_date: str
    session_open: datetime
    session_close: datetime
    session_timezone: str
    last_setup: SetupHistoryEntry | None = None
    setups: list[SetupHistoryEntry] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)
    counts_by_type: dict[str, dict[str, int]] = Field(default_factory=dict)
    counts_by_level: dict[str, int] = Field(default_factory=dict)
