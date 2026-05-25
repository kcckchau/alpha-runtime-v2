from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from alpha.models.enums import SetupGrade, SetupState, SetupType
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

    def transition(self, new_state: SetupState, reason: str = "") -> "Setup":
        """Return a new Setup with updated state. Immutable transition."""
        updates: dict[str, object] = {"state": new_state, "updated_at": datetime.utcnow()}
        if new_state == SetupState.TRIGGERED:
            updates["triggered_at"] = datetime.utcnow()
        if new_state in {SetupState.FAILED, SetupState.INVALIDATED}:
            updates["invalidated_at"] = datetime.utcnow()
            if reason:
                updates["invalidation_reason"] = reason
        return self.model_copy(update=updates)
