"""
Event normalizer contract.

Every HistoricalDataSource owns its own normalizer that converts
vendor-specific wire formats into the canonical BarEvent / TradeEvent /
QuoteEvent types before they enter the event bus.

This keeps all vendor-specific parsing logic completely isolated.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from alpha.models.enums import DataSourceId
from alpha.models.events import BarEvent, EventMetadata, QuoteEvent, TradeEvent


class EventNormalizer(ABC):
    """
    Converts raw vendor-format data into normalized pipeline events.

    Implementations live alongside their data source adapter.
    """

    @property
    @abstractmethod
    def source_id(self) -> DataSourceId: ...

    @abstractmethod
    def normalize_bar(self, raw: Any) -> BarEvent:
        """Convert raw bar dict/object to BarEvent."""

    def normalize_trade(self, raw: Any) -> TradeEvent:
        raise NotImplementedError(f"{self.source_id} normalizer has no trade support")

    def normalize_quote(self, raw: Any) -> QuoteEvent:
        raise NotImplementedError(f"{self.source_id} normalizer has no quote support")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _make_metadata(self, *, is_replay: bool = False) -> EventMetadata:
        return EventMetadata(
            event_id=uuid4(),
            source=self.source_id,
            received_at=datetime.now(timezone.utc),
            is_replay=is_replay,
        )
