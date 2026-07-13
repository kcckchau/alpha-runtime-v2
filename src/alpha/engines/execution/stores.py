"""
In-memory implementations of IdempotencyStore and IntentJournal.

Both are session-scoped (reset on process restart). The idempotency store
is intentionally in-memory: frontend session IDs change on page reload, so
cross-session deduplication is neither needed nor meaningful.

For production, IntentJournal should be backed by Parquet or a database
for permanent audit history. The in-memory implementation here is V1 only.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

from alpha.engines.execution.models import IntentAuditRecord

logger = logging.getLogger(__name__)

_UTC = timezone.utc


class InMemoryIdempotencyStore:
    """
    Maps idempotency_key → intent_id for the current session.

    check_and_register() is NOT thread-safe for multi-threaded use,
    but asyncio is single-threaded so this is fine within the event loop.
    """

    def __init__(self) -> None:
        self._store: dict[str, str] = {}   # idempotency_key → intent_id

    def check_and_register(self, idempotency_key: str, intent_id: str) -> bool:
        """
        Returns True if this is a new key (caller should proceed).
        Returns False if already seen (caller should return original response).
        """
        if idempotency_key in self._store:
            return False
        self._store[idempotency_key] = intent_id
        return True

    def get_intent_id(self, idempotency_key: str) -> str | None:
        return self._store.get(idempotency_key)

    def __len__(self) -> int:
        return len(self._store)


class InMemoryIntentJournal:
    """
    Append-only in-memory audit log for intent lifecycle events.

    Every significant state transition is recorded with the exact market
    and account snapshot IDs so the decision can be replayed later.

    V1: in-memory only. V2: persist to Parquet for permanent audit trail.
    """

    def __init__(self) -> None:
        self._records: dict[str, list[IntentAuditRecord]] = defaultdict(list)

    async def record(self, entry: IntentAuditRecord) -> None:
        self._records[entry.intent_id].append(entry)
        logger.debug(
            "IntentJournal: intent=%s event=%s detail=%s",
            entry.intent_id[:8], entry.event, entry.detail,
        )

    async def get_intent_history(self, intent_id: str) -> list[IntentAuditRecord]:
        return list(self._records.get(intent_id, []))

    def all_intent_ids(self) -> list[str]:
        return list(self._records.keys())


def _now() -> datetime:
    return datetime.now(_UTC)
