"""
PostgreSQL / TimescaleDB storage backend.

Hypertables are used for all time-series data (bars, trades, quotes,
market_states, setups, orders, executions).

The StorageEngine owns the SQLAlchemy async engine and passes sessions
to these helpers. No business logic lives here.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PostgresStore:
    """
    Async PostgreSQL store backed by asyncpg + SQLAlchemy.

    All methods accept SQLAlchemy AsyncSession instances injected by
    the StorageEngine to keep transaction management in one place.
    """

    # ── Bars ──────────────────────────────────────────────────────────────────

    async def upsert_bars(self, session: Any, rows: list[dict[str, Any]]) -> int:
        """Insert or update OHLCV bars. Returns row count."""
        raise NotImplementedError

    async def query_bars(
        self,
        session: Any,
        symbol: str,
        timeframe: str,
        start: Any,
        end: Any,
        limit: int = 10_000,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    # ── Trades ────────────────────────────────────────────────────────────────

    async def upsert_trades(self, session: Any, rows: list[dict[str, Any]]) -> int:
        raise NotImplementedError

    # ── Market states ─────────────────────────────────────────────────────────

    async def insert_market_state(self, session: Any, row: dict[str, Any]) -> None:
        raise NotImplementedError

    async def latest_market_state(
        self, session: Any, symbol: str
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    # ── Setups ────────────────────────────────────────────────────────────────

    async def upsert_setup(self, session: Any, row: dict[str, Any]) -> None:
        raise NotImplementedError

    async def open_setups(self, session: Any, symbol: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    # ── Orders / Executions ───────────────────────────────────────────────────

    async def upsert_order(self, session: Any, row: dict[str, Any]) -> None:
        raise NotImplementedError

    async def insert_execution(self, session: Any, row: dict[str, Any]) -> None:
        raise NotImplementedError

    async def daily_pnl(self, session: Any, date_str: str) -> float:
        raise NotImplementedError
