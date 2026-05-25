"""
BaseEngine: lifecycle contract for all runtime engines.

State machine:
  CREATED → INITIALIZING → READY → STARTING → RUNNING → STOPPING → STOPPED
                                                                  ↘ ERROR
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from alpha.models.enums import EngineState, HealthStatus


logger = logging.getLogger(__name__)


class EngineHealth:
    def __init__(
        self,
        status: HealthStatus,
        engine_name: str,
        details: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        self.status = status
        self.engine_name = engine_name
        self.details = details or {}
        self.error = error
        self.checked_at = datetime.now(timezone.utc)

    def is_healthy(self) -> bool:
        return self.status == HealthStatus.HEALTHY


class BaseEngine(ABC):
    """
    Abstract base for all runtime engines.

    Subclasses implement `_on_initialize`, `_on_start`, `_on_stop`, and
    optionally `_health_check`. The public `initialize / start / stop`
    methods manage state transitions and error handling.
    """

    def __init__(self) -> None:
        self._state = EngineState.CREATED
        self._error: Exception | None = None
        self._started_at: datetime | None = None
        self._stopped_at: datetime | None = None

    # ── Identity ──────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable engine identifier."""

    # ── Public lifecycle ──────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """Acquire resources. Called once before `start`."""
        self._assert_state(EngineState.CREATED)
        self._state = EngineState.INITIALIZING
        try:
            await self._on_initialize()
            self._state = EngineState.READY
            logger.info("[%s] initialized", self.name)
        except Exception as exc:
            self._transition_error(exc)
            raise

    async def start(self) -> None:
        """Begin processing. Called after `initialize`."""
        self._assert_state(EngineState.READY)
        self._state = EngineState.STARTING
        try:
            self._started_at = datetime.now(timezone.utc)
            await self._on_start()
            self._state = EngineState.RUNNING
            logger.info("[%s] started", self.name)
        except Exception as exc:
            self._transition_error(exc)
            raise

    async def stop(self) -> None:
        """Graceful shutdown. Safe to call from any running state."""
        if self._state not in {
            EngineState.RUNNING,
            EngineState.STARTING,
            EngineState.ERROR,
        }:
            return
        self._state = EngineState.STOPPING
        try:
            await self._on_stop()
        except Exception as exc:
            logger.exception("[%s] error during stop: %s", self.name, exc)
        finally:
            self._state = EngineState.STOPPED
            self._stopped_at = datetime.now(timezone.utc)
            logger.info("[%s] stopped", self.name)

    async def health_check(self) -> EngineHealth:
        """Return current health. Override `_health_check` to add checks."""
        if self._state == EngineState.ERROR:
            return EngineHealth(
                HealthStatus.UNHEALTHY,
                self.name,
                error=str(self._error),
            )
        if self._state != EngineState.RUNNING:
            return EngineHealth(
                HealthStatus.DEGRADED,
                self.name,
                details={"state": self._state},
            )
        return await self._health_check()

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def state(self) -> EngineState:
        return self._state

    @property
    def is_running(self) -> bool:
        return self._state == EngineState.RUNNING

    # ── Overrideable hooks ────────────────────────────────────────────────────

    async def _on_initialize(self) -> None:
        """Override to set up connections, load config, etc."""

    async def _on_start(self) -> None:
        """Override to spawn background tasks, open streams, etc."""

    @abstractmethod
    async def _on_stop(self) -> None:
        """Override to cancel tasks, close connections, flush buffers."""

    async def _health_check(self) -> EngineHealth:
        return EngineHealth(HealthStatus.HEALTHY, self.name)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _assert_state(self, expected: EngineState) -> None:
        if self._state != expected:
            raise RuntimeError(
                f"[{self.name}] expected state {expected}, got {self._state}"
            )

    def _transition_error(self, exc: Exception) -> None:
        self._state = EngineState.ERROR
        self._error = exc
        logger.exception("[%s] error: %s", self.name, exc)
