"""
Clock abstraction that enables replay and live modes to share engine code.

WallClock: delegates to system time.
ReplayClock: advances virtual time at configurable speed, so `now()` inside
             engine logic returns the replay timestamp rather than wall time.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Protocol, runtime_checkable

UTC = timezone.utc


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime: ...
    async def sleep(self, seconds: float) -> None: ...
    async def sleep_until(self, target: datetime) -> None: ...

    @property
    def is_replay(self) -> bool: ...


class WallClock:
    """Real-time clock backed by the system clock."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def sleep_until(self, target: datetime) -> None:
        delta = (target - self.now()).total_seconds()
        if delta > 0:
            await asyncio.sleep(delta)

    @property
    def is_replay(self) -> bool:
        return False


class ReplayClock:
    """
    Virtual clock for replay mode.

    Virtual time advances at `speed` × wall-clock rate starting from
    `start_time`. Calling `now()` computes:

        start_time + (wall_elapsed * speed)

    `sleep_until(target)` converts target virtual time to a wall-clock
    sleep so engines that respect the clock naturally pace themselves.
    """

    def __init__(self, start_time: datetime, speed: float = 1.0) -> None:
        if speed <= 0:
            raise ValueError("speed must be > 0")
        self._virtual_start = start_time
        self._wall_start = datetime.now(UTC)
        self._speed = speed

    def now(self) -> datetime:
        elapsed_wall = (datetime.now(UTC) - self._wall_start).total_seconds()
        return self._virtual_start + timedelta(seconds=elapsed_wall * self._speed)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds / self._speed)

    async def sleep_until(self, target: datetime) -> None:
        delta_virtual = (target - self.now()).total_seconds()
        if delta_virtual > 0:
            await asyncio.sleep(delta_virtual / self._speed)

    @property
    def is_replay(self) -> bool:
        return True

    @property
    def speed(self) -> float:
        return self._speed
