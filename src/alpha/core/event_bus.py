"""
Async in-process event bus.

Engines publish events and subscribe to event types they care about.
Subscriptions can optionally filter by symbol (None = receive all symbols).

Architecture:
  - Each subscription gets its own bounded asyncio.Queue
  - A dispatcher task routes published events to matching queues
  - Worker tasks drain each subscription queue and call the handler
  - Backpressure: publishers wait when a subscriber queue is full
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Coroutine, Any
from uuid import UUID, uuid4

from alpha.models.events import AnyEvent, BaseEvent
from alpha.models.enums import EventType


logger = logging.getLogger(__name__)

HandlerT = Callable[[AnyEvent], Coroutine[Any, Any, None]]


@dataclass
class Subscription:
    subscription_id: UUID = field(default_factory=uuid4)
    event_type: EventType = EventType.BAR
    symbol: str | None = None          # None → all symbols
    handler: HandlerT = field(repr=False, default=lambda e: None)  # type: ignore[assignment]
    queue: asyncio.Queue[AnyEvent] = field(repr=False, default_factory=lambda: asyncio.Queue(maxsize=1000))
    _task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)


class EventBus:
    """
    Central event router. Must be started before use.

    Usage::

        bus = EventBus()
        await bus.start()

        sub = bus.subscribe(EventType.BAR, my_handler, symbol="AAPL")
        await bus.publish(bar_event)

        bus.unsubscribe(sub)
        await bus.stop()
    """

    def __init__(self, queue_size: int = 2000) -> None:
        self._queue_size = queue_size
        self._subscriptions: dict[EventType, list[Subscription]] = defaultdict(list)
        self._running = False
        self._lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        logger.debug("EventBus started")

    async def stop(self) -> None:
        self._running = False
        async with self._lock:
            for subs in self._subscriptions.values():
                for sub in subs:
                    if sub._task and not sub._task.done():
                        sub._task.cancel()
        logger.debug("EventBus stopped")

    # ── Pub / Sub ─────────────────────────────────────────────────────────────

    async def publish(self, event: AnyEvent) -> None:
        """Route event to all matching subscribers."""
        if not self._running:
            return

        targets = self._subscriptions.get(event.event_type, [])
        for sub in targets:
            if sub.symbol is not None and sub.symbol != event.symbol:
                continue
            if sub.queue.full():
                logger.warning(
                    "EventBus: queue full for subscription %s (%s/%s), applying backpressure",
                    sub.subscription_id,
                    event.event_type,
                    event.symbol,
                )
            await sub.queue.put(event)

    def subscribe(
        self,
        event_type: EventType,
        handler: HandlerT,
        *,
        symbol: str | None = None,
        queue_size: int | None = None,
    ) -> Subscription:
        """Register a handler and return a Subscription handle."""
        sub = Subscription(
            event_type=event_type,
            symbol=symbol,
            handler=handler,
            queue=asyncio.Queue(maxsize=queue_size or self._queue_size),
        )
        sub._task = asyncio.ensure_future(self._drain(sub))
        self._subscriptions[event_type].append(sub)
        logger.debug(
            "EventBus: subscribed %s to %s symbol=%s",
            sub.subscription_id,
            event_type,
            symbol,
        )
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub._task and not sub._task.done():
            sub._task.cancel()
        subs = self._subscriptions.get(sub.event_type, [])
        try:
            subs.remove(sub)
        except ValueError:
            pass

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _drain(self, sub: Subscription) -> None:
        while True:
            event = await sub.queue.get()
            try:
                await sub.handler(event)
            except Exception:
                logger.exception(
                    "EventBus: handler error for subscription %s", sub.subscription_id
                )
            finally:
                sub.queue.task_done()
