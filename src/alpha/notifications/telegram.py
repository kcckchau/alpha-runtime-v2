"""
Telegram notifier — sends trade alerts when PositionMonitor fires WOULD_ENTER.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import httpx

from alpha.config.settings import TelegramSettings
from alpha.models.enums import EventType, OrderSide
from alpha.models.events import AnyEvent, PositionSignalEvent

logger = logging.getLogger(__name__)

_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:
    """
    Subscribes to POSITION_SIGNAL on the EventBus.
    Sends a Telegram message on WOULD_ENTER.
    Sends a follow-up on WOULD_EXIT with P&L.
    """

    def __init__(self, settings: TelegramSettings, event_bus: object) -> None:
        self._settings = settings
        self._bus = event_bus
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if not self._settings.enabled:
            logger.info("TelegramNotifier: disabled (no token/chat_id configured)")
            return
        self._client = httpx.AsyncClient(timeout=10)
        from alpha.core.event_bus import EventBus
        if isinstance(self._bus, EventBus):
            self._bus.subscribe(EventType.POSITION_SIGNAL, self._on_signal)
        logger.info("TelegramNotifier: started (chat_id=%s)", self._settings.chat_id)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    async def _on_signal(self, event: AnyEvent) -> None:
        if not isinstance(event, PositionSignalEvent):
            return
        if event.signal_type == "would_enter":
            await self._send_entry(event)
        elif event.signal_type == "would_exit":
            await self._send_exit(event)

    async def _send_entry(self, event: PositionSignalEvent) -> None:
        direction = "▲ LONG" if event.direction == OrderSide.BUY else "▼ SHORT"
        entry = float(event.entry_price)
        stop = float(event.stop)
        target = float(event.target)
        risk = abs(entry - stop)
        reward = abs(target - entry)
        rr = reward / risk if risk > 0 else 0
        stop_pts = stop - entry if event.direction == OrderSide.BUY else entry - stop
        target_pts = target - entry if event.direction == OrderSide.BUY else entry - target

        lines = [
            f"🎯 *ENTRY SIGNAL*",
            f"{direction} — `{event.setup_type.replace('_', ' ')}`",
            f"Grade: *{event.grade}*  |  R:R: *{rr:.1f}*",
            f"",
            f"Entry:  `{entry:,.2f}`",
            f"Stop:   `{stop:,.2f}`  ({stop_pts:+.1f} pts)",
            f"Target: `{target:,.2f}`  ({target_pts:+.1f} pts)",
        ]
        if event.intrabar_delta is not None:
            sign = "+" if event.intrabar_delta > 0 else ""
            lines.append(f"")
            lines.append(f"Δ Delta: `{sign}{event.intrabar_delta}`" +
                         (f"  |  BAI: `{event.bid_ask_imbalance:.3f}`"
                          if event.bid_ask_imbalance is not None else ""))

        await self._send("\n".join(lines))

    async def _send_exit(self, event: PositionSignalEvent) -> None:
        pnl = float(event.pnl_pts or 0)
        emoji = "✅" if pnl >= 0 else "❌"
        reason = (event.exit_reason or "").replace("_", " ").upper()
        sign = "+" if pnl >= 0 else ""

        lines = [
            f"{emoji} *EXIT* — {reason}",
            f"`{event.setup_type.replace('_', ' ')}`",
            f"P&L: *{sign}{pnl:.2f} pts*",
        ]
        if event.bars_held is not None:
            lines.append(f"Held: {event.bars_held}s")
        if event.mfe is not None and event.mae is not None:
            lines.append(f"MFE: +{float(event.mfe):.1f}  MAE: -{float(event.mae):.1f}")

        await self._send("\n".join(lines))

    async def _send(self, text: str) -> None:
        if not self._client:
            return
        url = _API.format(token=self._settings.bot_token)
        try:
            resp = await self._client.post(url, json={
                "chat_id": self._settings.chat_id,
                "text": text,
                "parse_mode": "Markdown",
            })
            if resp.status_code != 200:
                logger.warning("Telegram send failed: %s %s", resp.status_code, resp.text)
        except Exception:
            logger.exception("Telegram send error")
