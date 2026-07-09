"""
Telegram notifier — sends alerts for setup signals and trade entries/exits.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import httpx

from alpha.config.settings import TelegramSettings
from alpha.models.enums import EventType, OrderSide, SetupState
from alpha.models.events import AnyEvent, PositionSignalEvent, SetupEvent

logger = logging.getLogger(__name__)

_API = "https://api.telegram.org/bot{token}/sendMessage"

# Only notify on these state transitions — ignore FORMING churn.
_NOTIFY_STATES = {SetupState.CONFIRMED, SetupState.TRIGGERED, SetupState.FAILED, SetupState.INVALIDATED}


class TelegramNotifier:
    """
    Subscribes to SETUP and POSITION_SIGNAL on the EventBus.

    Setup alerts:
      CONFIRMED  → setup is ready, shows entry/stop/target if available
      TRIGGERED  → entry condition met
      FAILED / INVALIDATED → setup is dead

    Position alerts (from PositionMonitor):
      WOULD_ENTER → entry signal with R:R
      WOULD_EXIT  → exit with P&L
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
            self._bus.subscribe(EventType.SETUP, self._on_setup, drop_if_full=True)
            self._bus.subscribe(EventType.POSITION_SIGNAL, self._on_signal)
        logger.info("TelegramNotifier: started (chat_id=%s)", self._settings.chat_id)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    async def _on_setup(self, event: AnyEvent) -> None:
        if not isinstance(event, SetupEvent):
            return
        if event.setup_state not in _NOTIFY_STATES:
            return
        # Skip replay events — we only want live signals.
        if event.metadata.is_replay:
            return
        await self._send_setup(event)

    async def _send_setup(self, event: SetupEvent) -> None:
        state = event.setup_state
        name = event.setup_type.replace("_", " ").upper()

        if state == SetupState.CONFIRMED:
            emoji = "🟡"
            header = f"{emoji} *SETUP CONFIRMED*"
        elif state == SetupState.TRIGGERED:
            emoji = "🟢"
            header = f"{emoji} *SETUP TRIGGERED*"
        elif state == SetupState.FAILED:
            emoji = "🔴"
            header = f"{emoji} *SETUP FAILED*"
        else:  # INVALIDATED
            emoji = "⚫"
            header = f"{emoji} *SETUP INVALIDATED*"

        lines = [
            header,
            f"`{event.symbol}` — {name}",
        ]
        if event.grade:
            grade_part = f"Grade: *{event.grade}*"
            if event.score is not None:
                grade_part += f"  score: {event.score:.0f}"
            lines.append(grade_part)

        if event.entry_trigger or event.stop_reference or event.target_reference:
            lines.append("")
            if event.entry_trigger:
                lines.append(f"Entry:  `{float(event.entry_trigger):,.2f}`")
            if event.stop_reference:
                lines.append(f"Stop:   `{float(event.stop_reference):,.2f}`")
            if event.target_reference:
                lines.append(f"Target: `{float(event.target_reference):,.2f}`")
            if event.entry_trigger and event.stop_reference and event.target_reference:
                risk = abs(float(event.entry_trigger) - float(event.stop_reference))
                reward = abs(float(event.target_reference) - float(event.entry_trigger))
                if risk > 0:
                    lines.append(f"R:R:    *{reward / risk:.1f}*")

        await self._send("\n".join(lines))

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
