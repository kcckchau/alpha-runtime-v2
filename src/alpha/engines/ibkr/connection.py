"""
Shared IBKR connection wrapper.

One IBKRConnection instance is created by BootstrapEngine and shared across
all three IBKR adapters (historical, live, order).

TWS ports:
  7497 — TWS paper trading
  7496 — TWS live trading
IB Gateway ports:
  4001 — Gateway paper trading
  4002 — Gateway live trading

Client ID must be unique per connection to TWS/Gateway.
If you get Error 326 "client id already in use", another application
is already connected with that ID — change IBKR__CLIENT_ID in .env.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import TYPE_CHECKING

from alpha.config.settings import IBKRSettings

if TYPE_CHECKING:
    from ib_insync import IB

logger = logging.getLogger(__name__)


class IBKRConnectionError(RuntimeError):
    pass


class IBKRConnection:
    """
    Manages the lifecycle of a single ib_insync IB() connection.

    `get()` connects lazily and returns the shared IB instance.
    `ensure_connected()` is safe to call repeatedly — it's a no-op if already up.
    """

    def __init__(self, settings: IBKRSettings) -> None:
        self._settings = settings
        self._ib: IB | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> "IB":
        """Return connected IB instance, retrying if TWS hasn't released the previous
        client ID yet (common after a quick Ctrl-C / restart)."""
        async with self._lock:
            if self._ib is not None and self._ib.isConnected():
                return self._ib
            return await self._connect_with_retry()

    async def disconnect(self) -> None:
        if self._ib and self._ib.isConnected():
            self._ib.disconnect()
            logger.info("IBKR disconnected")
        self._ib = None

    @property
    def is_connected(self) -> bool:
        return self._ib is not None and self._ib.isConnected()

    async def _connect_with_retry(self, max_attempts: int = 6, base_delay: float = 5.0) -> "IB":
        """Retry connecting to TWS/Gateway with exponential backoff.

        TWS holds the clientId slot for ~30-60s after a dirty disconnect.
        With 6 attempts at 5/10/20/20/20/20s delays, we cover that window automatically.
        """
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await self._connect()
            except IBKRConnectionError as exc:
                last_exc = exc
                if attempt == max_attempts:
                    break
                delay = min(base_delay * (2 ** (attempt - 1)), 20.0)
                logger.warning(
                    "IBKR connection attempt %d/%d failed — retrying in %.0fs (%s)",
                    attempt,
                    max_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
        raise last_exc  # type: ignore[misc]

    async def _connect(self) -> "IB":
        from ib_insync import IB

        ib = IB()
        logger.info(
            "Connecting to IBKR %s:%d clientId=%d",
            self._settings.host,
            self._settings.port,
            self._settings.client_id,
        )
        try:
            await ib.connectAsync(
                self._settings.host,
                self._settings.port,
                clientId=self._settings.client_id,
                timeout=self._settings.timeout,
            )
        except Exception as exc:
            with suppress(Exception):
                ib.disconnect()
            exc_name = type(exc).__name__
            exc_text = str(exc).strip()
            exc_detail = f"{exc_name}: {exc_text}" if exc_text else exc_name
            hint = (
                "Check that TWS/Gateway is running and "
                f"that clientId {self._settings.client_id} is not already in use "
                "(change IBKR__CLIENT_ID in .env if needed)."
            )
            raise IBKRConnectionError(
                f"IBKR connection failed ({self._settings.host}:{self._settings.port}): "
                f"{exc_detail}. {hint}"
            ) from exc

        self._ib = ib
        logger.info("IBKR connected — accounts: %s", ib.managedAccounts())
        return ib
