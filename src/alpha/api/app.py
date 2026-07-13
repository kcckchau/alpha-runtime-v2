from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from alpha.api.connection_manager import ConnectionManager
from alpha.api.routes import health, runtime, trade_intents, ws

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("API starting")
    app.state.backfill_jobs: dict = {}  # type: ignore[assignment]

    # Wire the ConnectionManager. When alpha run embeds the API it injects
    # app.state.event_bus before uvicorn starts its lifespan; alpha api standalone leaves
    # it unset and the push WS simply has no subscribers.
    manager = ConnectionManager()
    app.state.connection_manager = manager
    event_bus = getattr(app.state, "event_bus", None)
    if event_bus is not None:
        manager.subscribe_to_event_bus(event_bus)
        # Wire intrabar broadcast into PositionMonitor so it can push 1s flow updates
        try:
            from alpha.runtime_registry import get_bootstrap
            bootstrap = get_bootstrap()
            if bootstrap is not None and hasattr(bootstrap, "_position_monitor"):
                pm = bootstrap._position_monitor
                if pm is not None:
                    pm.set_intrabar_broadcast(manager.broadcast)
                    logger.info("API: intrabar broadcast wired to PositionMonitor")
        except Exception:
            logger.warning("API: could not wire intrabar broadcast to PositionMonitor")
        logger.info("API: ConnectionManager wired to EventBus — push WS active")
    else:
        logger.info("API: no EventBus injected — push WS inactive (polling WS still works)")

    yield
    logger.info("API shutting down")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Alpha Runtime v2",
        description="Production intraday trading runtime API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(runtime.router)
    app.include_router(trade_intents.router)
    app.include_router(ws.router)

    return app


app = create_app()
