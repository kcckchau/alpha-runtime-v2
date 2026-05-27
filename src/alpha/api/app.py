from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from alpha.api.routes import health, runtime, ws

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("API starting")
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
    app.include_router(ws.router)

    return app


app = create_app()
