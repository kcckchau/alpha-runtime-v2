"""
Alpha Runtime v2 — entrypoint.

Commands:
  alpha run       Start the full runtime (mode from config)
  alpha api       Start the FastAPI server only

One-shot data operations (download raw data, backfill Parquet, backtest)
live in scripts/ instead of here — they're batch jobs that exit when done,
not long-running services. See scripts/download_raw.py, scripts/backfill.py,
scripts/backtest.py.
"""

from __future__ import annotations

import asyncio
import logging
import sys

import click
import structlog
import uvicorn

from alpha.config.loader import get_settings


def _configure_logging(level: str) -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )


@click.group()
def cli() -> None:
    """Alpha Runtime v2 CLI."""


@cli.command()
def run() -> None:
    """Start the full trading runtime (engines + API server in one process)."""
    settings = get_settings()
    _configure_logging(settings.runtime.log_level)

    async def _run() -> None:
        from alpha.engines.bootstrap.engine import BootstrapEngine
        from alpha.api.app import app as api_app

        engine = BootstrapEngine(settings)

        # Inject EventBus into the API app before uvicorn starts its lifespan.
        # ConnectionManager.subscribe_to_event_bus() is called in the lifespan.
        api_app.state.event_bus = engine.event_bus

        # Register bootstrap for API route access (thesis, runtime state, etc.)
        from alpha.runtime_registry import set_bootstrap
        set_bootstrap(engine)

        await engine.initialize()

        # Inject execution coordinator after initialize() so wire_execution() has run.
        coordinator = getattr(engine, "_execution_coordinator", None)
        if coordinator is not None:
            api_app.state.execution_coordinator = coordinator

        import uvicorn
        config = uvicorn.Config(
            api_app,
            host=settings.api.host,
            port=settings.api.port,
            log_level="warning",   # avoid duplicate log lines alongside the runtime
            reload=False,
        )
        server = uvicorn.Server(config)

        async def _engine_task() -> None:
            await engine.start()
            await asyncio.Event().wait()

        try:
            await asyncio.gather(_engine_task(), server.serve())
        finally:
            await engine.stop()

    asyncio.run(_run())


@cli.command()
def api() -> None:
    """Start the FastAPI server only."""
    settings = get_settings()
    _configure_logging(settings.runtime.log_level)
    uvicorn.run(
        "alpha.api.app:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=settings.api.reload,
    )


if __name__ == "__main__":
    cli()
