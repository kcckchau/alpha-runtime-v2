"""
Alpha Runtime v2 — entrypoint.

Commands:
  alpha run       Start the full runtime (mode from config)
  alpha api       Start the FastAPI server only
  alpha backfill  Run historical backfill for configured symbols/dates
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
    """Start the full trading runtime."""
    settings = get_settings()
    _configure_logging(settings.runtime.log_level)

    async def _run() -> None:
        from alpha.engines.bootstrap.engine import BootstrapEngine
        engine = BootstrapEngine(settings)
        try:
            await engine.initialize()
            await engine.start()
            await asyncio.Event().wait()
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


@cli.command()
@click.option("--start", required=True, help="Start date YYYY-MM-DD")
@click.option("--end", required=True, help="End date YYYY-MM-DD")
def backfill(start: str, end: str) -> None:
    """Run historical backfill for configured symbols."""
    settings = get_settings()
    _configure_logging(settings.runtime.log_level)
    click.echo(f"Backfill {start} → {end} for {settings.runtime.symbols}")
    # TODO: drive HistoricalDataEngine in HISTORICAL_BACKFILL mode


if __name__ == "__main__":
    cli()
