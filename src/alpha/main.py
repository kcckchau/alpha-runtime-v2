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
@click.option("--start", default=None, help="Start date YYYY-MM-DD (default: warmup-driven per timeframe)")
@click.option("--end", default=None, help="End date YYYY-MM-DD (default: today)")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be fetched without making IBKR requests")
def backfill(start: str | None, end: str | None, dry_run: bool) -> None:
    """Run historical backfill for configured symbols.

    Fetches only missing gaps from IBKR and writes them to the local Parquet cache.
    Running multiple times is safe: existing bars are never overwritten.

    Timeframe windows (when --start is omitted):
      1m  ~3 days    (EMA9/21 + VWAP session warmup)
      5m  ~7 days    (EMA9/21 + VWAP session warmup)
      1h  ~250 days  (EMA9/21/50 + SMA100/200)
      1d  ~1500 days (EMA9/21 + SMA50/100/200)
    """
    from alpha.config.settings import AlphaSettings
    from alpha.models.enums import RuntimeMode

    settings = get_settings()
    _configure_logging(settings.runtime.log_level)

    # Force HISTORICAL_BACKFILL mode and optionally inject explicit date range
    overrides = settings.model_dump()
    overrides["runtime"]["mode"] = RuntimeMode.HISTORICAL_BACKFILL
    if start:
        overrides["replay"]["start_date"] = start
    if end:
        overrides["replay"]["end_date"] = end
    backfill_settings = AlphaSettings.model_validate(overrides)

    if dry_run:
        from datetime import datetime, timedelta, timezone
        hist = backfill_settings.historical
        now = datetime.now(timezone.utc)
        end_dt = (
            datetime(
                backfill_settings.replay.end_date.year,
                backfill_settings.replay.end_date.month,
                backfill_settings.replay.end_date.day,
                23, 59, 59, tzinfo=timezone.utc,
            )
            if backfill_settings.replay.end_date
            else now
        )
        m1_days = max(3, hist.minute1_warmup_bars // 390 + 1)
        m5_days = max(7, hist.minute5_warmup_bars // 78 + 2)
        h1_days = int(hist.hourly_warmup_bars * 0.22) + 30
        d1_days = int(hist.daily_warmup_bars * 1.5)
        explicit_start = backfill_settings.replay.start_date
        click.echo("Dry-run backfill plan (no IBKR requests will be made):")
        click.echo(f"  Symbols : {backfill_settings.runtime.symbols}")
        click.echo(f"  End     : {end_dt.date()}")
        click.echo(f"  1m start: {(end_dt - timedelta(days=m1_days)).date()} ({m1_days}d window) explicit={explicit_start}")
        click.echo(f"  5m start: {(end_dt - timedelta(days=m5_days)).date()} ({m5_days}d window) explicit={explicit_start}")
        click.echo(f"  1h start: {(end_dt - timedelta(days=h1_days)).date()} ({h1_days}d window) explicit={explicit_start}")
        click.echo(f"  1d start: {(end_dt - timedelta(days=d1_days)).date()} ({d1_days}d window) explicit={explicit_start}")
        return

    async def _run() -> None:
        from alpha.engines.bootstrap.engine import BootstrapEngine
        engine = BootstrapEngine(backfill_settings)
        try:
            await engine.initialize()
            await engine.start()
        finally:
            await engine.stop()

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
