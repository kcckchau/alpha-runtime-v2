"""
Read-only API over the shadow research Parquet tree (src/alpha/research/).

Serves the web dashboard's research chart viewer — visualizing what
LevelObserver (Phase 0) and LevelInteractionEngine (Phase 1) recorded,
overlaid on the actual price bars. Never touches the live trading path;
this is a pure read layer over already-written Parquet files.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import APIRouter, HTTPException

from alpha.config.loader import get_settings
from alpha.research.query import build_chart_payload, list_session_dates, list_symbols

router = APIRouter(prefix="/research", tags=["research"])


def _research_root() -> Path:
    settings = get_settings()
    return Path(settings.storage.parquet_root).parent / "research"


@router.get("/symbols")
async def get_symbols() -> dict:
    """List symbols that have recorded research data."""
    loop = asyncio.get_running_loop()
    symbols = await loop.run_in_executor(None, list_symbols, _research_root())
    return {"symbols": symbols}


@router.get("/sessions")
async def get_sessions(symbol: str) -> dict:
    """List session dates available for a symbol (union of any level_observations dates)."""
    loop = asyncio.get_running_loop()
    dates = await loop.run_in_executor(None, list_session_dates, _research_root(), symbol)
    return {"symbol": symbol, "dates": dates}


@router.get("/chart")
async def get_chart(symbol: str, date: str) -> dict:
    """Chart-ready payload: reconstructed M1 bars + ORH/ORL + episodes for one session."""
    loop = asyncio.get_running_loop()
    payload = await loop.run_in_executor(
        None, build_chart_payload, _research_root(), symbol, date
    )
    if not payload["bars"]:
        raise HTTPException(
            status_code=404,
            detail=f"No research data for {symbol} on {date}. "
                   f"Run: python scripts/research_replay.py --symbol {symbol} --date {date} --fetch",
        )
    return payload
