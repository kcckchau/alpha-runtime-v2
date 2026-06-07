from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str
    version: str = "0.1.0"


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/", response_model=HealthResponse)
async def root() -> HealthResponse:
    """Root handler — silences browser/proxy GET / probes."""
    return HealthResponse(status="ok")
