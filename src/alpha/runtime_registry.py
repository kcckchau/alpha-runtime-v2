"""
Runtime registry — holds a reference to the live BootstrapEngine instance
so that API routes can access live engine state without circular imports.

Usage:
  # In the main entrypoint, after creating the bootstrap engine:
  from alpha.runtime_registry import set_bootstrap
  set_bootstrap(bootstrap)

  # In API routes:
  from alpha.runtime_registry import get_bootstrap
  bootstrap = get_bootstrap()
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alpha.engines.bootstrap.engine import BootstrapEngine

_bootstrap: "BootstrapEngine | None" = None


def set_bootstrap(engine: "BootstrapEngine") -> None:
    """Register the live BootstrapEngine instance."""
    global _bootstrap
    _bootstrap = engine


def get_bootstrap() -> "BootstrapEngine | None":
    """Return the live BootstrapEngine instance, or None if not yet started."""
    return _bootstrap
