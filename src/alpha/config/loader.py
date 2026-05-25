"""
Config loader: constructs and caches the AlphaSettings singleton.

Call `get_settings()` everywhere; the result is cached on first call.
Tests can inject a fresh settings object via `override_settings()`.
"""

from __future__ import annotations

from functools import lru_cache

from alpha.config.settings import AlphaSettings


@lru_cache(maxsize=1)
def get_settings() -> AlphaSettings:
    return AlphaSettings()


def override_settings(settings: AlphaSettings) -> None:
    """Replace the cached singleton. Use in tests only."""
    get_settings.cache_clear()
    get_settings.__wrapped__ = lambda: settings  # type: ignore[attr-defined]
    # Simpler test pattern: monkeypatch get_settings directly.
