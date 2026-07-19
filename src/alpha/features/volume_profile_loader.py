"""
VolumeProfileLoader — loads pre-built volume profiles from JSON at session open.

Looks up the prior RTH session's profile and the overnight Globex profile for a
given session date. Profiles are built offline by build_volume_profiles.py and
stored as JSON in data/volume_profiles/{symbol}/{date}_{type}.json.

Prior session logic:
    - RTH profile: the most recent available RTH profile with date < session_date.
      (Skips weekends and holidays automatically by searching backwards.)
    - Globex profile: the Globex profile keyed to session_date itself (covers
      18:00 ET prior evening → 09:30 ET current morning).

Returns None for either profile when the file does not exist, so callers can
gate level features cleanly without crashing during the first session of a
new data set.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from alpha.models.volume_profile import VolumeProfile


def _serialize_profile(profile: VolumeProfile) -> dict:
    """Serialize a VolumeProfile to a JSON-safe dict."""
    data = profile.model_dump()
    data["poc"] = str(data["poc"])
    data["vah"] = str(data["vah"])
    data["val"] = str(data["val"])
    data["hvn_levels"] = [str(x) for x in data["hvn_levels"]]
    data["lvn_levels"] = [str(x) for x in data["lvn_levels"]]
    data["session_date"] = str(data["session_date"])
    return data


class VolumeProfileLoader:
    def __init__(self, profiles_dir: Path, max_lookback_days: int = 10) -> None:
        """
        profiles_dir: root of volume_profiles/ directory.
        max_lookback_days: how far back to search for the prior RTH profile.
        """
        self.profiles_dir = profiles_dir
        self.max_lookback_days = max_lookback_days

    def load_prior_rth(self, symbol: str, session_date: date) -> VolumeProfile | None:
        """Return the most recent RTH profile before session_date."""
        for offset in range(1, self.max_lookback_days + 1):
            candidate = session_date - timedelta(days=offset)
            profile = self._load(symbol, candidate, "rth")
            if profile is not None:
                return profile
        return None

    def load_globex(self, symbol: str, session_date: date) -> VolumeProfile | None:
        """Return the Globex profile for the overnight session leading into session_date."""
        return self._load(symbol, session_date, "globex")

    def load_session_pair(
        self, symbol: str, session_date: date
    ) -> tuple[VolumeProfile | None, VolumeProfile | None]:
        """Return (prior_rth, globex) — convenience for session open."""
        return self.load_prior_rth(symbol, session_date), self.load_globex(symbol, session_date)

    def save(self, symbol: str, profile: VolumeProfile) -> None:
        """Persist a VolumeProfile to the standard JSON path."""
        path = self._profile_path(symbol, profile.session_date, profile.session_type)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(_serialize_profile(profile), f, indent=2)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _profile_path(self, symbol: str, d: date, session_type: str) -> Path:
        return self.profiles_dir / symbol / f"{d}_{session_type}.json"

    def _load(self, symbol: str, d: date, session_type: str) -> VolumeProfile | None:
        path = self._profile_path(symbol, d, session_type)
        if not path.exists():
            return None
        with open(path) as f:
            data = json.load(f)
        # Deserialize string fields back to Decimal
        data["poc"] = Decimal(data["poc"])
        data["vah"] = Decimal(data["vah"])
        data["val"] = Decimal(data["val"])
        data["hvn_levels"] = [Decimal(x) for x in data["hvn_levels"]]
        data["lvn_levels"] = [Decimal(x) for x in data["lvn_levels"]]
        data.setdefault("source", "bars")  # backwards-compat: files built before source field was added
        return VolumeProfile(**data)
