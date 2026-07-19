"""
Read-only query helpers over the research Parquet tree.

Used by the /research API routes to serve chart-ready payloads for the web
dashboard's research viewer. Never used by any live trading path — this
module only reads what LevelObserver / LevelInteractionEngine already wrote
(see src/alpha/research/level_observer.py, interaction/engine.py).

M1 OHLC reconstruction:
  LevelBarObservation does not store bar OHLC directly (see DESIGN.md) — it
  stores signed tick distances from each level. The VWAP row is present on
  every bar with volume > 0, so open/high/low/close are recovered as
  `level_value + distance_ticks * tick_size` from that row. This mirrors the
  approach already used by scripts/export_research_bundle.py.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from alpha.features.volume_profile_loader import VolumeProfileLoader

# Separation threshold (episode CLOSE boundary) mirrored from
# LevelDistanceConfig defaults in interaction/config.py — keep these two in
# sync if that config ever changes. Phase 0 (LevelBarObservation) only stores
# `proximity_band_ticks` (the ENTRY threshold, 0.20x ATR) continuously for
# every bar; separation (0.50x ATR, the wider EXIT threshold, must hold for
# min_separation_bars=3 consecutive bars) is only recorded by Phase 1 inside
# active episode windows. To draw a continuous separation band across the
# whole session we recompute it here from the same `volatility_reference`
# (ATR-14) and `tick_size` every LevelBarObservation row already carries.
_SEPARATION_ATR_FRACTION = Decimal("0.50")
_FALLBACK_SEPARATION_TICKS = 25


def _read_partition_dir(part_dir: Path) -> list[dict[str, Any]]:
    if not part_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for f in sorted(part_dir.glob("*.parquet")):
        rows.extend(pq.ParquetFile(str(f)).read().to_pylist())
    return rows


def list_symbols(research_root: Path, dataset: str = "level_observations") -> list[str]:
    """Return symbols with recorded research observations."""
    base = research_root / dataset
    if not base.exists():
        return []
    return sorted(child.name for child in base.iterdir() if child.is_dir())


def list_session_dates(research_root: Path, symbol: str, dataset: str = "level_observations") -> list[str]:
    """Return sorted session_date strings available for a symbol under a dataset.

    dataset: "level_observations" (Phase 0) — the superset; episodes/episode_bars
    are always a subset of dates that have level_observations.
    """
    base = research_root / dataset / symbol
    if not base.exists():
        return []
    dates = []
    for child in base.iterdir():
        if child.is_dir() and child.name.startswith("session_date="):
            dates.append(child.name.split("=", 1)[1])
    return sorted(dates)


def _separation_ticks(atr_14: Decimal | None, tick_size: Decimal) -> int:
    """Mirrors LevelDistanceConfig.separation_ticks() exactly (same truncating
    int() cast, not rounding) so the drawn band matches the real threshold."""
    if atr_14 is None or atr_14 == 0:
        return _FALLBACK_SEPARATION_TICKS
    raw = float(atr_14) * float(_SEPARATION_ATR_FRACTION) / float(tick_size)
    return max(1, int(raw))


def _reconstruct_bars(obs_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Decimal | None]]:
    """Group LevelBarObservation rows by bar_timestamp and reconstruct OHLCV.

    Returns (bars, static_levels) where static_levels has the last-seen
    orh/orl values for the session (None if never locked).
    """
    by_ts: dict[Any, dict[str, dict[str, Any]]] = {}
    for row in obs_rows:
        by_ts.setdefault(row["bar_timestamp"], {})[row["level_type"]] = row

    static_levels: dict[str, Decimal | None] = {"orh": None, "orl": None}
    bars: list[dict[str, Any]] = []

    for ts in sorted(by_ts.keys()):
        levels = by_ts[ts]
        vwap_row = levels.get("vwap")
        if vwap_row is None:
            # No volume this bar (should not happen in practice) — skip; we
            # have no anchor to reconstruct OHLC from.
            continue

        tick_size = Decimal(vwap_row["tick_size"])
        level_value = Decimal(vwap_row["level_value"])

        def price_at(distance_field: str) -> float:
            return float(level_value + Decimal(vwap_row[distance_field]) * tick_size)

        rth_vwap_row = levels.get("rth_vwap")
        proximity_ticks = int(vwap_row["proximity_band_ticks"])
        atr_14 = Decimal(vwap_row["volatility_reference"]) if vwap_row.get("volatility_reference") else None
        separation_ticks = _separation_ticks(atr_14, tick_size)

        bars.append({
            "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            "open": price_at("open_distance_ticks"),
            "high": price_at("high_distance_ticks"),
            "low": price_at("low_distance_ticks"),
            "close": price_at("close_distance_ticks"),
            "vwap": float(level_value),
            "rth_vwap": float(rth_vwap_row["level_value"]) if rth_vwap_row is not None else None,
            # ATR-derived "watch" band radius in ticks — same definition used by
            # LevelObserver/_proximity_band for every level type on this bar
            # (it's a function of ATR/tick_size only, not level-specific), so
            # one value covers vwap/rth_vwap/orh/orl bands for this timestamp.
            # This is the episode ENTRY threshold only — see separation_ticks
            # below for the (wider) EXIT threshold.
            "proximity_ticks": proximity_ticks,
            "separation_ticks": separation_ticks,
            "tick_size": float(tick_size),
            "session_phase": vwap_row.get("session_phase"),
            "orb_state": vwap_row.get("orb_state"),
        })

        for level_type in ("orh", "orl"):
            row = levels.get(level_type)
            if row is not None:
                static_levels[level_type] = Decimal(row["level_value"])

    return bars, static_levels


def _episode_row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("started_at", "ended_at"):
        v = out.get(key)
        if v is not None and hasattr(v, "isoformat"):
            out[key] = v.isoformat()
    return out


def _bar_record_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    if hasattr(out.get("bar_timestamp"), "isoformat"):
        out["bar_timestamp"] = out["bar_timestamp"].isoformat()
    for key in ("level_value_at_timestamp", "atr_14"):
        if out.get(key) is not None:
            out[key] = float(out[key])
    return out


def _episode_identity(row: dict[str, Any]) -> tuple[Any, ...]:
    """Return the stable identity of an interaction episode.

    Research replay is intentionally append-only and assigns UUIDs (session_id,
    level_id, episode_id) and interaction_index fresh each run — none of these
    are stable across runs. Two episodes from different runs are logically
    identical iff they share the same symbol, level geometry (level_type +
    session_scope), timestamp window, and policy version.
    """
    return (
        row["symbol"],
        row["level_type"],
        row["session_scope"],
        row["started_at"],
        row["ended_at"],
        row["policy_config_hash"],
        row["geometry_version"],
    )


def _load_vp(
    profiles_root: Path | None,
    symbol: str,
    session_date_str: str,
) -> tuple[dict | None, dict | None]:
    """Load prior RTH and Globex VP profiles for the session. Returns (rth, globex) dicts."""
    if profiles_root is None:
        return None, None
    loader = VolumeProfileLoader(profiles_root)
    d = date.fromisoformat(session_date_str)
    rth_profile = loader.load_prior_rth(symbol, d)
    globex_profile = loader.load_globex(symbol, d)

    def _to_dict(p: Any) -> dict | None:
        if p is None:
            return None
        return {
            "poc": float(p.poc),
            "vah": float(p.vah),
            "val": float(p.val),
            "hvn_levels": [float(x) for x in p.hvn_levels],
            "lvn_levels": [float(x) for x in p.lvn_levels],
            "source": p.source,
        }

    return _to_dict(rth_profile), _to_dict(globex_profile)


def build_chart_payload(
    research_root: Path,
    symbol: str,
    session_date: str,
    profiles_root: Path | None = None,
) -> dict[str, Any]:
    """Build the full chart-ready payload for one symbol/session_date.

    Shape:
      {
        "symbol", "session_date",
        "bars": [{timestamp, open, high, low, close, vwap, session_phase, orb_state}, ...],
        "orb_high": float | None, "orb_low": float | None,
        "episodes": [
          {..EpisodeSummary fields.., "bars": [..EpisodeBarRecord fields..]},
          ...
        ],
      }
    """
    obs_rows = _read_partition_dir(
        research_root / "level_observations" / symbol / f"session_date={session_date}"
    )
    bars, static_levels = _reconstruct_bars(obs_rows)

    episode_rows = _read_partition_dir(
        research_root / "interaction" / "episodes" / symbol / f"session_date={session_date}"
    )
    bar_rows = _read_partition_dir(
        research_root / "interaction" / "episode_bars" / symbol / f"session_date={session_date}"
    )

    bars_by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in bar_rows:
        bars_by_episode.setdefault(row["episode_id"], []).append(_bar_record_to_dict(row))
    for episode_bars in bars_by_episode.values():
        episode_bars.sort(key=lambda r: r["bar_seq"])

    # A replay is append-only and UUIDs are generated per run. Collapse exact
    # logical reruns here so rerunning a date cannot multiply chart markers or
    # bias analysis. Keep the first sorted file's row; duplicate runs have the
    # same episode/bar geometry by definition of _episode_identity().
    unique_episode_rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in episode_rows:
        unique_episode_rows.setdefault(_episode_identity(row), row)

    # Re-number interaction_index sequentially per (level_type, session_scope)
    # after dedup — the original index is assigned per-run and is not stable.
    level_counters: dict[tuple[str, str], int] = {}
    episodes: list[dict[str, Any]] = []
    for row in sorted(unique_episode_rows.values(), key=lambda r: r["started_at"]):
        key = (row["level_type"], row["session_scope"])
        level_counters[key] = level_counters.get(key, 0) + 1
        ep = _episode_row_to_dict(row)
        ep["interaction_index"] = level_counters[key]
        ep["bars"] = bars_by_episode.get(row["episode_id"], [])
        episodes.append(ep)

    vp_rth, vp_globex = _load_vp(profiles_root, symbol, session_date)

    return {
        "symbol": symbol,
        "session_date": session_date,
        "bars": bars,
        "orb_high": float(static_levels["orh"]) if static_levels["orh"] is not None else None,
        "orb_low": float(static_levels["orl"]) if static_levels["orl"] is not None else None,
        "episodes": episodes,
        "vp_rth": vp_rth,
        "vp_globex": vp_globex,
    }
