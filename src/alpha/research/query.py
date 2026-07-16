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

from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

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

    Research replay is intentionally append-only, so rerunning the same
    deterministic replay produces new UUIDs for logically identical episodes.
    The UUID is therefore not suitable for read-side identity. This key retains
    every attribute that defines the actual interaction and its policy version.
    """
    return (
        row["symbol"],
        row["session_id"],
        row["level_id"],
        row["interaction_index"],
        row["started_at"],
        row["ended_at"],
        row["policy_config_hash"],
        row["geometry_version"],
    )


def build_chart_payload(research_root: Path, symbol: str, session_date: str) -> dict[str, Any]:
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

    episodes: list[dict[str, Any]] = []
    for row in sorted(unique_episode_rows.values(), key=lambda r: r["started_at"]):
        ep = _episode_row_to_dict(row)
        ep["bars"] = bars_by_episode.get(row["episode_id"], [])
        episodes.append(ep)

    return {
        "symbol": symbol,
        "session_date": session_date,
        "bars": bars,
        "orb_high": float(static_levels["orh"]) if static_levels["orh"] is not None else None,
        "orb_low": float(static_levels["orl"]) if static_levels["orl"] is not None else None,
        "episodes": episodes,
    }
