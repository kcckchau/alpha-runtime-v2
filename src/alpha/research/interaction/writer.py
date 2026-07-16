"""
EpisodeWriter: persists EpisodeSummary and EpisodeBarRecord to Parquet.

Partition layout:
  {research_root}/interaction/episodes/{symbol}/session_date={YYYY-MM-DD}/part-{run_id}-{seq:06d}.parquet
  {research_root}/interaction/episode_bars/{symbol}/session_date={YYYY-MM-DD}/part-{run_id}-{seq:06d}.parquet

Episodes and their bar records are written atomically as a pair.
"""
from __future__ import annotations

import logging
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from alpha.research.interaction.models import EpisodeBarRecord, EpisodeSummary

logger = logging.getLogger(__name__)

_EPISODE_SCHEMA = pa.schema([
    pa.field("episode_id",                      pa.string()),
    pa.field("prior_episode_id",                pa.string()),      # nullable
    pa.field("level_id",                        pa.string()),
    pa.field("level_type",                      pa.string()),
    pa.field("symbol",                          pa.string()),
    pa.field("session_id",                      pa.string()),
    pa.field("session_date",                    pa.string()),
    pa.field("interaction_index",               pa.int32()),
    pa.field("started_at",                      pa.timestamp("us", tz="UTC")),
    pa.field("ended_at",                        pa.timestamp("us", tz="UTC")),  # nullable
    pa.field("pre_entry_close_side",            pa.string()),      # nullable
    pa.field("pre_entry_close_distance_ticks",  pa.int32()),       # nullable
    pa.field("approach_side",                   pa.string()),
    pa.field("bar_count",                       pa.int32()),
    pa.field("range_span_count",                 pa.int32()),
    pa.field("max_above_ticks",                 pa.int32()),
    pa.field("max_below_ticks",                 pa.int32()),
    pa.field("end_side",                        pa.string()),      # nullable
    pa.field("end_reason",                      pa.string()),
    pa.field("close_side_flip_count",           pa.int32()),
    pa.field("max_consecutive_closes_above",    pa.int32()),
    pa.field("max_consecutive_closes_below",    pa.int32()),
    pa.field("is_valid_for_research",           pa.bool_()),
    pa.field("policy_version",                  pa.string()),
    pa.field("policy_config_hash",              pa.string()),
    pa.field("geometry_version",                pa.string()),
])

_BAR_SCHEMA = pa.schema([
    pa.field("episode_id",              pa.string()),
    pa.field("bar_seq",                 pa.int32()),
    pa.field("bar_timestamp",           pa.timestamp("us", tz="UTC")),
    pa.field("symbol",                  pa.string()),
    pa.field("session_id",              pa.string()),
    pa.field("session_date",            pa.string()),
    pa.field("open_side",               pa.string()),
    pa.field("close_side",              pa.string()),
    pa.field("range_spans_level",       pa.bool_()),
    pa.field("high_distance_ticks",     pa.int32()),
    pa.field("low_distance_ticks",      pa.int32()),
    pa.field("close_distance_ticks",    pa.int32()),
    pa.field("level_value_at_timestamp", pa.string()),  # Decimal → str
    pa.field("sequence_num",            pa.int64()),    # nullable
])


class EpisodeWriter:
    def __init__(self, research_root: Path, run_id: str | None = None) -> None:
        self._root = research_root
        self._run_id = run_id or f"ep-{uuid4().hex[:8]}"

        # Buffers: (symbol, session_date) → list of row dicts
        self._ep_buf: dict[tuple, list[dict]] = defaultdict(list)
        self._bar_buf: dict[tuple, list[dict]] = defaultdict(list)

        self._seq: dict[str, int] = defaultdict(int)   # per table name
        self._episodes_written = 0
        self._bars_written = 0

    @property
    def episodes_written(self) -> int:
        return self._episodes_written

    def write(self, summary: EpisodeSummary, bars: list[EpisodeBarRecord]) -> None:
        bk = (summary.symbol, summary.session_date)
        self._ep_buf[bk].append(_summary_to_row(summary))
        for bar in bars:
            self._bar_buf[bk].append(_bar_to_row(bar))

    def flush(self) -> None:
        for bk in list(self._ep_buf.keys()):
            symbol, session_date = bk
            ep_rows = self._ep_buf.pop(bk, [])
            bar_rows = self._bar_buf.pop(bk, [])
            if not ep_rows:
                continue
            try:
                self._write_table("episodes", symbol, session_date, ep_rows, _EPISODE_SCHEMA)
                self._write_table("episode_bars", symbol, session_date, bar_rows, _BAR_SCHEMA)
                self._episodes_written += len(ep_rows)
                self._bars_written += len(bar_rows)
            except Exception:
                logger.exception("EpisodeWriter flush failed for %s/%s", symbol, session_date)
                # Re-queue on failure (same retry pattern as LevelObservationWriter)
                self._ep_buf[bk].extend(ep_rows)
                self._bar_buf[bk].extend(bar_rows)

    def _write_table(
        self,
        table_name: str,
        symbol: str,
        session_date: str,
        rows: list[dict],
        schema: pa.Schema,
    ) -> None:
        part_dir = self._root / "interaction" / table_name / symbol / f"session_date={session_date}"
        part_dir.mkdir(parents=True, exist_ok=True)

        seq_key = f"{table_name}/{symbol}/{session_date}"
        self._seq[seq_key] += 1
        seq = self._seq[seq_key]
        file_path = part_dir / f"part-{self._run_id}-{seq:06d}.parquet"

        table = pa.Table.from_pylist(rows, schema=schema)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=part_dir, suffix=".parquet.tmp")
        try:
            os.close(tmp_fd)
            pq.write_table(table, tmp_path, compression="snappy")
            os.replace(tmp_path, file_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def _summary_to_row(s: EpisodeSummary) -> dict:
    return {
        "episode_id":                      s.episode_id,
        "prior_episode_id":                s.prior_episode_id,
        "level_id":                        s.level_id,
        "level_type":                      s.level_type,
        "symbol":                          s.symbol,
        "session_id":                      s.session_id,
        "session_date":                    s.session_date,
        "interaction_index":               s.interaction_index,
        "started_at":                      s.started_at,
        "ended_at":                        s.ended_at,
        "pre_entry_close_side":            s.pre_entry_close_side,
        "pre_entry_close_distance_ticks":  s.pre_entry_close_distance_ticks,
        "approach_side":                   s.approach_side,
        "bar_count":                       s.bar_count,
        "range_span_count":                s.range_span_count,
        "max_above_ticks":                 s.max_above_ticks,
        "max_below_ticks":                 s.max_below_ticks,
        "end_side":                        s.end_side,
        "end_reason":                      s.end_reason,
        "close_side_flip_count":           s.close_side_flip_count,
        "max_consecutive_closes_above":    s.max_consecutive_closes_above,
        "max_consecutive_closes_below":    s.max_consecutive_closes_below,
        "is_valid_for_research":           s.is_valid_for_research,
        "policy_version":                  s.policy_version,
        "policy_config_hash":              s.policy_config_hash,
        "geometry_version":                s.geometry_version,
    }


def _bar_to_row(b: EpisodeBarRecord) -> dict:
    return {
        "episode_id":               b.episode_id,
        "bar_seq":                  b.bar_seq,
        "bar_timestamp":            b.bar_timestamp,
        "symbol":                   b.symbol,
        "session_id":               b.session_id,
        "session_date":             b.session_date,
        "open_side":                b.open_side,
        "close_side":               b.close_side,
        "range_spans_level":        b.range_spans_level,
        "high_distance_ticks":      b.high_distance_ticks,
        "low_distance_ticks":       b.low_distance_ticks,
        "close_distance_ticks":     b.close_distance_ticks,
        "level_value_at_timestamp": str(b.level_value_at_timestamp),
        "sequence_num":             b.sequence_num,
    }
