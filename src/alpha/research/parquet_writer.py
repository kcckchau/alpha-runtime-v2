"""
Buffered Parquet writer for LevelBarObservation.

Partition layout (research-specific, separate from production storage):
  {research_root}/level_observations/{symbol}/session_date={YYYY-MM-DD}/
      part-{run_id}-{seq:06d}.parquet

Design rules:
  - Rows accumulate in memory until flush() is called or buffer hits max_buffer_rows.
  - flush() writes ONE part file per (symbol, session_date) batch and is idempotent
    on an empty buffer.
  - On write failure: buffer is retained, error is logged, retry on next flush().
    Rows are only dropped after pending count exceeds max_pending_rows.
  - File names embed run_id and a monotonic sequence so restarts never overwrite
    existing files from the same or prior sessions.
  - Writes are atomic: temp file → os.replace().
  - One writer instance is shared across all symbols; partitioning is handled
    internally per flush batch.
"""

from __future__ import annotations

import logging
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from alpha.research.models import LevelBarObservation

logger = logging.getLogger(__name__)

# PyArrow schema for LevelBarObservation.
# Must stay in sync with models.LevelBarObservation field order.
_ARROW_SCHEMA = pa.schema([
    pa.field("obs_id",                        pa.string()),
    pa.field("run_id",                        pa.string()),
    pa.field("run_mode",                      pa.string()),
    pa.field("source_bar_id",                 pa.string()),   # nullable
    pa.field("symbol",                        pa.string()),
    pa.field("session_date",                  pa.string()),
    pa.field("bar_timestamp",                 pa.timestamp("us", tz="UTC")),
    pa.field("level_type",                    pa.string()),
    pa.field("level_value",                   pa.string()),   # Decimal → string for lossless repr
    pa.field("open_distance_ticks",           pa.int32()),
    pa.field("high_distance_ticks",           pa.int32()),
    pa.field("low_distance_ticks",            pa.int32()),
    pa.field("close_distance_ticks",          pa.int32()),
    pa.field("bar_overlaps_level",            pa.bool_()),
    pa.field("open_side",                     pa.string()),
    pa.field("close_side",                    pa.string()),
    pa.field("proximity_band_ticks",          pa.int32()),
    pa.field("volatility_reference",          pa.string()),   # Decimal → string
    pa.field("volatility_definition_version", pa.string()),
    pa.field("tick_size",                     pa.string()),   # Decimal → string
    pa.field("session_phase",                 pa.string()),
    pa.field("orb_state",                     pa.string()),
    pa.field("schema_version",                pa.string()),
    pa.field("producer_version",              pa.string()),
    pa.field("data_quality",                  pa.string()),
])

# Batch key: group rows by (symbol, session_date) so each part file is
# confined to one partition.
_BatchKey = tuple[str, str]  # (symbol, session_date)


def _obs_to_row(obs: "LevelBarObservation") -> dict:
    return {
        "obs_id":                        str(obs.obs_id),
        "run_id":                        obs.run_id,
        "run_mode":                      str(obs.run_mode),
        "source_bar_id":                 obs.source_bar_id,
        "symbol":                        obs.symbol,
        "session_date":                  obs.session_date,
        "bar_timestamp":                 obs.bar_timestamp,
        "level_type":                    str(obs.level_type),
        "level_value":                   str(obs.level_value),
        "open_distance_ticks":           obs.open_distance_ticks,
        "high_distance_ticks":           obs.high_distance_ticks,
        "low_distance_ticks":            obs.low_distance_ticks,
        "close_distance_ticks":          obs.close_distance_ticks,
        "bar_overlaps_level":            obs.bar_overlaps_level,
        "open_side":                     str(obs.open_side),
        "close_side":                    str(obs.close_side),
        "proximity_band_ticks":          obs.proximity_band_ticks,
        "volatility_reference":          str(obs.volatility_reference),
        "volatility_definition_version": obs.volatility_definition_version,
        "tick_size":                     str(obs.tick_size),
        "session_phase":                 obs.session_phase,
        "orb_state":                     obs.orb_state,
        "schema_version":                obs.schema_version,
        "producer_version":              obs.producer_version,
        "data_quality":                  obs.data_quality,
    }


class LevelObservationWriter:
    """
    Accumulates LevelBarObservation rows and writes them to partitioned Parquet.

    Thread safety: not thread-safe. Intended for use from a single asyncio
    event loop; flush() may be called from engine shutdown.
    """

    def __init__(
        self,
        research_root: Path,
        run_id: str | None = None,
        max_buffer_rows: int = 500,
        max_pending_rows: int = 10_000,
    ) -> None:
        from uuid import uuid4
        self._root = research_root
        self._run_id = run_id or f"auto-{uuid4().hex[:8]}"
        self._max_buffer = max_buffer_rows
        self._max_pending = max_pending_rows

        # Buffer: _BatchKey → list of row dicts
        self._pending: dict[_BatchKey, list[dict]] = defaultdict(list)
        self._pending_count: int = 0

        # Seen obs_ids within this run — prevents duplicate rows from replayed
        # or double-delivered bars. Reset is intentionally NOT done on flush;
        # the set is bounded by session size (~1200 entries per RTH session).
        self._seen_obs_ids: set[str] = set()

        # Per-partition file sequence counter — monotonically increasing so
        # restarts never collide with prior part files.
        self._seq: dict[_BatchKey, int] = defaultdict(int)

        self._rows_written: int = 0
        self._flush_errors: int = 0

    @property
    def flush_errors(self) -> int:
        return self._flush_errors

    # ── Public API ────────────────────────────────────────────────────────────

    def add(self, obs: "LevelBarObservation") -> None:
        """Add one observation to the buffer.

        Silently deduplicates based on obs_id (which is deterministic, so
        replaying identical bars produces identical obs_ids).
        Triggers an automatic flush when max_buffer_rows is reached.
        """
        obs_id_str = str(obs.obs_id)
        if obs_id_str in self._seen_obs_ids:
            return
        self._seen_obs_ids.add(obs_id_str)

        key: _BatchKey = (obs.symbol, obs.session_date)
        self._pending[key].append(_obs_to_row(obs))
        self._pending_count += 1

        if self._pending_count >= self._max_buffer:
            self.flush()

    def flush(self) -> None:
        """Write all buffered rows to Parquet and clear the buffer.

        On write failure: retains rows, logs error, increments error counter.
        After max_pending_rows are retained without a successful flush, the
        oldest batch is dropped to prevent unbounded memory growth.
        """
        if self._pending_count == 0:
            return

        failed_keys: list[_BatchKey] = []
        written_keys: list[_BatchKey] = []

        for key, rows in self._pending.items():
            if not rows:
                written_keys.append(key)
                continue
            symbol, session_date = key
            try:
                self._write_partition(symbol, session_date, rows)
                written_keys.append(key)
                self._rows_written += len(rows)
                logger.debug(
                    "LevelObservationWriter: wrote %d rows → %s/%s",
                    len(rows), symbol, session_date,
                )
            except Exception:
                self._flush_errors += 1
                logger.exception(
                    "LevelObservationWriter: flush failed for %s/%s (%d rows retained, "
                    "flush_errors=%d)",
                    symbol, session_date, len(rows), self._flush_errors,
                )
                failed_keys.append(key)

        for key in written_keys:
            self._pending_count -= len(self._pending.pop(key, []))

        # Overflow guard: if retained rows exceed max_pending, drop oldest batches.
        if self._pending_count > self._max_pending:
            drop_count = 0
            for key in list(self._pending.keys()):
                if self._pending_count <= self._max_pending // 2:
                    break
                dropped = self._pending.pop(key, [])
                drop_count += len(dropped)
                self._pending_count -= len(dropped)
            logger.error(
                "LevelObservationWriter: pending rows exceeded max_pending=%d — "
                "dropped %d rows to prevent memory growth",
                self._max_pending, drop_count,
            )

    def reset_session(self) -> None:
        """Call at session rollover to flush pending rows and reset dedup set.

        The dedup set is keyed per run_id, so resetting at session boundaries
        prevents unbounded growth while keeping intra-session dedup intact.
        """
        self.flush()
        self._seen_obs_ids.clear()

    @property
    def pending_count(self) -> int:
        return self._pending_count

    @property
    def rows_written(self) -> int:
        return self._rows_written

    # ── Internal ──────────────────────────────────────────────────────────────

    def _write_partition(self, symbol: str, session_date: str, rows: list[dict]) -> None:
        """Write rows as one part file under the correct partition directory.

        Files are written in chunks of max_buffer_rows so no single part file
        exceeds that limit regardless of input batch size.
        """
        key: _BatchKey = (symbol, session_date)
        partition_dir = self._root / "level_observations" / symbol / f"session_date={session_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Split into chunks of max_buffer_rows to honour the per-file row limit.
        chunk_size = self._max_buffer
        for chunk_start in range(0, len(rows), chunk_size):
            chunk = rows[chunk_start: chunk_start + chunk_size]
            self._seq[key] += 1
            seq = self._seq[key]
            file_name = f"part-{self._run_id}-{seq:06d}.parquet"
            file_path = partition_dir / file_name

            table = pa.Table.from_pylist(chunk, schema=_ARROW_SCHEMA)

            tmp_fd, tmp_path_str = tempfile.mkstemp(
                dir=partition_dir, suffix=".parquet.tmp"
            )
            try:
                os.close(tmp_fd)
                pq.write_table(table, tmp_path_str, compression="snappy")
                os.replace(tmp_path_str, file_path)
            except Exception:
                try:
                    os.unlink(tmp_path_str)
                except OSError:
                    pass
                raise
