"""
Parquet storage backend.

Partition layout:
  {parquet_root}/{data_type}/{symbol}/year={YYYY}/month={MM}/day={DD}/data.parquet

data_type: bars | trades | quotes | snapshots | market_states | setups | orders
"""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
from collections.abc import Iterable
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from alpha.config.settings import StorageSettings

logger = logging.getLogger(__name__)


def _partition_path(
    root: Path,
    data_type: str,
    symbol: str,
    d: date,
) -> Path:
    return root / data_type / symbol / f"year={d.year}" / f"month={d.month:02d}" / f"day={d.day:02d}"


class ParquetStore:
    """
    Low-level Parquet read/write for any event type.

    Callers pass PyArrow Tables; schema enforcement is handled per data type
    in the StorageEngine using typed Pydantic → Arrow converters.
    """

    def __init__(self, settings: StorageSettings) -> None:
        self._root = settings.parquet_root
        self._compress = settings.compress
        self._row_group_size = settings.row_group_size

    def write(
        self,
        table: pa.Table,
        data_type: str,
        symbol: str,
        d: date,
        dedup_key: str | None = None,
        prefer_live: bool = False,
    ) -> Path:
        """Write table to Parquet partition, appending to any existing data.

        If dedup_key is provided, rows in the existing file whose dedup_key
        value appears in the new table are removed before concatenation — i.e.
        new rows win (upsert semantics). Use this for event types where the
        same logical record may be written more than once (e.g. setups on restart).

        If prefer_live is also set, an existing row with is_replay=False is
        protected from being overwritten by an incoming row for the same key —
        live rows are the authoritative source for ingestion metadata (e.g.
        received_at, used for feed latency stats), which a later replay/backfill
        rewrite of the same logical record would otherwise silently discard.
        """
        path = _partition_path(self._root, data_type, symbol, d)
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / "data.parquet"
        if file_path.exists():
            try:
                existing = pq.ParquetFile(file_path).read()
                has_dedup_key = (
                    dedup_key is not None
                    and dedup_key in existing.schema.names
                    and dedup_key in table.schema.names
                )
                if has_dedup_key:
                    import pyarrow.compute as pc
                    if (
                        prefer_live
                        and "is_replay" in existing.schema.names
                        and "is_replay" in table.schema.names
                    ):
                        live_rows = existing.filter(pc.invert(existing.column("is_replay")))
                        live_keys = live_rows.column(dedup_key)
                        new_key_col = table.column(dedup_key)
                        table = table.filter(pc.invert(pc.is_in(new_key_col, value_set=live_keys)))
                    # Remove existing rows whose dedup_key appears in the (possibly
                    # now-filtered) new table.
                    new_keys = table.column(dedup_key)
                    mask = pc.invert(pc.is_in(existing.column(dedup_key), value_set=new_keys))
                    existing = existing.filter(mask)
                # Use "permissive" so null-typed columns (e.g. optional string fields
                # that happen to be None on the first write) merge cleanly with typed
                # columns written in subsequent events.
                table = pa.concat_tables([existing, table], promote_options="permissive")
            except Exception as exc:
                logger.warning(
                    "Corrupted Parquet file discarded and replaced: %s (%s: %s)",
                    file_path,
                    type(exc).__name__,
                    exc,
                )
                file_path.unlink(missing_ok=True)

        # Write atomically: write to a temp file in the same directory, then rename.
        # This ensures the file is never in a partially-written state if the process
        # is interrupted or if a concurrent reader observes the file mid-write.
        tmp_fd, tmp_path = tempfile.mkstemp(dir=path, suffix=".parquet.tmp")
        try:
            os.close(tmp_fd)
            pq.write_table(
                table,
                tmp_path,
                compression=self._compress,
                row_group_size=self._row_group_size,
            )
            os.replace(tmp_path, file_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        logger.debug("Wrote %d rows → %s", len(table), file_path)
        return file_path

    def write_streaming(
        self,
        table_chunks: Iterable[pa.Table],
        data_type: str,
        symbol: str,
        d: date,
    ) -> Path:
        """Write a very large dataset without ever materializing it as one
        in-memory table — for data types with no per-row dedup key (quotes),
        where a bulk write always starts from a clean file (day-level skip
        in BackfillEngine, or clear_quotes() on --force), so there's no
        existing data to merge against.

        Mirrors Databento's own DBNStore.to_parquet() implementation: small
        chunks, one pyarrow.parquet.ParquetWriter kept open for the whole
        file, each chunk written as its own row group, never concatenated.
        Two earlier attempts that instead built one giant pa.Table before
        writing (list-of-dicts, then chunked-but-still-concatenated) drove
        RSS to 55GB and then 67GB on a real 43M-row quotes day and had to
        be killed — this never holds more than one chunk in memory.

        Does NOT support dedup_key/prefer_live — if a caller needs upsert
        semantics against existing data, use write() instead.
        """
        path = _partition_path(self._root, data_type, symbol, d)
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / "data.parquet"

        tmp_fd, tmp_path = tempfile.mkstemp(dir=path, suffix=".parquet.tmp")
        os.close(tmp_fd)
        writer: pq.ParquetWriter | None = None
        total_rows = 0
        try:
            for chunk in table_chunks:
                if chunk.num_rows == 0:
                    continue
                if writer is None:
                    writer = pq.ParquetWriter(
                        tmp_path,
                        schema=chunk.schema,
                        compression=self._compress,
                    )
                writer.write_table(chunk, row_group_size=self._row_group_size)
                total_rows += chunk.num_rows
            if writer is not None:
                writer.close()
                writer = None
                os.replace(tmp_path, file_path)
            else:
                # No rows at all — nothing to write, clean up the empty temp file.
                os.unlink(tmp_path)
        except Exception:
            if writer is not None:
                with contextlib.suppress(Exception):
                    writer.close()
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise

        logger.debug("Wrote %d rows (streamed) → %s", total_rows, file_path)
        return file_path

    def read(
        self,
        data_type: str,
        symbol: str,
        d: date,
        columns: list[str] | None = None,
    ) -> pa.Table | None:
        path = _partition_path(self._root, data_type, symbol, d) / "data.parquet"
        if not path.exists():
            return None
        try:
            return pq.read_table(path, columns=columns)
        except Exception:
            logger.warning(
                "Corrupted Parquet file (treating as missing): %s", path
            )
            return None

    def read_range(
        self,
        data_type: str,
        symbol: str,
        start: date,
        end: date,
        columns: list[str] | None = None,
    ) -> pa.Table:
        tables: list[pa.Table] = []
        from datetime import timedelta
        current = start
        while current <= end:
            t = self.read(data_type, symbol, current, columns)
            if t is not None:
                tables.append(t)
            current += timedelta(days=1)
        if not tables:
            return pa.table({})
        return pa.concat_tables(tables, promote_options="permissive")

    def exists(self, data_type: str, symbol: str, d: date) -> bool:
        return (_partition_path(self._root, data_type, symbol, d) / "data.parquet").exists()

    def delete(self, data_type: str, symbol: str, d: date) -> None:
        """Remove a partition's file, if present.

        Used for data types with no natural per-row dedup key (e.g. quotes,
        where a tick has no stable identity to upsert on) — a forced refetch
        deletes the old file first so the next write starts clean instead of
        appending duplicate rows on top of the old ones.
        """
        (_partition_path(self._root, data_type, symbol, d) / "data.parquet").unlink(missing_ok=True)

    def list_dates(self, data_type: str, symbol: str) -> list[date]:
        base = self._root / data_type / symbol
        if not base.exists():
            return []
        dates: list[date] = []
        for year_dir in sorted(base.glob("year=*")):
            for month_dir in sorted(year_dir.glob("month=*")):
                for day_dir in sorted(month_dir.glob("day=*")):
                    try:
                        y = int(year_dir.name.split("=")[1])
                        m = int(month_dir.name.split("=")[1])
                        d_val = int(day_dir.name.split("=")[1])
                        dates.append(date(y, m, d_val))
                    except (ValueError, IndexError):
                        pass
        return dates
