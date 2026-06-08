"""
Parquet storage backend.

Partition layout:
  {parquet_root}/{data_type}/{symbol}/year={YYYY}/month={MM}/day={DD}/data.parquet

data_type: bars | trades | quotes | snapshots | market_states | setups | orders
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

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
    ) -> Path:
        path = _partition_path(self._root, data_type, symbol, d)
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / "data.parquet"
        if file_path.exists():
            try:
                existing = pq.ParquetFile(file_path).read()
                table = pa.concat_tables([existing, table], promote_options="default")
            except Exception:
                # File is corrupted or empty (e.g. interrupted write). Discard it
                # and overwrite with the incoming table only.
                logger.warning(
                    "Corrupted Parquet file discarded and replaced: %s", file_path
                )
                file_path.unlink(missing_ok=True)
        pq.write_table(
            table,
            file_path,
            compression=self._compress,
            row_group_size=self._row_group_size,
        )
        logger.debug("Wrote %d rows → %s", len(table), file_path)
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
        return pq.read_table(path, columns=columns)

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
