"""
Measure how long a single Databento historical fetch takes for each timeframe
and bar count we actually use during bootstrap catchup.

Notes:
  - No ohlcv-5m schema — M5 bars must be derived from M1 (resample).
  - Each schema has its own available end; use conservative per-schema ends
    to avoid 422 data_schema_not_fully_available errors.

Usage:
    python scripts/test_catchup_fetch_time.py
"""

import databento as db
import os
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

api_key = os.environ["DATABENTO__API_KEY"]
hist = db.Historical(api_key)

SYMBOL = "MNQ.c.0"
DATASET = "GLBX.MDP3"

info = hist.metadata.get_dataset_range(dataset=DATASET)
available_end = datetime.fromisoformat(info["end"].replace("Z", "+00:00"))
now = datetime.now(timezone.utc)
print(f"Available end (M1): {available_end.isoformat()}")
print(f"Historical lag:     {(now - available_end).total_seconds() / 60:.1f} min\n")

# H1/D1 available end lags behind M1 — truncate to safe boundaries
h1_end = available_end.replace(minute=0, second=0, microsecond=0)
d1_end = available_end.replace(hour=0, minute=0, second=0, microsecond=0)

cases = [
    # (label, schema, start, end, target_bars)
    ("M1  3 days",   "ohlcv-1m", available_end - timedelta(days=3),   available_end, 300),
    ("H1  8 months", "ohlcv-1h", available_end - timedelta(days=250),  h1_end,        1000),
    ("D1  4 years",  "ohlcv-1d", available_end - timedelta(days=1460), d1_end,        1000),
]

for label, schema, start_dt, end_dt, target_bars in cases:
    t0 = time.perf_counter()
    try:
        bars = list(hist.timeseries.get_range(
            dataset=DATASET,
            schema=schema,
            symbols=[SYMBOL],
            start=start_dt.isoformat(),
            end=end_dt.isoformat(),
            stype_in="continuous",
        ))
        elapsed = time.perf_counter() - t0
        print(
            f"{label:14s} | bars={len(bars):5d} (target={target_bars}) "
            f"| elapsed={elapsed:.2f}s"
        )
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"{label:14s} | ERROR after {elapsed:.2f}s: {e}")

print("\nNote: M5 bars are not available from Databento — must be resampled from M1.")
