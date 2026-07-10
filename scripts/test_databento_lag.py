import databento as db
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import os

load_dotenv()

api_key = os.environ["DATABENTO__API_KEY"]
now = datetime.now(timezone.utc)

hist = db.Historical(api_key)

info = hist.metadata.get_dataset_range(dataset="GLBX.MDP3")
available_end = datetime.fromisoformat(info["end"].replace("Z", "+00:00"))
print(f"Dataset range:  start={info['start']}  end={info['end']}")
print(f"Historical lag: {(now - available_end).total_seconds() / 60:.1f} minutes behind now\n")

# ── Historical API lag per schema ─────────────────────────────────────────────
schemas = [
    ("ohlcv-1m", timedelta(hours=2),  33, "1m"),
    ("ohlcv-1h", timedelta(days=3),   34, "1h"),
    ("ohlcv-1d", timedelta(days=30),  35, "1d"),
]

for schema, lookback, rtype_int, label in schemas:
    try:
        bars = list(hist.timeseries.get_range(
            dataset="GLBX.MDP3",
            schema=schema,
            symbols=["MNQ.c.0"],
            start=(available_end - lookback).isoformat(),
            end=available_end.isoformat(),
            stype_in="continuous",
        ))
        if bars:
            latest_ts = datetime.fromtimestamp(bars[-1].ts_event / 1e9, tz=timezone.utc)
            lag_min = (now - latest_ts).total_seconds() / 60
            print(f"Historical {label:3s} | latest bar: {latest_ts.isoformat()} | lag: {lag_min:.0f} min behind now")
        else:
            print(f"Historical {label:3s} | no bars returned in lookback window")
    except Exception as e:
        print(f"Historical {label:3s} | error: {e}")

# ── Live feed replay depth ────────────────────────────────────────────────────
print("\nConnecting to live feed with start=48h ago ...")
live = db.Live(key=api_key)
live.subscribe(
    dataset="GLBX.MDP3",
    schema="ohlcv-1m",
    symbols=["MNQ.c.0"],
    stype_in="continuous",
    start=(now - timedelta(hours=48)).isoformat(),
)

count = 0
first_bar_ts = None
for record in live:
    rtype = int(getattr(record, "rtype", -1))
    if rtype == 33:  # ohlcv-1m
        ts = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
        if first_bar_ts is None:
            first_bar_ts = ts
            print(f"Live feed first bar: {ts.isoformat()} | reaches {(now - ts).total_seconds() / 3600:.1f}h back")
        count += 1
        if count >= 5:
            break

live.stop()
