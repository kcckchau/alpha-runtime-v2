import databento as db
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import os

load_dotenv()

api_key = os.environ["DATABENTO__API_KEY"]
now = datetime.now(timezone.utc)

# ── 1. Historical API: where does it cut off? ─────────────────────────────────
hist = db.Historical(api_key)

info = hist.metadata.get_dataset_range(dataset="GLBX.MDP3")
available_end = datetime.fromisoformat(info["end"].replace("Z", "+00:00"))
print(f"Dataset range:  start={info['start']}  end={info['end']}")
print(f"Historical lag: {(now - available_end).total_seconds() / 60:.1f} minutes behind now")

bars = list(hist.timeseries.get_range(
    dataset="GLBX.MDP3",
    schema="ohlcv-1m",
    symbols=["MNQ.c.0"],
    start=(available_end - timedelta(hours=1)).isoformat(),
    end=available_end.isoformat(),
    stype_in="continuous",
))
if bars:
    latest_ts = datetime.fromtimestamp(bars[-1].ts_event / 1e9, tz=timezone.utc)
    print(f"Latest 1m bar:  {latest_ts.isoformat()}")
else:
    print("No bars returned")

# ── 2. Live feed: what is the earliest start it will replay? ──────────────────
print("\nConnecting to live feed with start=24h ago ...")
live = db.Live(key=api_key)
live.subscribe(
    dataset="GLBX.MDP3",
    schema="ohlcv-1m",
    symbols=["MNQ.c.0"],
    stype_in="continuous",
    start=(now - timedelta(hours=24)).isoformat(),  # ask for 24h back
)

first_bar_ts = None
count = 0
for record in live:
    rtype = int(getattr(record, "rtype", -1))
    if rtype == 33:  # ohlcv-1m
        ts = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
        if first_bar_ts is None:
            first_bar_ts = ts
            print(f"First bar from live feed: {ts.isoformat()}")
            print(f"Live feed replay reaches: {(now - ts).total_seconds() / 3600:.1f} hours back")
        count += 1
        if count >= 5:
            break

live.stop()
