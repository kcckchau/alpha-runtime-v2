"""
Databento raw data inspector.

Usage:
    python scripts/databento_inspect.py                    # historical bars (default)
    python scripts/databento_inspect.py --mode plan        # check plan / available schemas
    python scripts/databento_inspect.py --mode bars        # last N completed 1m bars
    python scripts/databento_inspect.py --mode bars --hours 6 --bars 20
    python scripts/databento_inspect.py --mode trades      # last N trade ticks
    python scripts/databento_inspect.py --mode quotes      # last N mbp-1 quotes
    python scripts/databento_inspect.py --mode live        # live stream (Ctrl-C to stop)
    python scripts/databento_inspect.py --mode live --schemas ohlcv-1m trades mbp-1
    python scripts/databento_inspect.py --mode vwap        # VWAP session boundary check

Reads DATABENTO_API_KEY from environment (or .env file via python-dotenv if installed).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

# ── API key ───────────────────────────────────────────────────────────────────

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# App uses Pydantic double-underscore convention (DATABENTO__API_KEY);
# Databento SDK uses single underscore (DATABENTO_API_KEY). Accept both.
API_KEY = os.environ.get("DATABENTO__API_KEY") or os.environ.get("DATABENTO_API_KEY", "")
if not API_KEY:
    sys.exit("ERROR: set DATABENTO__API_KEY or DATABENTO_API_KEY in environment / .env")

DATASET   = "GLBX.MDP3"
SYMBOL    = "MNQ.c.0"
STYPE_IN  = "continuous"
PRICE_DIV = 1_000_000_000

_UTC = timezone.utc


def fmt_ts(ts_ns: int) -> str:
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=_UTC)
    # also show ET
    try:
        from zoneinfo import ZoneInfo
        et = dt.astimezone(ZoneInfo("America/New_York"))
        return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}Z  ({et.strftime('%H:%M:%S ET')})"
    except Exception:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_px(raw: int | None) -> str:
    if raw is None:
        return "—"
    return f"{raw / PRICE_DIV:.2f}"


# ── Modes ─────────────────────────────────────────────────────────────────────

def mode_plan(client) -> None:
    print("=" * 60)
    print("PLAN / DATASET INFO")
    print("=" * 60)

    try:
        r = client.metadata.get_dataset_range(dataset=DATASET)
        print(f"Dataset range: {r.get('start')} → {r.get('end')}")
    except Exception as e:
        print(f"  dataset range: ERROR {e}")

    hist_schemas: list[str] = []
    try:
        hist_schemas = sorted(client.metadata.list_schemas(dataset=DATASET))
    except Exception as e:
        print(f"  historical schemas: ERROR {e}")

    # ── Live gateway entitlement check ────────────────────────────────────────
    # Historical list_schemas only covers the REST API. Live is a separate
    # WebSocket gateway with its own entitlements. We probe it by subscribing
    # to each candidate schema and checking whether the gateway returns an
    # error record (rtype=21) or a valid symbol-mapping record (rtype=22).
    import databento as db
    import threading

    LIVE_SCHEMAS = ["ohlcv-1s", "ohlcv-1m", "trades", "mbp-1", "tbbo", "bbo-1s", "mbp-10", "mbo"]
    live_results: dict[str, str] = {}

    for schema in LIVE_SCHEMAS:
        live_results[schema] = "timeout"
        outcome = threading.Event()

        def _probe(schema=schema):
            try:
                lc = db.Live(key=API_KEY)
                lc.subscribe(dataset=DATASET, schema=schema, symbols=[SYMBOL], stype_in=STYPE_IN)
                for rec in lc:
                    rtype = getattr(rec, "rtype", -1)
                    if int(rtype) == 21:
                        live_results[schema] = f"DENIED — {getattr(rec, 'err', str(rec))[:60]}"
                    else:
                        live_results[schema] = "OK"
                    lc.stop()
                    break
            except Exception as e:
                live_results[schema] = f"ERROR — {e}"
            finally:
                outcome.set()

        t = threading.Thread(target=_probe, daemon=True)
        t.start()
        outcome.wait(timeout=8)

    # ── Combined table ────────────────────────────────────────────────────────
    all_schemas = sorted(set(hist_schemas) | set(LIVE_SCHEMAS))
    col = 16
    print(f"\n  {'schema':<{col}}  {'historical':>12}  {'live':>12}")
    print(f"  {'-'*col}  {'----------':>12}  {'----------':>12}")
    for s in all_schemas:
        hist = "OK" if s in hist_schemas else "—"
        live = live_results.get(s, "—")
        live_short = "OK" if live == "OK" else ("DENIED" if "DENIED" in live else live[:12])
        print(f"  {s:<{col}}  {hist:>12}  {live_short:>12}")


def _safe_end(client, end: datetime, schema: str = "ohlcv-1m") -> datetime:
    """
    Clamp end to Databento's available range to avoid 422 errors.
    Historical data lags wall clock by a few minutes; we query the schema-specific
    available end and fall back to a 10-minute buffer if the API call fails.
    """
    try:
        info = client.metadata.get_dataset_range(dataset=DATASET, schema=schema)
        available = info.get("end", "")
        if available:
            avail_dt = datetime.fromisoformat(available.replace("Z", "+00:00"))
            if end > avail_dt:
                return avail_dt
            return end
    except Exception:
        pass
    # Fallback: subtract 10-minute lag buffer
    return end - timedelta(minutes=10)


def mode_bars(client, sym: str, hours: int, n: int) -> None:
    end = _safe_end(client, datetime.now(_UTC), "ohlcv-1m")
    start = end - timedelta(hours=hours)

    print("=" * 60)
    print(f"OHLCV-1M  {sym}  last {hours}h  (showing up to {n} bars)")
    print("=" * 60)
    print(f"{'ts_event (bar timestamp)':42s}  {'open':>10}  {'high':>10}  {'low':>10}  {'close':>10}  {'volume':>8}  {'vwap':>10}  {'trades':>6}  rtype")
    print("-" * 120)

    try:
        store = client.timeseries.get_range(
            dataset=DATASET,
            schema="ohlcv-1m",
            symbols=[sym],
            stype_in=STYPE_IN,
            start=start.isoformat(),
            end=end.isoformat(),
        )
    except Exception as e:
        print(f"ERROR: {e}")
        return

    rows = list(store)
    # Show last n bars
    rows = rows[-n:]
    for rec in rows:
        vwap = getattr(rec, "vwap", None)
        trade_count = getattr(rec, "trade_count", None)
        print(
            f"{fmt_ts(rec.ts_event):42s}  "
            f"{fmt_px(rec.open):>10}  "
            f"{fmt_px(rec.high):>10}  "
            f"{fmt_px(rec.low):>10}  "
            f"{fmt_px(rec.close):>10}  "
            f"{rec.volume:>8}  "
            f"{fmt_px(vwap) if vwap else '—':>10}  "
            f"{trade_count if trade_count is not None else '—':>6}  "
            f"rtype={rec.rtype}"
        )
    print(f"\n  Shown: {len(rows)} bars")


def mode_vwap(client, sym: str) -> None:
    """
    Fetch bars around the CME maintenance window to check timestamp conventions.
    Shows the last bar before 17:00 ET and first bar after 18:00 ET — critical
    for verifying whether the gap is > 3600s (resets chart VWAP) or = 3600s (doesn't).
    """
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")

    # Use yesterday's session boundary (17:00–18:00 ET)
    now_et = datetime.now(_ET)
    target_date = now_et.date()
    # maintenance window: 17:00-18:00 ET
    window_start = datetime(target_date.year, target_date.month, target_date.day, 16, 45, tzinfo=_ET)
    window_end   = datetime(target_date.year, target_date.month, target_date.day, 18, 15, tzinfo=_ET)

    print("=" * 60)
    print("VWAP SESSION BOUNDARY CHECK")
    print(f"Fetching bars around maintenance window: {window_start.strftime('%H:%M ET')} → {window_end.strftime('%H:%M ET')}")
    print("=" * 60)
    print(f"{'ts_event':42s}  {'close':>10}  {'volume':>8}  {'gap_from_prev':>15}")
    print("-" * 90)

    try:
        store = client.timeseries.get_range(
            dataset=DATASET,
            schema="ohlcv-1m",
            symbols=[sym],
            stype_in=STYPE_IN,
            start=window_start.astimezone(_UTC).isoformat(),
            end=window_end.astimezone(_UTC).isoformat(),
        )
    except Exception as e:
        print(f"ERROR: {e}")
        return

    prev_ts_ns = None
    for rec in store:
        gap = ""
        if prev_ts_ns is not None:
            gap_s = (rec.ts_event - prev_ts_ns) / 1e9
            flag = " ← RESETS VWAP" if gap_s > 3600 else (" ← EXACT 3600 (bug!)" if gap_s == 3600 else "")
            gap = f"{gap_s:>8.0f}s{flag}"
        print(f"{fmt_ts(rec.ts_event):42s}  {fmt_px(rec.close):>10}  {rec.volume:>8}  {gap}")
        prev_ts_ns = rec.ts_event

    if prev_ts_ns is None:
        print("  No bars returned — market may be closed or outside available range")


def mode_trades(client, sym: str, hours: int, n: int) -> None:
    end = _safe_end(client, datetime.now(_UTC), "trades")
    start = end - timedelta(hours=hours)

    print("=" * 60)
    print(f"TRADES  {sym}  last {hours}h  (showing last {n})")
    print("=" * 60)
    print(f"{'ts_event':42s}  {'price':>10}  {'size':>6}  {'side':>6}  {'match_type':>12}  sequence")
    print("-" * 100)

    try:
        store = client.timeseries.get_range(
            dataset=DATASET,
            schema="trades",
            symbols=[sym],
            stype_in=STYPE_IN,
            start=start.isoformat(),
            end=end.isoformat(),
        )
    except Exception as e:
        print(f"ERROR: {e}")
        return

    rows = list(store)
    rows = rows[-n:]
    for rec in rows:
        side = getattr(rec, "side", "?")
        match_type = getattr(rec, "match_type", "?")
        seq = getattr(rec, "sequence", "?")
        print(
            f"{fmt_ts(rec.ts_event):42s}  "
            f"{fmt_px(rec.price):>10}  "
            f"{rec.size:>6}  "
            f"{str(side):>6}  "
            f"{str(match_type):>12}  "
            f"{seq}"
        )


def mode_quotes(client, sym: str, hours: int, n: int) -> None:
    end = _safe_end(client, datetime.now(_UTC), "mbp-1")
    start = end - timedelta(hours=hours)

    print("=" * 60)
    print(f"MBP-1 QUOTES  {sym}  last {hours}h  (showing last {n})")
    print("=" * 60)
    print(f"{'ts_event':42s}  {'bid_px':>10}  {'bid_sz':>7}  {'ask_px':>10}  {'ask_sz':>7}  rtype")
    print("-" * 100)

    try:
        store = client.timeseries.get_range(
            dataset=DATASET,
            schema="mbp-1",
            symbols=[sym],
            stype_in=STYPE_IN,
            start=start.isoformat(),
            end=end.isoformat(),
        )
    except Exception as e:
        print(f"ERROR: {e}")
        return

    rows = list(store)
    rows = rows[-n:]
    for rec in rows:
        lvl = rec.levels[0]
        print(
            f"{fmt_ts(rec.ts_event):42s}  "
            f"{fmt_px(lvl.bid_px):>10}  "
            f"{lvl.bid_sz:>7}  "
            f"{fmt_px(lvl.ask_px):>10}  "
            f"{lvl.ask_sz:>7}  "
            f"rtype={rec.rtype}"
        )


def mode_live(sym: str, schemas: list[str]) -> None:
    import databento as db

    print("=" * 60)
    print(f"LIVE STREAM  {sym}  schemas={schemas}")
    print("Ctrl-C to stop")
    print("=" * 60)

    client = db.Live(key=API_KEY)
    for schema in schemas:
        client.subscribe(
            dataset=DATASET,
            schema=schema,
            symbols=[sym],
            stype_in=STYPE_IN,
        )
        print(f"  subscribed: {schema}")
    print()

    try:
        for rec in client:
            rtype = getattr(rec, "rtype", "?")
            ts_ns = getattr(rec, "ts_event", None)
            ts_str = fmt_ts(ts_ns) if ts_ns else "?"

            # Pretty-print by rtype
            if rtype in (32, 33, 34, 35):   # ohlcv
                tf = {32: "S1", 33: "M1", 34: "H1", 35: "D1"}.get(rtype, "?")
                vwap = getattr(rec, "vwap", None)
                print(
                    f"[BAR-{tf}]  {ts_str}  "
                    f"O={fmt_px(rec.open)} H={fmt_px(rec.high)} L={fmt_px(rec.low)} C={fmt_px(rec.close)}  "
                    f"V={rec.volume}  vwap={fmt_px(vwap) if vwap else '—'}"
                )
            elif rtype == 0:   # trade
                side = getattr(rec, "side", "?")
                mt   = getattr(rec, "match_type", "?")
                print(f"[TRADE]    {ts_str}  px={fmt_px(rec.price)}  sz={rec.size}  side={side}  match={mt}")
            elif rtype == 1:   # mbp-1
                lvl = rec.levels[0]
                print(f"[QUOTE]    {ts_str}  bid={fmt_px(lvl.bid_px)}x{lvl.bid_sz}  ask={fmt_px(lvl.ask_px)}x{lvl.ask_sz}")
            elif rtype == 10:  # mbp-10
                lvl = rec.levels[0]
                print(f"[BOOK-10]  {ts_str}  best_bid={fmt_px(lvl.bid_px)}x{lvl.bid_sz}  best_ask={fmt_px(lvl.ask_px)}x{lvl.ask_sz}  (10 levels)")
            elif rtype == 22:  # symbol mapping
                iid  = getattr(rec, "instrument_id", "?")
                sym  = getattr(rec, "stype_in_symbol", "?")
                print(f"[MAPPING]  instrument_id={iid}  symbol={sym}")
            elif rtype == 23:  # system
                print(f"[SYSTEM]   {getattr(rec, 'msg', repr(rec))}")
            elif rtype == 21:  # error
                print(f"[ERROR]    {getattr(rec, 'err', repr(rec))}")
            else:
                print(f"[UNKNOWN rtype={rtype}]  {repr(rec)[:120]}")
    except KeyboardInterrupt:
        print("\nStopped.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Databento raw data inspector")
    parser.add_argument("--mode", default="bars",
                        choices=["plan", "bars", "trades", "quotes", "live", "vwap"],
                        help="What to inspect")
    parser.add_argument("--symbol", default=SYMBOL, help="Databento symbol (default: MNQ.c.0)")
    parser.add_argument("--hours", type=int, default=2, help="Lookback window in hours (historical modes)")
    parser.add_argument("--bars", type=int, default=15, help="Max rows to display")
    parser.add_argument("--schemas", nargs="+", default=["ohlcv-1m", "trades"],
                        help="Schemas for live mode (default: ohlcv-1m trades)")
    args = parser.parse_args()

    sym = args.symbol

    import databento as db
    client = db.Historical(key=API_KEY)

    if args.mode == "plan":
        mode_plan(client)
    elif args.mode == "bars":
        mode_bars(client, sym, args.hours, args.bars)
    elif args.mode == "vwap":
        mode_vwap(client, sym)
    elif args.mode == "trades":
        mode_trades(client, sym, args.hours, args.bars)
    elif args.mode == "quotes":
        mode_quotes(client, sym, args.hours, args.bars)
    elif args.mode == "live":
        mode_live(sym, args.schemas)


if __name__ == "__main__":
    main()
