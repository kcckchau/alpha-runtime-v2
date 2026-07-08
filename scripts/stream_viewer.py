"""
Stream Viewer — watch raw Databento and IBKR output side by side.

Shows every record as it arrives: bars, quotes, trades, raw fields.
Useful for understanding what each source actually sends and how they differ.

Usage:
    # Databento (default)
    uv run python scripts/stream_viewer.py

    # IBKR
    uv run python scripts/stream_viewer.py --source ibkr

    # Both simultaneously
    uv run python scripts/stream_viewer.py --source both

    # Filter to one type
    uv run python scripts/stream_viewer.py --only trades
    uv run python scripts/stream_viewer.py --only quotes
    uv run python scripts/stream_viewer.py --only bars
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

# Make sure project src is on the path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alpha.config.loader import get_settings

_UTC = timezone.utc
_PRICE_SCALE = Decimal("1000000000")


# ── Formatting ────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
BLUE   = "\033[34m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"


def _ts() -> str:
    return datetime.now(_UTC).strftime("%H:%M:%S.%f")[:-3]


def _label(source: str, kind: str) -> str:
    color = CYAN if source == "DATABENTO" else YELLOW
    kind_color = {
        "BAR":   GREEN,
        "QUOTE": BLUE,
        "TRADE": RED,
        "RAW":   DIM,
        "SYS":   DIM,
    }.get(kind, WHITE)
    return f"{DIM}{_ts()}{RESET}  {color}{BOLD}{source:<12}{RESET} {kind_color}{kind:<6}{RESET}"


def _print_bar(source: str, symbol: str, tf: str,
               o, h, l, c, vol, ts, extra: str = "") -> None:
    print(
        f"{_label(source, 'BAR')}  "
        f"{BOLD}{symbol}{RESET} [{tf}]  "
        f"O={o}  H={h}  L={l}  C={GREEN}{c}{RESET}  V={vol}  "
        f"ts={ts}{('  ' + DIM + extra + RESET) if extra else ''}"
    )


def _print_quote(source: str, symbol: str,
                 bid, bid_sz, ask, ask_sz, ts, extra: str = "") -> None:
    spread = float(ask - bid) if bid and ask else 0
    print(
        f"{_label(source, 'QUOTE')}  "
        f"{BOLD}{symbol}{RESET}  "
        f"bid={GREEN}{bid}{RESET}×{bid_sz}  "
        f"ask={RED}{ask}{RESET}×{ask_sz}  "
        f"spread={YELLOW}{spread:.2f}{RESET}  "
        f"ts={ts}{('  ' + DIM + extra + RESET) if extra else ''}"
    )


def _print_trade(source: str, symbol: str,
                 price, size, side: str, ts, extra: str = "") -> None:
    side_color = GREEN if side == "BUY" else RED if side == "SELL" else DIM
    print(
        f"{_label(source, 'TRADE')}  "
        f"{BOLD}{symbol}{RESET}  "
        f"px={price}  sz={size}  "
        f"side={side_color}{side}{RESET}  "
        f"ts={ts}{('  ' + DIM + extra + RESET) if extra else ''}"
    )


def _print_raw(source: str, kind: str, msg: str) -> None:
    print(f"{_label(source, 'SYS')}  {DIM}{kind}: {msg}{RESET}")


# ── Databento viewer ──────────────────────────────────────────────────────────

async def run_databento(settings, symbol: str, only: str | None) -> None:
    import threading
    import databento as db
    import databento_dbn as dbn

    _PRICE_SCALE_I = 1_000_000_000

    def to_dec(raw: int) -> Decimal:
        return Decimal(raw) / _PRICE_SCALE

    def to_dt(ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1e9, tz=_UTC).strftime("%H:%M:%S.%f")[:-3]

    # Resolve symbol → Databento continuous format (MNQ-09 → MNQ.c.0)
    id_map: dict[int, str] = {}
    try:
        from alpha.instruments import resolve_symbol
        sym_def = resolve_symbol(symbol)
        root = sym_def.root_symbol or sym_def.ticker
        db_sym = f"{root}{settings.databento.continuous_suffix}"
    except Exception:
        root = symbol.split("-")[0]
        db_sym = f"{root}{settings.databento.continuous_suffix}"

    print(f"{DIM}  Resolving {symbol} → {CYAN}{db_sym}{RESET}\n")

    client = db.Live(key=settings.databento.api_key.get_secret_value())

    # Subscribe based on filter
    dataset = settings.databento.dataset
    stype   = settings.databento.stype_in

    if only in (None, "bars"):
        client.subscribe(dataset=dataset, schema="ohlcv-1s", symbols=[db_sym], stype_in=stype)
        client.subscribe(dataset=dataset, schema="ohlcv-1m", symbols=[db_sym], stype_in=stype)
    if only in (None, "quotes"):
        client.subscribe(dataset=dataset, schema="mbp-1",    symbols=[db_sym], stype_in=stype)
    if only in (None, "trades"):
        client.subscribe(dataset=dataset, schema="trades",   symbols=[db_sym], stype_in=stype)

    _RTYPE_OHLCV = {32: "1s", 33: "1m", 34: "1h", 35: "1d"}
    _RTYPE_MBP1  = 1
    _RTYPE_TRADE = 0
    _RTYPE_SYMMAP = 22
    _RTYPE_SYSTEM = 23
    _RTYPE_ERROR  = 21

    loop = asyncio.get_running_loop()

    def record_loop():
        for record in client:
            rtype = int(getattr(record, "rtype", -1))

            if rtype == _RTYPE_SYMMAP:
                iid = getattr(record, "instrument_id", None)
                sym_in = getattr(record, "stype_in_symbol", "")
                if iid is not None:
                    id_map[iid] = symbol
                _print_raw("DATABENTO", "SYMBOL_MAP",
                            f"instrument_id={iid} → {sym_in} (={symbol})")
                continue

            if rtype == _RTYPE_SYSTEM:
                _print_raw("DATABENTO", "SYSTEM", getattr(record, "msg", ""))
                continue

            if rtype == _RTYPE_ERROR:
                _print_raw("DATABENTO", "ERROR", str(getattr(record, "err", record)))
                continue

            iid = getattr(record, "instrument_id", None)
            ticker = id_map.get(iid, symbol) if iid else symbol

            if rtype in _RTYPE_OHLCV:
                tf = _RTYPE_OHLCV[rtype]
                _print_bar(
                    "DATABENTO", ticker, tf,
                    to_dec(record.open), to_dec(record.high),
                    to_dec(record.low),  to_dec(record.close),
                    record.volume, to_dt(record.ts_event),
                    f"rtype={rtype}",
                )

            elif rtype == _RTYPE_MBP1:
                levels = getattr(record, "levels", [])
                if levels:
                    lvl = levels[0]
                    raw_px = getattr(record, "price", dbn.UNDEF_PRICE)
                    extra = f"last_px={to_dec(raw_px)}" if raw_px != dbn.UNDEF_PRICE else "quote-only"
                    _print_quote(
                        "DATABENTO", ticker,
                        to_dec(lvl.bid_px), lvl.bid_sz,
                        to_dec(lvl.ask_px), lvl.ask_sz,
                        to_dt(record.ts_event), extra,
                    )

            elif rtype == _RTYPE_TRADE:
                match_type = str(getattr(record, "match_type", "?"))
                side_raw   = str(getattr(record, "side", ""))
                side = "BUY" if "A" in side_raw else "SELL" if "B" in side_raw else "?"
                _print_trade(
                    "DATABENTO", ticker,
                    to_dec(record.price), record.size, side,
                    to_dt(record.ts_event),
                    f"match_type={match_type} seq={getattr(record, 'sequence', '?')}",
                )

    print(f"\n{CYAN}{BOLD}━━━ DATABENTO stream: {symbol} (dataset={dataset}) ━━━{RESET}\n")
    thread = threading.Thread(target=record_loop, daemon=True)
    thread.start()

    # Keep async loop alive while thread runs
    while thread.is_alive():
        await asyncio.sleep(0.1)


# ── IBKR viewer ───────────────────────────────────────────────────────────────

async def run_ibkr(settings, symbol: str, only: str | None) -> None:
    from ib_insync import IB, util
    from alpha.instruments import resolve_symbol
    from alpha.engines.ibkr.contracts import make_contract, normalize_bar, normalize_quote, TIMEFRAME_TO_IBKR
    from alpha.models.enums import BarTimeframe

    util.patchAsyncio()

    ib = IB()
    cfg = settings.ibkr
    print(f"\n{YELLOW}{BOLD}━━━ IBKR stream: {symbol} (host={cfg.host}:{cfg.port}) ━━━{RESET}\n")

    await ib.connectAsync(cfg.host, cfg.port, clientId=cfg.client_id + 10)

    sym_def  = resolve_symbol(symbol)
    contract = make_contract(sym_def)
    await ib.qualifyContractsAsync(contract)

    # ── Bars ──────────────────────────────────────────────────────────────────
    if only in (None, "bars"):
        bar_size = TIMEFRAME_TO_IBKR[BarTimeframe.M1]
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting=bar_size,
            whatToShow=cfg.what_to_show,
            useRTH=cfg.use_rth,
            formatDate=2,
            keepUpToDate=True,
        )

        def on_bar_update(bar_list, has_new_bar: bool):
            seq = list(bar_list)
            if not seq:
                return
            # In-progress bar (always last)
            b = seq[-1]
            _print_bar(
                "IBKR", symbol, "1m (partial)",
                b.open, b.high, b.low, b.close, b.volume,
                str(b.date), "in-progress",
            )
            # Completed bar
            if has_new_bar and len(seq) >= 2:
                b2 = seq[-2]
                _print_bar(
                    "IBKR", symbol, "1m (DONE)",
                    b2.open, b2.high, b2.low, b2.close, b2.volume,
                    str(b2.date), "completed",
                )

        bars.updateEvent += on_bar_update

    # ── Quotes ────────────────────────────────────────────────────────────────
    if only in (None, "quotes"):
        ticker = ib.reqMktData(contract, "", False, False)

        def on_quote_update(t):
            bid = getattr(t, "bid", None)
            ask = getattr(t, "ask", None)
            bid_sz = getattr(t, "bidSize", None)
            ask_sz = getattr(t, "askSize", None)
            last   = getattr(t, "last", None)
            if bid and ask and bid > 0 and ask > 0:
                _print_quote(
                    "IBKR", symbol,
                    Decimal(str(bid)), bid_sz,
                    Decimal(str(ask)), ask_sz,
                    _ts(),
                    f"last={last}",
                )

        ticker.updateEvent += on_quote_update

    # ── Tick trades ───────────────────────────────────────────────────────────
    if only in (None, "trades"):
        tick_ticker = ib.reqTickByTickData(contract, "Last", 0, True)
        last_seen = [0]

        def on_tick_update(t):
            ticks = getattr(t, "tickByTicks", [])
            new_count = len(ticks)
            if new_count <= last_seen[0]:
                return
            for tick in ticks[last_seen[0]:]:
                price = getattr(tick, "price", None)
                size  = getattr(tick, "size", None)
                if price and size and price > 0:
                    _print_trade(
                        "IBKR", symbol,
                        Decimal(str(price)), size, "?",
                        _ts(), "tick-by-tick",
                    )
            last_seen[0] = new_count

        tick_ticker.updateEvent += on_tick_update

    # Run until interrupted
    await asyncio.sleep(86400)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="Watch raw Databento / IBKR stream")
    parser.add_argument("--source", choices=["databento", "ibkr", "both"],
                        default="databento", help="Which source to stream")
    parser.add_argument("--symbol", default=None,
                        help="Symbol to watch (default: first in RUNTIME__SYMBOLS)")
    parser.add_argument("--only", choices=["bars", "quotes", "trades"], default=None,
                        help="Filter to a single record type")
    args = parser.parse_args()

    settings = get_settings()
    symbol   = args.symbol or settings.runtime.symbols[0]

    print(f"\n{BOLD}Stream Viewer{RESET}  symbol={CYAN}{symbol}{RESET}  "
          f"source={YELLOW}{args.source}{RESET}  "
          f"filter={args.only or 'all'}")
    print(f"{DIM}Press Ctrl+C to stop{RESET}\n")

    tasks = []
    if args.source in ("databento", "both"):
        tasks.append(asyncio.create_task(run_databento(settings, symbol, args.only)))
    if args.source in ("ibkr", "both"):
        tasks.append(asyncio.create_task(run_ibkr(settings, symbol, args.only)))

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print(f"\n{DIM}Stopped.{RESET}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{DIM}Stopped.{RESET}")
