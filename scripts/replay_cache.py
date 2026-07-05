"""
replay_cache.py — Local disk cache for Databento replay downloads and structured result saving.

Two classes:
  ReplayCache       — caches downloaded bars/trades/quotes so repeated replay runs skip Databento API
  ReplayResultSaver — collects per-bar engine state during replay and writes JSON + CSV at the end

Cache layout:
  data/replay_cache/{symbol}/{YYYY-MM-DD}/
    bars_w{warmup_days}.parquet    # all bars (warmup + session) for a given warmup depth
    trades.parquet                 # session-window trades (debounced quotes stored here too)
    quotes.parquet                 # session-window quotes (price-change only — already debounced)
    meta.json                      # record counts, timestamps, schema version

Results layout:
  data/replay_results/{symbol}/{YYYY-MM-DD}/
    bars_YYYYMMDD_HHMMSS.json      # bars-only run
    bars_YYYYMMDD_HHMMSS.csv
    full_YYYYMMDD_HHMMSS.json      # full-signals run
    full_YYYYMMDD_HHMMSS.csv
    index.json                     # list of all saved runs for this session
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from alpha.models.enums import BarTimeframe, DataSourceId, TakerSide
from alpha.models.events import BarEvent, EventMetadata, QuoteEvent, TradeEvent

if TYPE_CHECKING:
    from alpha.models.market_state import MarketState
    from alpha.models.snapshot import BarSnapshot
    from alpha.models.thesis import ActiveThesis

_UTC = timezone.utc
_CACHE_SCHEMA_VERSION = "1"


# ── Serialization helpers (mirrors StorageEngine._serialize_event) ────────────

def _dec(v: Decimal | None) -> str | None:
    return str(v) if v is not None else None


def _serialize_bar(e: BarEvent) -> dict:
    return {
        "event_id": str(e.metadata.event_id),
        "symbol": e.symbol,
        "timestamp": e.timestamp.isoformat(),
        "source": str(e.metadata.source),
        "received_at": e.metadata.received_at.isoformat(),
        "is_replay": e.metadata.is_replay,
        "timeframe": str(e.timeframe),
        "open": _dec(e.open),
        "high": _dec(e.high),
        "low": _dec(e.low),
        "close": _dec(e.close),
        "volume": e.volume,
        "vwap": _dec(e.vwap),
        "trade_count": e.trade_count,
        "is_partial": e.is_partial,
    }


def _deserialize_bar(row: dict) -> BarEvent:
    ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
    recv = datetime.fromisoformat(str(row["received_at"]).replace("Z", "+00:00"))
    raw_tf = str(row.get("timeframe", "BarTimeframe.M1"))
    # Stored as "BarTimeframe.M1", "BarTimeframe.M5", etc. — extract the value part.
    tf_val = raw_tf.split(".")[-1]  # "M1", "M5", ...
    try:
        timeframe = BarTimeframe[tf_val]
    except KeyError:
        timeframe = BarTimeframe.M1
    return BarEvent(
        symbol=str(row["symbol"]),
        timestamp=ts,
        timeframe=timeframe,
        open=Decimal(str(row["open"])),
        high=Decimal(str(row["high"])),
        low=Decimal(str(row["low"])),
        close=Decimal(str(row["close"])),
        volume=int(row["volume"]),
        vwap=Decimal(str(row["vwap"])) if row.get("vwap") is not None else None,
        trade_count=int(row["trade_count"]) if row.get("trade_count") is not None else None,
        is_partial=bool(row.get("is_partial", False)),
        metadata=EventMetadata(
            event_id=row.get("event_id", str(uuid4())),
            source=row.get("source", str(DataSourceId.DATABENTO)),
            received_at=recv,
            is_replay=bool(row.get("is_replay", True)),
        ),
    )


def _serialize_trade(e: TradeEvent) -> dict:
    return {
        "event_id": str(e.metadata.event_id),
        "symbol": e.symbol,
        "timestamp": e.timestamp.isoformat(),
        "source": str(e.metadata.source),
        "received_at": e.metadata.received_at.isoformat(),
        "price": _dec(e.price),
        "size": e.size,
        "taker_side": str(e.taker_side),
        "trade_id": e.trade_id,
    }


def _deserialize_trade(row: dict) -> TradeEvent:
    ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
    recv = datetime.fromisoformat(str(row["received_at"]).replace("Z", "+00:00"))
    raw_side = str(row.get("taker_side", ""))
    if "BUY" in raw_side:
        side = TakerSide.BUY
    elif "SELL" in raw_side:
        side = TakerSide.SELL
    else:
        side = TakerSide.UNKNOWN
    return TradeEvent(
        symbol=str(row["symbol"]),
        timestamp=ts,
        price=Decimal(str(row["price"])),
        size=int(row["size"]),
        taker_side=side,
        trade_id=str(row.get("trade_id") or ""),
        metadata=EventMetadata(
            event_id=row.get("event_id", str(uuid4())),
            source=row.get("source", str(DataSourceId.DATABENTO)),
            received_at=recv,
            is_replay=True,
        ),
    )


def _serialize_quote(e: QuoteEvent) -> dict:
    return {
        "event_id": str(e.metadata.event_id),
        "symbol": e.symbol,
        "timestamp": e.timestamp.isoformat(),
        "source": str(e.metadata.source),
        "received_at": e.metadata.received_at.isoformat(),
        "bid_price": _dec(e.bid_price),
        "bid_size": e.bid_size,
        "ask_price": _dec(e.ask_price),
        "ask_size": e.ask_size,
    }


def _deserialize_quote(row: dict) -> QuoteEvent:
    ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
    recv = datetime.fromisoformat(str(row["received_at"]).replace("Z", "+00:00"))
    return QuoteEvent(
        symbol=str(row["symbol"]),
        timestamp=ts,
        bid_price=Decimal(str(row["bid_price"])),
        bid_size=int(row["bid_size"]),
        ask_price=Decimal(str(row["ask_price"])),
        ask_size=int(row["ask_size"]),
        metadata=EventMetadata(
            event_id=row.get("event_id", str(uuid4())),
            source=row.get("source", str(DataSourceId.DATABENTO)),
            received_at=recv,
            is_replay=True,
        ),
    )


def _write_parquet(rows: list[dict], path: Path) -> None:
    """Atomic write: temp file → rename."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    try:
        pq.write_table(table, tmp)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _read_parquet(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


# ── ReplayCache ───────────────────────────────────────────────────────────────

class ReplayCache:
    """Read/write disk cache for Databento replay downloads."""

    def __init__(self, repo_root: Path) -> None:
        self._root = repo_root / "data" / "replay_cache"

    def _dir(self, symbol: str, d: date) -> Path:
        return self._root / symbol / str(d)

    def _bars_path(self, symbol: str, d: date, warmup_days: int) -> Path:
        return self._dir(symbol, d) / f"bars_w{warmup_days}.parquet"

    def _trades_path(self, symbol: str, d: date) -> Path:
        return self._dir(symbol, d) / "trades.parquet"

    def _quotes_path(self, symbol: str, d: date) -> Path:
        return self._dir(symbol, d) / "quotes.parquet"

    def _meta_path(self, symbol: str, d: date) -> Path:
        return self._dir(symbol, d) / "meta.json"

    def _m5_bars_path(self, symbol: str, d: date, warmup_days: int) -> Path:
        return self._dir(symbol, d) / f"m5_bars_w{warmup_days}.parquet"

    def _h1_bars_path(self, symbol: str, d: date, warmup_days: int) -> Path:
        return self._dir(symbol, d) / f"h1_bars_w{warmup_days}.parquet"

    # ── bars (M1) ─────────────────────────────────────────────────────────────

    def bars_cached(self, symbol: str, d: date, warmup_days: int) -> bool:
        return self._bars_path(symbol, d, warmup_days).exists()

    def load_bars(self, symbol: str, d: date, warmup_days: int) -> list[BarEvent]:
        rows = _read_parquet(self._bars_path(symbol, d, warmup_days))
        return [_deserialize_bar(r) for r in rows]

    def save_bars(self, bars: list[BarEvent], symbol: str, d: date, warmup_days: int) -> None:
        _write_parquet([_serialize_bar(b) for b in bars], self._bars_path(symbol, d, warmup_days))

    # ── bars (M5) ─────────────────────────────────────────────────────────────

    def m5_bars_cached(self, symbol: str, d: date, warmup_days: int) -> bool:
        return self._m5_bars_path(symbol, d, warmup_days).exists()

    def load_m5_bars(self, symbol: str, d: date, warmup_days: int) -> list[BarEvent]:
        rows = _read_parquet(self._m5_bars_path(symbol, d, warmup_days))
        return [_deserialize_bar(r) for r in rows]

    def save_m5_bars(self, bars: list[BarEvent], symbol: str, d: date, warmup_days: int) -> None:
        _write_parquet([_serialize_bar(b) for b in bars], self._m5_bars_path(symbol, d, warmup_days))

    # ── bars (H1) ─────────────────────────────────────────────────────────────

    def h1_bars_cached(self, symbol: str, d: date, warmup_days: int) -> bool:
        return self._h1_bars_path(symbol, d, warmup_days).exists()

    def load_h1_bars(self, symbol: str, d: date, warmup_days: int) -> list[BarEvent]:
        rows = _read_parquet(self._h1_bars_path(symbol, d, warmup_days))
        return [_deserialize_bar(r) for r in rows]

    def save_h1_bars(self, bars: list[BarEvent], symbol: str, d: date, warmup_days: int) -> None:
        _write_parquet([_serialize_bar(b) for b in bars], self._h1_bars_path(symbol, d, warmup_days))

    # ── trades ────────────────────────────────────────────────────────────────

    def trades_cached(self, symbol: str, d: date) -> bool:
        return self._trades_path(symbol, d).exists()

    def load_trades(self, symbol: str, d: date) -> list[TradeEvent]:
        rows = _read_parquet(self._trades_path(symbol, d))
        return [_deserialize_trade(r) for r in rows]

    def save_trades(self, trades: list[TradeEvent], symbol: str, d: date) -> None:
        _write_parquet([_serialize_trade(t) for t in trades], self._trades_path(symbol, d))

    # ── quotes ────────────────────────────────────────────────────────────────

    def quotes_cached(self, symbol: str, d: date) -> bool:
        return self._quotes_path(symbol, d).exists()

    def load_quotes(self, symbol: str, d: date) -> list[QuoteEvent]:
        rows = _read_parquet(self._quotes_path(symbol, d))
        return [_deserialize_quote(r) for r in rows]

    def save_quotes(self, quotes: list[QuoteEvent], symbol: str, d: date) -> None:
        _write_parquet([_serialize_quote(q) for q in quotes], self._quotes_path(symbol, d))

    # ── meta ──────────────────────────────────────────────────────────────────

    def read_meta(self, symbol: str, d: date) -> dict | None:
        p = self._meta_path(symbol, d)
        if not p.exists():
            return None
        return json.loads(p.read_text())

    def write_meta(self, symbol: str, d: date, meta: dict) -> None:
        p = self._meta_path(symbol, d)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({**meta, "schema_version": _CACHE_SCHEMA_VERSION}, indent=2))


# ── ReplayResultSaver ─────────────────────────────────────────────────────────

class ReplayResultSaver:
    """
    Accumulates per-bar engine state during a replay session and writes
    structured JSON + CSV at the end.

    JSON is the primary format for AI analysis. Each bar includes:
      - OHLCV + indicators
      - Full thesis state (type, state, confidence, evidence, entry/stop/target)
      - changed_this_bar flag — lets AI scan only inflection points
      - Active setups (compact TYPE:STATE strings)
      - Market state (trend, day_type, live_bias)

    CSV is a narrow tabular summary for quick human eyeballing in a spreadsheet.
    """

    # CSV columns — intentionally narrower than JSON
    _CSV_COLUMNS = [
        "time_et", "timestamp_utc",
        "open", "high", "low", "close", "volume",
        "vwap", "ema9", "ema21", "ema20", "atr14",
        "ema9_5m", "ema21_5m",
        "session_phase", "is_above_vwap", "vwap_deviation_pct",
        "thesis_type", "thesis_state", "thesis_confidence", "thesis_changed",
        "setups_active_count",
        "trend", "day_type", "live_bias",
    ]

    def __init__(self, repo_root: Path, symbol: str, session_date: date) -> None:
        self._out_dir = repo_root / "data" / "replay_results" / symbol / str(session_date)
        self._bars: list[dict] = []
        self._thesis_change_count = 0
        self._setup_triggered_count = 0
        self._setup_invalidated_count = 0
        self._setup_total_count = 0
        self._seen_setup_ids: set = set()

    def record_bar(
        self,
        bar: BarEvent,
        snap: "BarSnapshot | None",
        thesis: "ActiveThesis | None",
        active_setups: list,
        market_state: "MarketState | None",
        prev_thesis_type: object,
        prev_thesis_state: object,
    ) -> None:
        """Called once per session bar after bus.flush(). Accumulates state."""
        from datetime import timedelta
        _ET = timezone(timedelta(hours=-4))

        time_et = bar.timestamp.astimezone(_ET).strftime("%H:%M")

        # ── indicators ────────────────────────────────────────────────────────
        indic: dict = {}
        if snap:
            indic = {
                "vwap": _dec(snap.vwap),
                "vwap_deviation_pct": round(snap.vwap_deviation_pct, 4),
                "ema9": _dec(snap.ema_9),
                "ema21": _dec(snap.ema_21),
                "ema20": _dec(snap.ema_20),
                "ema50": _dec(snap.ema_50),
                "ema9_5m": _dec(snap.ema9_5m),
                "ema21_5m": _dec(snap.ema21_5m),
                "ema9_1h": _dec(snap.ema9_1h),
                "ema21_1h": _dec(snap.ema21_1h),
                "ema50_1h": _dec(snap.ema50_1h),
                "sma200_1h": _dec(snap.sma200_1h),
                "atr14": _dec(snap.atr_14),
                "session_phase": str(snap.session_phase),
                "orb_high": _dec(snap.orb_high),
                "orb_low": _dec(snap.orb_low),
                "orb_state": str(snap.orb_state),
                "is_above_vwap": snap.is_above_vwap,
                "bars_above_vwap": snap.bars_above_vwap,
                "bars_below_vwap": snap.bars_below_vwap,
                "vwap_cross_up": snap.vwap_cross_up,
                "vwap_cross_down": snap.vwap_cross_down,
                "swept_below_vwap": snap.swept_below_vwap,
                "is_new_hod": snap.is_new_hod,
                "is_new_lod": snap.is_new_lod,
                "is_lower_high": snap.is_lower_high,
                "bar_close_position_pct": round(snap.bar_close_position_pct, 3) if snap.bar_close_position_pct is not None else None,
                "ema9_slope": round(snap.ema_9_slope, 6) if snap.ema_9_slope is not None else None,
                "ema9_slope_direction": snap.ema_9_slope_direction,
            }

        # ── thesis ────────────────────────────────────────────────────────────
        thesis_dict: dict | None = None
        changed = False
        if thesis and thesis.dominant:
            dom = thesis.dominant
            ttype = dom.thesis_type.value if hasattr(dom.thesis_type, "value") else str(dom.thesis_type)
            tstate = dom.state.value if hasattr(dom.state, "value") else str(dom.state)
            changed = (dom.state != prev_thesis_state or dom.thesis_type != prev_thesis_type)
            if changed:
                self._thesis_change_count += 1
            thesis_dict = {
                "type": ttype,
                "state": tstate,
                "confidence": round(dom.confidence, 3),
                "bars_alive": dom.bars_alive,
                "changed_this_bar": changed,
                "pre_vwap_break": getattr(dom, "pre_vwap_break", False),
                "entry": _dec(dom.entry),
                "stop": _dec(dom.stop),
                "target": _dec(dom.target),
                "rejection_high": _dec(dom.rejection_high) if hasattr(dom, "rejection_high") else None,
                "sweep_low": _dec(dom.sweep_low) if hasattr(dom, "sweep_low") else None,
                "evidence_positive": [e.text for e in dom.evidence if e.positive] if dom.evidence else [],
                "evidence_negative": [e.text for e in dom.evidence if not e.positive] if dom.evidence else [],
                "commit_conditions": dom.commit_conditions or [],
                "invalidation_reason": dom.invalidation_reason,
            }

        # ── setups ────────────────────────────────────────────────────────────
        setup_labels = []
        for s in active_setups:
            sid = str(s.setup_id) if hasattr(s, "setup_id") else None
            stype = s.setup_type.value if hasattr(s.setup_type, "value") else str(s.setup_type)
            sstate = s.state.value if hasattr(s.state, "value") else str(s.state)
            setup_labels.append(f"{stype}:{sstate}")
            if sid and sid not in self._seen_setup_ids:
                self._seen_setup_ids.add(sid)
                self._setup_total_count += 1
            if "triggered" in sstate.lower():
                self._setup_triggered_count += 1
            if "invalid" in sstate.lower():
                self._setup_invalidated_count += 1

        # ── market state ──────────────────────────────────────────────────────
        mstate_dict: dict | None = None
        if market_state:
            mstate_dict = {
                "trend": str(market_state.trend),
                "trend_strength": round(market_state.trend_strength, 3),
                "vwap_state": str(market_state.vwap_state),
                "orb_state": str(market_state.orb_state),
                "day_type": str(market_state.day_type),
                "day_type_status": str(market_state.day_type_status),
                "live_bias": str(market_state.live_bias),
                "structure_score": round(market_state.structure_score, 3),
            }

        bar_dict = {
            "time_et": time_et,
            "timestamp_utc": bar.timestamp.isoformat(),
            "open": _dec(bar.open),
            "high": _dec(bar.high),
            "low": _dec(bar.low),
            "close": _dec(bar.close),
            "volume": bar.volume,
            **indic,
            "thesis": thesis_dict,
            "setups_active": setup_labels,
            "market_state": mstate_dict,
        }
        self._bars.append(bar_dict)

    def save(
        self,
        meta: dict,
        full_signals: bool,
        thesis_final: object | None,
        snap_final: "BarSnapshot | None",
        active_setups_at_close: list,
    ) -> tuple[Path, Path]:
        """Write JSON + CSV results. Returns (json_path, csv_path)."""
        self._out_dir.mkdir(parents=True, exist_ok=True)

        now = datetime.now(_UTC)
        run_ts = now.strftime("%Y%m%d_%H%M%S")
        prefix = "full" if full_signals else "bars"
        json_path = self._out_dir / f"{prefix}_{run_ts}.json"
        csv_path  = self._out_dir / f"{prefix}_{run_ts}.csv"

        # ── session summary ───────────────────────────────────────────────────
        closes = [float(b["close"]) for b in self._bars if b.get("close")]
        highs  = [float(b["high"])  for b in self._bars if b.get("high")]
        lows   = [float(b["low"])   for b in self._bars if b.get("low")]

        thesis_final_dict: dict | None = None
        if thesis_final is not None:
            dom = thesis_final
            ttype = dom.thesis_type.value if hasattr(dom.thesis_type, "value") else str(dom.thesis_type)
            tstate = dom.state.value if hasattr(dom.state, "value") else str(dom.state)
            thesis_final_dict = {
                "type": ttype,
                "state": tstate,
                "confidence": round(dom.confidence, 3),
                "bars_alive": dom.bars_alive,
                "entry": _dec(dom.entry) if hasattr(dom, "entry") else None,
                "stop": _dec(dom.stop) if hasattr(dom, "stop") else None,
                "target": _dec(dom.target) if hasattr(dom, "target") else None,
                "evidence_positive": [e.text for e in dom.evidence if e.positive] if dom.evidence else [],
                "evidence_negative": [e.text for e in dom.evidence if not e.positive] if dom.evidence else [],
                "invalidation_reason": dom.invalidation_reason if hasattr(dom, "invalidation_reason") else None,
            }

        final_snap: dict = {}
        if snap_final:
            final_snap = {
                "vwap": _dec(snap_final.vwap),
                "ema9": _dec(snap_final.ema_9),
                "ema21": _dec(snap_final.ema_21),
                "ema20": _dec(snap_final.ema_20),
                "ema50": _dec(snap_final.ema_50),
                "ema9_5m": _dec(snap_final.ema9_5m),
                "ema21_5m": _dec(snap_final.ema21_5m),
                "ema9_1h": _dec(snap_final.ema9_1h),
                "ema21_1h": _dec(snap_final.ema21_1h),
                "ema50_1h": _dec(snap_final.ema50_1h),
                "sma200_1h": _dec(snap_final.sma200_1h),
                "atr14": _dec(snap_final.atr_14),
                "orb_high": _dec(snap_final.orb_high),
                "orb_low": _dec(snap_final.orb_low),
            }

        setups_at_close = []
        for s in active_setups_at_close:
            stype = s.setup_type.value if hasattr(s.setup_type, "value") else str(s.setup_type)
            sstate = s.state.value if hasattr(s.state, "value") else str(s.state)
            setups_at_close.append({
                "setup_type": stype,
                "state": sstate,
                "score": s.score,
            })

        session_summary = {
            "session_open": self._bars[0]["open"] if self._bars else None,
            "session_high": str(max(highs)) if highs else None,
            "session_low": str(min(lows)) if lows else None,
            "session_close": self._bars[-1]["close"] if self._bars else None,
            "final_indicators": final_snap,
            "thesis_final": thesis_final_dict,
            "active_setups_at_close": setups_at_close,
            "thesis_change_count": self._thesis_change_count,
            "setup_total_seen": self._setup_total_count,
            "setup_triggered": self._setup_triggered_count,
            "setup_invalidated": self._setup_invalidated_count,
            "total_bars": len(self._bars),
        }

        result = {
            "meta": {**meta, "generated_at": now.isoformat(), "full_signals": full_signals},
            "session_summary": session_summary,
            "bars": self._bars,
        }

        # ── write JSON ────────────────────────────────────────────────────────
        json_path.write_text(json.dumps(result, indent=2, default=str))

        # ── write CSV ─────────────────────────────────────────────────────────
        with csv_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self._CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for b in self._bars:
                th = b.get("thesis") or {}
                ms = b.get("market_state") or {}
                writer.writerow({
                    "time_et": b.get("time_et"),
                    "timestamp_utc": b.get("timestamp_utc"),
                    "open": b.get("open"),
                    "high": b.get("high"),
                    "low": b.get("low"),
                    "close": b.get("close"),
                    "volume": b.get("volume"),
                    "vwap": b.get("vwap"),
                    "ema9": b.get("ema9"),
                    "ema21": b.get("ema21"),
                    "ema20": b.get("ema20"),
                    "atr14": b.get("atr14"),
                    "ema9_5m": b.get("ema9_5m"),
                    "ema21_5m": b.get("ema21_5m"),
                    "session_phase": b.get("session_phase"),
                    "is_above_vwap": b.get("is_above_vwap"),
                    "vwap_deviation_pct": b.get("vwap_deviation_pct"),
                    "thesis_type": th.get("type"),
                    "thesis_state": th.get("state"),
                    "thesis_confidence": th.get("confidence"),
                    "thesis_changed": th.get("changed_this_bar"),
                    "setups_active_count": len(b.get("setups_active") or []),
                    "trend": ms.get("trend"),
                    "day_type": ms.get("day_type"),
                    "live_bias": ms.get("live_bias"),
                })

        # ── update index.json ─────────────────────────────────────────────────
        index_path = self._out_dir / "index.json"
        index: dict = {"runs": []}
        if index_path.exists():
            try:
                index = json.loads(index_path.read_text())
            except Exception:
                pass
        index["symbol"] = meta.get("symbol")
        index["session_date"] = meta.get("session_date")
        index["runs"].append({
            "json_file": json_path.name,
            "csv_file": csv_path.name,
            "run_at": now.isoformat(),
            "full_signals": full_signals,
            "total_bars": len(self._bars),
        })
        index_path.write_text(json.dumps(index, indent=2))

        return json_path, csv_path
