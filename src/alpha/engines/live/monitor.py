"""
IngestionMonitor — per-symbol data quality tracker.

Monitors whether the live feed is healthy enough to allow signal generation.
The PositionMonitor checks is_signal_allowed() before emitting WOULD_ENTER.

Checks (per symbol):
  - Bar lag:   completed M1 bar arriving >250ms after expected wall-clock close time
  - Bar gap:   missing M1 bar (timestamp jump >1 minute + 5s tolerance)
  - Silence:   no trade or quote record for >30s during RTH (09:30–16:00 ET)

State machine:
  CLEAN      →  DEGRADED    any check fails
  DEGRADED   →  RECOVERING  first clean M1 bar arrives
  RECOVERING →  CLEAN       second consecutive clean bar (2 bars needed)
  any        →  FAILED      silence ≥5min during RTH (requires investigation to reset)

Kill switch:
  is_signal_allowed(symbol) → False when state is DEGRADED, RECOVERING, or FAILED
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from alpha.models.enums import DataQualityState

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")

# RTH window: 09:30–16:00 ET (silence checks only apply here)
_RTH_OPEN  = (9, 30)
_RTH_CLOSE = (16, 0)

# Thresholds
_BAR_LAG_WARN_MS    = 250   # ms — M1 bar arriving later than this is flagged
_SILENCE_DEGRADED_S = 30    # s  — silence this long during RTH → DEGRADED
_FAILED_S           = 300   # s  — silence this long during RTH → FAILED
_CLEAN_BARS_NEEDED  = 2     # consecutive clean bars required to exit DEGRADED/RECOVERING


def _is_rth(now: datetime) -> bool:
    """Return True if `now` falls within 09:30–16:00 ET."""
    et = now.astimezone(_ET)
    t = (et.hour, et.minute)
    return _RTH_OPEN <= t < _RTH_CLOSE


@dataclass
class _SymbolState:
    quality: DataQualityState = DataQualityState.CLEAN
    degraded_reason: str | None = None
    # Wall-clock time of the most recent record (trade or quote) — for silence detection
    last_record_at: datetime | None = None
    # Wall-clock time of the most recent completed M1 bar — for lag logging
    last_bar_at: datetime | None = None
    # Timestamp field of the most recent M1 bar — for gap detection
    last_bar_event_ts: datetime | None = None
    # Count of consecutive clean bars while in DEGRADED/RECOVERING
    consecutive_clean: int = 0


class IngestionMonitor:
    """
    Per-symbol data quality tracker.

    Create one instance per runtime, shared by LiveIngestionEngine
    (writes) and PositionMonitor (reads via is_signal_allowed).
    """

    def __init__(self, symbols: list[str]) -> None:
        self._states: dict[str, _SymbolState] = {s: _SymbolState() for s in symbols}

    # ── Public write API (called by LiveIngestionEngine) ──────────────────────

    def on_bar_received(
        self,
        symbol: str,
        bar_ts: datetime,
        received_at: datetime,
    ) -> None:
        """
        Call when a completed M1 bar arrives.

        bar_ts      — the bar's own timestamp (start-of-bar, e.g. 09:30:00)
        received_at — wall-clock time we received it (datetime.now(UTC))
        """
        state = self._states.get(symbol)
        if state is None:
            return

        state.last_bar_at = received_at
        state.last_record_at = received_at

        issues: list[str] = []

        # ── Lag check ────────────────────────────────────────────────────────
        # A bar with open timestamp T covers [T, T+60s). The exchange publishes
        # the closed bar just after T+60s. We expect delivery within 250ms.
        expected_wall = bar_ts.replace(second=0, microsecond=0) + timedelta(seconds=60)
        lag_ms = (received_at - expected_wall).total_seconds() * 1000
        if lag_ms > _BAR_LAG_WARN_MS:
            issues.append(f"bar_lag={lag_ms:.0f}ms>{_BAR_LAG_WARN_MS}ms")

        # ── Gap check ────────────────────────────────────────────────────────
        if state.last_bar_event_ts is not None:
            expected_next = state.last_bar_event_ts + timedelta(minutes=1)
            # 5-second tolerance for overnight/weekend micro-gaps between sessions
            if bar_ts > expected_next + timedelta(seconds=5):
                issues.append(
                    f"bar_gap: expected {expected_next.strftime('%H:%M:%S')} "
                    f"got {bar_ts.strftime('%H:%M:%S')}"
                )

        state.last_bar_event_ts = bar_ts

        if issues:
            self._degrade(symbol, state, "; ".join(issues))
        else:
            self._record_clean_bar(symbol, state)

    def on_skipped_records(self) -> None:
        """
        Call when Databento reports SKIPPED_RECORDS_AFTER_SLOW_READING.

        Degrades all tracked symbols — we don't know which instruments were
        affected, so the safe default is to treat all as suspect until the
        next clean bar arrives.
        """
        now = datetime.now(timezone.utc)
        for symbol, state in self._states.items():
            self._degrade(symbol, state, "Databento skipped records (slow reader)")
        logger.warning(
            "IngestionMonitor: Databento reported SKIPPED_RECORDS — degraded %d symbol(s)",
            len(self._states),
        )

    def on_ingress_overload(self) -> None:
        """
        Call when the bounded ingress queue is full and records are being dropped.

        Sets FAILED (not DEGRADED) because dropped records mean the in-memory
        state (VWAP, trade flow, quote imbalance, cumulative volume) is no longer
        consistent with the actual market feed — it cannot self-heal by waiting
        for the next clean bar. Recovery requires an explicit resync / restart.

        This is a DATA_LOSS condition, distinct from BACKLOGGED (lag only,
        no drops): a backlogged consumer still sees all events and recovers
        once it catches up. A queue full drop means the event is gone.

        Called via loop.call_soon_threadsafe() from the background record thread
        so state mutation stays on the event-loop thread (single writer).
        """
        reason = "Ingress records dropped — live state integrity unknown, resync required"
        for symbol, state in self._states.items():
            if state.quality != DataQualityState.FAILED:
                self._set_state(symbol, state, DataQualityState.FAILED, reason)
        logger.error(
            "IngestionMonitor: ingress DATA_LOSS — FAILED %d symbol(s)."
            " Resync required before signals resume.",
            len(self._states),
        )

    def on_record_received(self, symbol: str, now: datetime | None = None) -> None:
        """
        Call on every trade or quote record for silence detection.

        Passing `now` avoids repeated datetime.now() calls in hot paths;
        omit it and the method will call datetime.now(UTC) itself.
        """
        state = self._states.get(symbol)
        if state is None:
            return
        state.last_record_at = now or datetime.now(timezone.utc)

    def check_health(self, now: datetime | None = None) -> None:
        """
        Periodic silence check. Call every second from LiveIngestionEngine.

        Only marks DEGRADED/FAILED for silence during RTH — overnight silence is normal.
        """
        now = now or datetime.now(timezone.utc)
        if not _is_rth(now):
            return

        for symbol, state in self._states.items():
            if state.last_record_at is None:
                # No records ever received — don't penalise before the feed has started.
                continue

            silence_s = (now - state.last_record_at).total_seconds()

            if silence_s >= _FAILED_S:
                if state.quality != DataQualityState.FAILED:
                    self._set_state(
                        symbol, state, DataQualityState.FAILED,
                        f"silence={silence_s:.0f}s>={_FAILED_S}s during RTH",
                    )
            elif silence_s >= _SILENCE_DEGRADED_S:
                if state.quality == DataQualityState.CLEAN:
                    self._degrade(
                        symbol, state,
                        f"silence={silence_s:.0f}s>={_SILENCE_DEGRADED_S}s during RTH",
                    )

    # ── Public read API (called by PositionMonitor) ───────────────────────────

    def is_signal_allowed(self, symbol: str) -> bool:
        """
        Returns True only when data quality is CLEAN.

        DEGRADED, RECOVERING, and FAILED all block WOULD_ENTER signals.
        """
        state = self._states.get(symbol)
        if state is None:
            return True  # unknown symbol — don't block
        return state.quality == DataQualityState.CLEAN

    def get_quality(self, symbol: str) -> DataQualityState:
        state = self._states.get(symbol)
        return state.quality if state else DataQualityState.CLEAN

    def get_degraded_reason(self, symbol: str) -> str | None:
        state = self._states.get(symbol)
        return state.degraded_reason if state else None

    def summary(self) -> dict[str, dict]:
        """Snapshot of all symbol states — for health endpoints and dashboards."""
        return {
            sym: {
                "quality": str(s.quality),
                "degraded_reason": s.degraded_reason,
                "signals_allowed": s.quality == DataQualityState.CLEAN,
                "last_record_at": s.last_record_at.isoformat() if s.last_record_at else None,
                "last_bar_at": s.last_bar_at.isoformat() if s.last_bar_at else None,
            }
            for sym, s in self._states.items()
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _degrade(self, symbol: str, state: _SymbolState, reason: str) -> None:
        state.consecutive_clean = 0
        if state.quality not in (DataQualityState.DEGRADED, DataQualityState.FAILED):
            self._set_state(symbol, state, DataQualityState.DEGRADED, reason)
        else:
            state.degraded_reason = reason  # update reason without extra log noise

    def _record_clean_bar(self, symbol: str, state: _SymbolState) -> None:
        if state.quality == DataQualityState.CLEAN:
            return  # already healthy
        if state.quality == DataQualityState.FAILED:
            return  # FAILED is sticky — requires operator investigation to reset

        state.consecutive_clean += 1

        if state.consecutive_clean >= _CLEAN_BARS_NEEDED:
            self._set_state(symbol, state, DataQualityState.CLEAN, None)
            state.consecutive_clean = 0
        elif state.quality == DataQualityState.DEGRADED:
            # First clean bar after degradation — enter RECOVERING
            self._set_state(
                symbol, state, DataQualityState.RECOVERING,
                f"1/{_CLEAN_BARS_NEEDED} clean bars (waiting for confirmation)",
            )

    def _set_state(
        self,
        symbol: str,
        state: _SymbolState,
        new_quality: DataQualityState,
        reason: str | None,
    ) -> None:
        old = state.quality
        state.quality = new_quality
        state.degraded_reason = reason
        level = logging.WARNING if new_quality != DataQualityState.CLEAN else logging.INFO
        logger.log(
            level,
            "IngestionMonitor [%s]: %s → %s%s",
            symbol, old, new_quality,
            f" ({reason})" if reason else "",
        )
