"""
IngressObserver
===============
Lightweight per-record observer for the Databento live ingress loop.

Sits at the top of _record_loop(), called once per record before dispatch.
All work is O(1) and non-blocking — never touches disk, asyncio, or locks.

Emits a periodic summary to the logger (every LOG_INTERVAL_RECORDS records
or LOG_INTERVAL_SECS seconds, whichever comes first):

    IngressObserver | 500 records in 10.2s (49.0/s) | inter-record gap
    avg=20ms max=840ms | out_of_order=2 | ts_recv_latency avg=18ms max=91ms
    | queue_wait avg=2ms max=18ms | dropped=0

Warnings are emitted immediately when:
    - inter-record gap > GAP_WARN_MS (feed stall)
    - ts_recv latency > RECV_LATENCY_WARN_MS (network/SDK lag)
    - queue wait > QUEUE_WAIT_WARN_MS (ingress backlog building)
    - out-of-order ts_event detected per instrument
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

# Periodic summary cadence
LOG_INTERVAL_RECORDS = 500
LOG_INTERVAL_SECS = 30.0

# Warning thresholds
GAP_WARN_MS = 5_000          # >5s between records → possible feed stall
RECV_LATENCY_WARN_MS = 500   # >500ms ts_recv → local → network/SDK lag

# Queue wait thresholds (time a record sits in the bounded ingress queue)
QUEUE_WAIT_WARN_MS     = 100    # 100ms → WARN — backlog starting to build
QUEUE_WAIT_DEGRADE_MS  = 500    # 500ms → DEGRADED — consumer falling behind
QUEUE_WAIT_CRITICAL_MS = 2_000  # 2s    → CRITICAL — severe backlog

# Both warnings below fire from the same background thread that reads records
# off the wire. logging.StreamHandler flushes on every emit, so once latency
# crosses the threshold, an unthrottled warning-per-record turns the log call
# itself into extra per-record work on the exact thread that's already behind
# — a self-reinforcing loop, not just noise. Throttle to one line per window,
# folding the suppressed count into the next line that does print.
WARN_THROTTLE_S = 2.0


class IngressObserver:
    """
    Call observe(record, arrival_ns) once per record in the Databento thread.
    arrival_ns should be time.time_ns() captured immediately on record yield.
    """

    def __init__(self) -> None:
        self._records_seen: int = 0
        self._out_of_order: int = 0
        self._window_start_ns: int = time.time_ns()
        self._window_records: int = 0

        # Inter-record gap tracking
        self._last_arrival_ns: int | None = None
        self._max_gap_ms: float = 0.0
        self._total_gap_ms: float = 0.0
        self._gap_count: int = 0

        # ts_recv latency tracking (exchange recv → our machine)
        self._max_recv_latency_ms: float = 0.0
        self._total_recv_latency_ms: float = 0.0
        self._recv_latency_count: int = 0

        # Warning throttle state (shared window per warning type)
        self._last_gap_warn_ns: int | None = None
        self._suppressed_gap_warnings: int = 0
        self._last_recv_latency_warn_ns: int | None = None
        self._suppressed_recv_latency_warnings: int = 0
        self._last_queue_wait_warn_ns: int | None = None
        self._suppressed_queue_wait_warnings: int = 0

        # Per-(instrument_id, rtype) last ts_event for out-of-order detection.
        # Keyed by rtype so bars (ts_event = bar open) don't pollute trade/quote ordering.
        self._last_ts_event_ns: dict[tuple[int, int], int] = {}

        # ts_event → ts_recv latency (CME → Databento gateway)
        self._max_feed_latency_ms: float = 0.0
        self._total_feed_latency_ms: float = 0.0
        self._feed_latency_count: int = 0

        # Bounded ingress queue metrics (populated by _ingress_consumer in event loop)
        self._total_queue_wait_ms: float = 0.0
        self._max_queue_wait_ms: float = 0.0
        self._queue_wait_count: int = 0
        self._window_dropped: int = 0
        self._total_dropped: int = 0

    def observe(self, record: object, arrival_ns: int) -> None:
        self._records_seen += 1
        self._window_records += 1

        # ── Inter-record gap ──────────────────────────────────────────────────
        if self._last_arrival_ns is not None:
            gap_ms = (arrival_ns - self._last_arrival_ns) / 1_000_000
            self._gap_count += 1
            self._total_gap_ms += gap_ms
            if gap_ms > self._max_gap_ms:
                self._max_gap_ms = gap_ms
            if gap_ms > GAP_WARN_MS:
                if (
                    self._last_gap_warn_ns is None
                    or (arrival_ns - self._last_gap_warn_ns) / 1_000_000_000 >= WARN_THROTTLE_S
                ):
                    suppressed = self._suppressed_gap_warnings
                    self._suppressed_gap_warnings = 0
                    self._last_gap_warn_ns = arrival_ns
                    logger.warning(
                        "IngressObserver: feed gap %.0fms (no records for %.1fs)%s",
                        gap_ms, gap_ms / 1000,
                        f" [{suppressed} more suppressed]" if suppressed else "",
                    )
                else:
                    self._suppressed_gap_warnings += 1
        self._last_arrival_ns = arrival_ns

        # ── ts_recv latency (Databento gateway → our machine) ────────────────
        ts_recv = getattr(record, "ts_recv", None)
        if ts_recv is not None and ts_recv > 0:
            recv_latency_ms = (arrival_ns - ts_recv) / 1_000_000
            if recv_latency_ms >= 0:  # negative = clock skew, skip
                self._recv_latency_count += 1
                self._total_recv_latency_ms += recv_latency_ms
                if recv_latency_ms > self._max_recv_latency_ms:
                    self._max_recv_latency_ms = recv_latency_ms
                if recv_latency_ms > RECV_LATENCY_WARN_MS:
                    if (
                        self._last_recv_latency_warn_ns is None
                        or (arrival_ns - self._last_recv_latency_warn_ns) / 1_000_000_000 >= WARN_THROTTLE_S
                    ):
                        suppressed = self._suppressed_recv_latency_warnings
                        self._suppressed_recv_latency_warnings = 0
                        self._last_recv_latency_warn_ns = arrival_ns
                        logger.warning(
                            "IngressObserver: high ts_recv latency %.0fms (instrument_id=%s)%s",
                            recv_latency_ms, getattr(record, "instrument_id", "?"),
                            f" [{suppressed} more suppressed]" if suppressed else "",
                        )
                    else:
                        self._suppressed_recv_latency_warnings += 1

        # ── Feed latency (CME → Databento gateway: ts_recv - ts_event) ───────
        ts_event = getattr(record, "ts_event", None)
        if ts_recv is not None and ts_event is not None and ts_recv > 0 and ts_event > 0:
            feed_latency_ms = (ts_recv - ts_event) / 1_000_000
            if 0 <= feed_latency_ms < 60_000:  # sanity: skip negatives and bars (open time)
                self._feed_latency_count += 1
                self._total_feed_latency_ms += feed_latency_ms
                if feed_latency_ms > self._max_feed_latency_ms:
                    self._max_feed_latency_ms = feed_latency_ms

        # ── Out-of-order detection per (instrument_id, rtype) ────────────────
        iid = getattr(record, "instrument_id", None)
        rtype = getattr(record, "rtype", None)
        if ts_event is not None and iid is not None and rtype is not None:
            key = (int(iid), int(rtype))
            prev = self._last_ts_event_ns.get(key)
            if prev is not None and ts_event < prev:
                self._out_of_order += 1
                logger.debug(
                    "IngressObserver: out-of-order record instrument_id=%d rtype=%d "
                    "ts_event=%d prev=%d delta_ms=%.1f",
                    iid, int(rtype), ts_event, prev, (prev - ts_event) / 1_000_000,
                )
            if ts_event > 0:
                self._last_ts_event_ns[key] = ts_event

        # ── Periodic summary ──────────────────────────────────────────────────
        elapsed_s = (arrival_ns - self._window_start_ns) / 1_000_000_000
        if (
            self._window_records >= LOG_INTERVAL_RECORDS
            or elapsed_s >= LOG_INTERVAL_SECS
        ):
            self._log_summary(elapsed_s)
            self._reset_window(arrival_ns)

    def observe_queue_wait(self, queue_wait_ms: float) -> None:
        """
        Called by _ingress_consumer (event loop) for each record drained from
        the bounded queue. queue_wait_ms is the age of the record in the queue.
        """
        self._queue_wait_count += 1
        self._total_queue_wait_ms += queue_wait_ms
        if queue_wait_ms > self._max_queue_wait_ms:
            self._max_queue_wait_ms = queue_wait_ms

        if queue_wait_ms >= QUEUE_WAIT_DEGRADE_MS:
            now_ns = time.time_ns()
            if (
                self._last_queue_wait_warn_ns is None
                or (now_ns - self._last_queue_wait_warn_ns) / 1_000_000_000 >= WARN_THROTTLE_S
            ):
                suppressed = self._suppressed_queue_wait_warnings
                self._suppressed_queue_wait_warnings = 0
                self._last_queue_wait_warn_ns = now_ns
                level = logging.ERROR if queue_wait_ms >= QUEUE_WAIT_CRITICAL_MS else logging.WARNING
                logger.log(
                    level,
                    "IngressObserver: ingress queue backlog %.0fms (consumer falling behind)%s",
                    queue_wait_ms,
                    f" [{suppressed} more suppressed]" if suppressed else "",
                )
            else:
                self._suppressed_queue_wait_warnings += 1

    def record_ingress_drop(self) -> None:
        """
        Called by _record_loop (background thread) when put_nowait raises queue.Full.

        Only increments counters — no per-record logging to avoid a self-reinforcing
        slow-path feedback loop (log I/O on the already-overloaded record thread).
        The periodic summary in _log_summary() reports the window drop count.
        The throttled aggregate below fires at most once per WARN_THROTTLE_S seconds.
        """
        self._window_dropped += 1
        self._total_dropped += 1
        now_ns = time.time_ns()
        if (
            self._last_queue_wait_warn_ns is None
            or (now_ns - self._last_queue_wait_warn_ns) / 1_000_000_000 >= WARN_THROTTLE_S
        ):
            dropped_this_window = self._window_dropped
            self._last_queue_wait_warn_ns = now_ns
            logger.error(
                "IngressObserver: ingress queue full — %d record(s) dropped this window"
                " (total=%d)",
                dropped_this_window,
                self._total_dropped,
            )

    def _log_summary(self, elapsed_s: float) -> None:
        rate = self._window_records / elapsed_s if elapsed_s > 0 else 0.0

        gap_avg = (
            self._total_gap_ms / self._gap_count if self._gap_count > 0 else 0.0
        )
        recv_avg = (
            self._total_recv_latency_ms / self._recv_latency_count
            if self._recv_latency_count > 0
            else None
        )

        recv_str = (
            f" | recv_latency avg={recv_avg:.0f}ms max={self._max_recv_latency_ms:.0f}ms"
            if recv_avg is not None
            else ""
        )

        feed_avg = (
            self._total_feed_latency_ms / self._feed_latency_count
            if self._feed_latency_count > 0
            else None
        )
        feed_str = (
            f" | feed_latency avg={feed_avg:.0f}ms max={self._max_feed_latency_ms:.0f}ms"
            if feed_avg is not None
            else ""
        )

        queue_avg = (
            self._total_queue_wait_ms / self._queue_wait_count
            if self._queue_wait_count > 0
            else None
        )
        queue_str = (
            f" | queue_wait avg={queue_avg:.0f}ms max={self._max_queue_wait_ms:.0f}ms"
            f" dropped={self._window_dropped}"
            if queue_avg is not None
            else f" | queue_wait n/a dropped={self._window_dropped}"
        )

        logger.info(
            "IngressObserver | %d records in %.1fs (%.1f/s)"
            " | gap avg=%.0fms max=%.0fms"
            " | out_of_order=%d (total)%s%s%s",
            self._window_records, elapsed_s, rate,
            gap_avg, self._max_gap_ms,
            self._out_of_order,
            recv_str,
            feed_str,
            queue_str,
        )

    def _reset_window(self, now_ns: int) -> None:
        self._window_start_ns = now_ns
        self._window_records = 0
        self._total_gap_ms = 0.0
        self._max_gap_ms = 0.0
        self._gap_count = 0
        self._total_recv_latency_ms = 0.0
        self._max_recv_latency_ms = 0.0
        self._recv_latency_count = 0
        self._total_feed_latency_ms = 0.0
        self._max_feed_latency_ms = 0.0
        self._feed_latency_count = 0
        self._total_queue_wait_ms = 0.0
        self._max_queue_wait_ms = 0.0
        self._queue_wait_count = 0
        self._window_dropped = 0
        # out_of_order and _total_dropped are cumulative — not reset
