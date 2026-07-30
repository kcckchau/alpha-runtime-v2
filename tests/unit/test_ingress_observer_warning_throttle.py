"""
Unit tests for IngressObserver's warning throttle.

Reproduces a suspected feedback loop flagged while debugging live latency: the
"high ts_recv latency" and "feed gap" warnings fired on every single
qualifying record with no throttle, from the same background thread that
reads records off the wire. logging.StreamHandler flushes on every emit, so
once latency crossed the threshold, the warning call itself became extra
per-record work on the exact thread that was already behind — plausibly
self-reinforcing, not just log noise.

These tests pin the fixed behavior: repeated qualifying records within
WARN_THROTTLE_S collapse into a single log call (with a suppressed-count
tail), and the warning resumes once the throttle window has elapsed.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from alpha.engines.live.ingress_observer import RECV_LATENCY_WARN_MS, WARN_THROTTLE_S, IngressObserver

_NS_PER_S = 1_000_000_000


def _record(ts_recv_ns: int, instrument_id: int = 42004800) -> SimpleNamespace:
    return SimpleNamespace(ts_recv=ts_recv_ns, instrument_id=instrument_id, ts_event=None, rtype=None)


class TestRecvLatencyWarningThrottle:
    def test_many_qualifying_records_in_one_window_log_once(self) -> None:
        observer = IngressObserver()
        base_ns = 10 * _NS_PER_S
        latency_ns = int((RECV_LATENCY_WARN_MS + 100) * 1_000_000)  # always above threshold

        with patch("alpha.engines.live.ingress_observer.logger") as mock_logger:
            for i in range(20):
                arrival_ns = base_ns + i * 1_000_000  # 1ms apart — all inside the throttle window
                observer.observe(_record(arrival_ns - latency_ns), arrival_ns)

            assert mock_logger.warning.call_count == 1

    def test_warning_fires_again_after_throttle_window_elapses(self) -> None:
        observer = IngressObserver()
        base_ns = 10 * _NS_PER_S
        latency_ns = int((RECV_LATENCY_WARN_MS + 100) * 1_000_000)

        with patch("alpha.engines.live.ingress_observer.logger") as mock_logger:
            observer.observe(_record(base_ns - latency_ns), base_ns)
            later_ns = base_ns + int((WARN_THROTTLE_S + 0.5) * _NS_PER_S)
            observer.observe(_record(later_ns - latency_ns), later_ns)

            assert mock_logger.warning.call_count == 2

    def test_suppressed_count_is_reported_on_the_next_line(self) -> None:
        observer = IngressObserver()
        base_ns = 10 * _NS_PER_S
        latency_ns = int((RECV_LATENCY_WARN_MS + 100) * 1_000_000)

        with patch("alpha.engines.live.ingress_observer.logger") as mock_logger:
            for i in range(5):
                arrival_ns = base_ns + i * 1_000_000
                observer.observe(_record(arrival_ns - latency_ns), arrival_ns)
            later_ns = base_ns + int((WARN_THROTTLE_S + 0.5) * _NS_PER_S)
            observer.observe(_record(later_ns - latency_ns), later_ns)

            second_call_args = mock_logger.warning.call_args_list[1].args
            assert "4 more suppressed" in second_call_args[-1]

    def test_below_threshold_never_warns(self) -> None:
        observer = IngressObserver()
        base_ns = 10 * _NS_PER_S
        latency_ns = int((RECV_LATENCY_WARN_MS - 50) * 1_000_000)  # under threshold

        with patch("alpha.engines.live.ingress_observer.logger") as mock_logger:
            for i in range(10):
                arrival_ns = base_ns + i * _NS_PER_S * 3  # spaced well past the throttle window too
                observer.observe(_record(arrival_ns - latency_ns), arrival_ns)

            assert mock_logger.warning.call_count == 0
