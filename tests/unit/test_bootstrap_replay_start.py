"""
Unit tests for BootstrapEngine.compute_replay_start()'s cap on live-feed replay lookback.

Reproduces a bug traced from a live incident: the live feed subscribed with
`start=m1_end - 1 minute`, assuming catch-up always finishes close to "now".
On a run where catch-up took long enough (or the historical API's latest
available bar was itself stale) that m1_end was ~22 minutes behind real time,
this requested a ~22-minute intraday replay from Databento's live gateway —
landing right at market open, when that backlog is largest. Observed as
IngressObserver ts_recv latency climbing from ~13s to ~33s instead of
settling near zero.

These tests pin the fixed behavior: the requested replay start is floored so
it never reaches further back than _MAX_LIVE_REPLAY_LOOKBACK from "now",
regardless of how stale m1_end is — while still doing the intended 1-bar
overlap when catch-up genuinely did finish close to real time.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from alpha.engines.bootstrap.engine import _MAX_LIVE_REPLAY_LOOKBACK, compute_replay_start


class TestComputeReplayStart:
    def test_normal_catchup_uses_one_minute_overlap(self) -> None:
        now = datetime(2026, 7, 29, 13, 52, 0, tzinfo=timezone.utc)
        m1_end = now - timedelta(seconds=30)  # catch-up finished essentially caught up

        replay_start = compute_replay_start(m1_end, now)

        assert replay_start == m1_end - timedelta(minutes=1)

    def test_stale_m1_end_is_capped_not_used_directly(self) -> None:
        now = datetime(2026, 7, 29, 13, 52, 0, tzinfo=timezone.utc)
        m1_end = now - timedelta(minutes=22)  # slow catch-up / stale historical data

        replay_start = compute_replay_start(m1_end, now)

        assert replay_start == now - _MAX_LIVE_REPLAY_LOOKBACK
        assert replay_start > m1_end  # never requests the full 22-minute backlog

    def test_cap_boundary_is_inclusive(self) -> None:
        now = datetime(2026, 7, 29, 13, 52, 0, tzinfo=timezone.utc)
        m1_end = now - _MAX_LIVE_REPLAY_LOOKBACK + timedelta(minutes=1)  # 1-min overlap == cap exactly

        replay_start = compute_replay_start(m1_end, now)

        assert replay_start == now - _MAX_LIVE_REPLAY_LOOKBACK
