from __future__ import annotations

from datetime import datetime, timezone

from alpha.engines.historical.sources.ibkr import _future_contract_segments

_UTC = timezone.utc


def test_future_contract_segments_split_by_quarter_rolls() -> None:
    start = datetime(2025, 3, 20, 0, 0, tzinfo=_UTC)
    end = datetime(2025, 7, 1, 0, 0, tzinfo=_UTC)

    segments = _future_contract_segments(start, end)

    assert [contract_month for _, _, contract_month in segments] == ["202503", "202506", "202509"]
    assert segments[0][0] == start
    assert segments[-1][1] == end
    assert segments[0][1] <= segments[1][0]


def test_future_contract_segments_cover_long_historical_window() -> None:
    start = datetime(2022, 4, 18, 7, 51, 45, tzinfo=_UTC)
    end = datetime(2025, 3, 23, 13, 30, tzinfo=_UTC)

    segments = _future_contract_segments(start, end)

    assert segments[0][0] == start
    assert segments[-1][1] == end
    assert segments[0][2] == "202206"
    assert segments[-1][2] == "202503"
    assert len(segments) > 1
