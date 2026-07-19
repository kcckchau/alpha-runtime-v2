"""
VolumeProfileBuilder — pure function, no side effects.

Computes POC / VAH / VAL / HVN / LVN from a list of sealed bars or trades.

Bar-based distribution (build):
    For each bar, volume is distributed uniformly across all price bins
    spanned by [low, high]. Standard approximation when tick data unavailable.
    Volume is allocated with floor-remainder to guarantee:
        sum(distributed_volume_per_bar) == bar.volume  (no inflation)

    WARNING: bar-based HVN/LVN are unreliable because uniform spreading
    artificially flattens the distribution. Use POC/VA for research baseline
    only. HVN/LVN from bars should NOT be registered as trading levels.

Trade-based distribution (build_from_trades):
    Each trade is placed in the exact bin for its price.
    Taker side is tracked per bin → delta_distribution (buy - sell per bin).
    Produces exact volume placement and delta profile. Preferred when available.
    Data source: Databento trades schema (event_type="trade"), not MBP-1 quotes.

Policy version: VOLUME_PROFILE_POLICY_VERSION = "vp_v1"

Policy definitions (vp_v1):
    value_area_pct:         0.70 (70% of session volume)
    value_area_target:      math.ceil(total_volume * pct)  — ensures true ≥70%
    value_area_tie_break:   expand upward when vol_above == vol_below
    poc_tie_break:          lowest price wins when two bins share max volume
    hvn_definition:         strict local maximum (both neighbours strictly lower)
    lvn_definition:         strict local minimum (both neighbours strictly higher)
    hvn/lvn note:           single-bin, no smoothing, no prominence filter.
                            Treat as research visualization only; not suitable
                            as trading level triggers without further filtering.

HVN/LVN are capped at max_hvn / max_lvn. Default 5 each.
Bin size: 1.0 MNQ point (4 ticks) by default. Configurable.
"""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal, ROUND_DOWN

from alpha.models.bar import Bar
from alpha.models.enums import TakerSide
from alpha.models.trade import Trade
from alpha.models.volume_profile import VolumeProfile

VOLUME_PROFILE_POLICY_VERSION: str = "vp_v1"
DEFAULT_BIN_SIZE: Decimal = Decimal("1.0")
VALUE_AREA_PCT: float = 0.70
DEFAULT_MAX_HVN: int = 5
DEFAULT_MAX_LVN: int = 5


class VolumeProfileBuilder:
    def __init__(
        self,
        bin_size: Decimal = DEFAULT_BIN_SIZE,
        value_area_pct: float = VALUE_AREA_PCT,
        max_hvn: int = DEFAULT_MAX_HVN,
        max_lvn: int = DEFAULT_MAX_LVN,
    ) -> None:
        self.bin_size = bin_size
        self.value_area_pct = value_area_pct
        self.max_hvn = max_hvn
        self.max_lvn = max_lvn

    def build(
        self,
        bars: list[Bar],
        symbol: str,
        session_date: date,
        session_type: str = "rth",
    ) -> VolumeProfile:
        if not bars:
            raise ValueError(f"No bars provided for {symbol} {session_date} {session_type}")

        dist = self._build_distribution(bars)
        return self._finalize(dist, None, symbol, session_date, session_type, "bars")

    def build_from_trades(
        self,
        trades: list[Trade],
        symbol: str,
        session_date: date,
        session_type: str = "rth",
    ) -> VolumeProfile:
        if not trades:
            raise ValueError(f"No trades provided for {symbol} {session_date} {session_type}")

        dist, delta = self._build_distribution_from_trades(trades)
        return self._finalize(dist, delta, symbol, session_date, session_type, "trades")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _finalize(
        self,
        dist: dict[Decimal, int],
        delta: dict[Decimal, int] | None,
        symbol: str,
        session_date: date,
        session_type: str,
        source: str,
    ) -> VolumeProfile:
        sorted_levels = sorted(dist.keys())
        total_volume = sum(dist.values())

        # POC tie-break: lowest price wins (vp_v1 policy)
        poc = min(
            (k for k in dist if dist[k] == max(dist.values())),
        )
        vah, val, va_volume = self._value_area(dist, sorted_levels, poc, total_volume)
        hvns = self._hvn(dist, sorted_levels, poc)[: self.max_hvn]
        lvns = self._lvn(dist, sorted_levels)[: self.max_lvn]

        delta_dist = (
            {str(k): delta[k] for k in sorted_levels if k in delta}
            if delta is not None
            else None
        )

        return VolumeProfile(
            symbol=symbol,
            session_date=session_date,
            session_type=session_type,
            bin_size=float(self.bin_size),
            source=source,
            poc=poc,
            vah=vah,
            val=val,
            total_volume=total_volume,
            value_area_volume=va_volume,
            hvn_levels=hvns,
            lvn_levels=lvns,
            distribution={str(k): dist[k] for k in sorted_levels},
            delta_distribution=delta_dist,
        )

    def _bin(self, price: Decimal) -> Decimal:
        """Round price down to nearest bin boundary."""
        return (price / self.bin_size).to_integral_value(rounding=ROUND_DOWN) * self.bin_size

    def _build_distribution(self, bars: list[Bar]) -> dict[Decimal, int]:
        """
        Distribute each bar's volume uniformly across its price bins.

        Uses floor-remainder allocation to guarantee conservation:
            sum(bin_volumes for bar) == bar.volume  (no inflation, no loss)
        """
        dist: dict[Decimal, int] = {}

        for bar in bars:
            low_bin = self._bin(bar.low)
            high_bin = self._bin(bar.high)

            bins: list[Decimal] = []
            current = low_bin
            while current <= high_bin:
                bins.append(current)
                current += self.bin_size

            if not bins:
                continue

            n = len(bins)
            base = bar.volume // n
            remainder = bar.volume - base * n

            # Floor allocation: every bin gets base; first `remainder` bins get +1
            for i, b in enumerate(bins):
                alloc = base + (1 if i < remainder else 0)
                dist[b] = dist.get(b, 0) + alloc

        return dist

    def _build_distribution_from_trades(
        self, trades: list[Trade]
    ) -> tuple[dict[Decimal, int], dict[Decimal, int]]:
        """Exact volume placement per trade. Returns (volume_dist, delta_dist)."""
        vol: dict[Decimal, int] = {}
        delta: dict[Decimal, int] = {}

        for trade in trades:
            b = self._bin(trade.price)
            vol[b] = vol.get(b, 0) + trade.size
            # Compare enum directly — avoids str() representation surprises
            if trade.taker_side == TakerSide.BUY:
                delta[b] = delta.get(b, 0) + trade.size
            elif trade.taker_side == TakerSide.SELL:
                delta[b] = delta.get(b, 0) - trade.size

        return vol, delta

    def _value_area(
        self,
        dist: dict[Decimal, int],
        sorted_levels: list[Decimal],
        poc: Decimal,
        total_volume: int,
    ) -> tuple[Decimal, Decimal, int]:
        """
        Expand from POC until value_area_pct of total volume is captured.

        Target uses ceil to guarantee true ≥ value_area_pct (vp_v1 policy).
        Tie-break: expand upward when vol_above == vol_below (vp_v1 policy).
        """
        target = math.ceil(total_volume * self.value_area_pct)
        poc_idx = sorted_levels.index(poc)

        lo_idx = poc_idx
        hi_idx = poc_idx
        va_volume = dist[poc]

        while va_volume < target:
            next_hi = hi_idx + 1
            next_lo = lo_idx - 1
            vol_above = dist[sorted_levels[next_hi]] if next_hi < len(sorted_levels) else 0
            vol_below = dist[sorted_levels[next_lo]] if next_lo >= 0 else 0

            if vol_above == 0 and vol_below == 0:
                break

            if vol_above >= vol_below:
                hi_idx = next_hi
                va_volume += vol_above
            else:
                lo_idx = next_lo
                va_volume += vol_below

        return sorted_levels[hi_idx], sorted_levels[lo_idx], va_volume

    def _hvn(
        self,
        dist: dict[Decimal, int],
        sorted_levels: list[Decimal],
        poc: Decimal,
    ) -> list[Decimal]:
        """Local maxima excluding POC, ranked by volume descending."""
        hvns: list[tuple[Decimal, int]] = []
        for i in range(1, len(sorted_levels) - 1):
            level = sorted_levels[i]
            if level == poc:
                continue
            if dist[level] > dist[sorted_levels[i - 1]] and dist[level] > dist[sorted_levels[i + 1]]:
                hvns.append((level, dist[level]))
        hvns.sort(key=lambda x: x[1], reverse=True)
        return [lvl for lvl, _ in hvns]

    def _lvn(
        self,
        dist: dict[Decimal, int],
        sorted_levels: list[Decimal],
    ) -> list[Decimal]:
        """Local minima, ranked by volume ascending (lowest volume first)."""
        lvns: list[tuple[Decimal, int]] = []
        for i in range(1, len(sorted_levels) - 1):
            level = sorted_levels[i]
            if dist[level] < dist[sorted_levels[i - 1]] and dist[level] < dist[sorted_levels[i + 1]]:
                lvns.append((level, dist[level]))
        lvns.sort(key=lambda x: x[1])
        return [lvl for lvl, _ in lvns]
