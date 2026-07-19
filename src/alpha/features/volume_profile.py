"""
VolumeProfileBuilder — pure function, no side effects.

Computes POC / VAH / VAL / HVN / LVN from a list of sealed bars.

Volume distribution method:
    For each bar, volume is distributed uniformly across all price bins
    spanned by [low, high]. This is the standard approach when tick-level
    data is not available.

    vol_per_bin = bar.volume / number_of_bins_in_bar_range

Value area rule: standard 70% starting from POC, expanding up or down
one bin at a time toward the side with more volume.

HVN: local maximum in the volume distribution (adjacent bins both lower).
LVN: local minimum in the volume distribution (adjacent bins both higher).

Bin size: 1.0 MNQ point (4 ticks) by default. Configurable.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_DOWN

from alpha.models.bar import Bar
from alpha.models.volume_profile import VolumeProfile

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
        sorted_levels = sorted(dist.keys())
        total_volume = sum(dist.values())

        poc = max(dist, key=lambda k: dist[k])
        vah, val, va_volume = self._value_area(dist, sorted_levels, poc, total_volume)
        hvns = self._hvn(dist, sorted_levels, poc)[: self.max_hvn]
        lvns = self._lvn(dist, sorted_levels)[: self.max_lvn]

        return VolumeProfile(
            symbol=symbol,
            session_date=session_date,
            session_type=session_type,
            bin_size=float(self.bin_size),
            poc=poc,
            vah=vah,
            val=val,
            total_volume=total_volume,
            value_area_volume=va_volume,
            hvn_levels=hvns,
            lvn_levels=lvns,
            distribution={str(k): v for k, v in zip(sorted_levels, [dist[l] for l in sorted_levels])},
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _bin(self, price: Decimal) -> Decimal:
        """Round price down to nearest bin boundary."""
        return (price / self.bin_size).to_integral_value(rounding=ROUND_DOWN) * self.bin_size

    def _build_distribution(self, bars: list[Bar]) -> dict[Decimal, int]:
        dist: dict[Decimal, float] = {}

        for bar in bars:
            low_bin = self._bin(bar.low)
            high_bin = self._bin(bar.high)

            # Collect all bins this bar spans
            bins: list[Decimal] = []
            current = low_bin
            while current <= high_bin:
                bins.append(current)
                current += self.bin_size

            if not bins:
                continue

            vol_per_bin = bar.volume / len(bins)
            for b in bins:
                dist[b] = dist.get(b, 0.0) + vol_per_bin

        return {k: max(1, int(round(v))) for k, v in dist.items()}

    def _value_area(
        self,
        dist: dict[Decimal, int],
        sorted_levels: list[Decimal],
        poc: Decimal,
        total_volume: int,
    ) -> tuple[Decimal, Decimal, int]:
        """Expand from POC until value_area_pct of total volume is captured."""
        target = int(total_volume * self.value_area_pct)
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
