from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class LevelDistanceConfig:
    """
    ATR-fraction thresholds for episode boundary detection.
    All thresholds are computed per-bar using the current ATR value.
    Fallback tick values are used when ATR is not yet warmed.
    """
    proximity_atr_fraction: float = 0.20   # same as existing proximity_band in level_observer
    separation_atr_fraction: float = 0.50
    min_separation_bars: int = 3           # must hold separation distance for this many bars
    max_episode_bars: int = 60
    max_bar_gap_seconds: int = 90          # 1m30s — gap detection threshold
    fallback_proximity_ticks: int = 10     # when ATR unavailable
    fallback_separation_ticks: int = 25

    def proximity_ticks(self, atr_14: Decimal | None, tick_size: Decimal) -> int:
        if atr_14 is None or atr_14 == 0:
            return self.fallback_proximity_ticks
        raw = float(atr_14) * self.proximity_atr_fraction / float(tick_size)
        return max(1, int(raw))

    def separation_ticks(self, atr_14: Decimal | None, tick_size: Decimal) -> int:
        if atr_14 is None or atr_14 == 0:
            return self.fallback_separation_ticks
        raw = float(atr_14) * self.separation_atr_fraction / float(tick_size)
        return max(1, int(raw))

    def config_hash(self) -> str:
        d = {
            "proximity_atr_fraction": self.proximity_atr_fraction,
            "separation_atr_fraction": self.separation_atr_fraction,
            "min_separation_bars": self.min_separation_bars,
            "max_episode_bars": self.max_episode_bars,
            "fallback_proximity_ticks": self.fallback_proximity_ticks,
            "fallback_separation_ticks": self.fallback_separation_ticks,
        }
        return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()[:12]
