from datetime import date
from decimal import Decimal

from pydantic import BaseModel


class VolumeProfile(BaseModel):
    """
    Sealed volume profile for one session (RTH or Globex).

    Computed offline from sealed bars — never updated after creation.
    Loaded at session open as static reference levels.

    bin_size: price per bin (1.0 point for MNQ = 4 ticks).
    distribution: sorted dict of {bin_lower_bound_str: volume}.
    """

    model_config = {"frozen": True}

    symbol: str
    session_date: date
    session_type: str          # "rth" | "globex"
    bin_size: float

    poc: Decimal               # price level with highest volume
    vah: Decimal               # value area high (70% of session volume)
    val: Decimal               # value area low
    total_volume: int
    value_area_volume: int     # volume within [val, vah]

    hvn_levels: list[Decimal]  # high volume nodes, ranked by volume desc, POC excluded
    lvn_levels: list[Decimal]  # low volume nodes, ranked by volume asc

    # Full distribution for research. Key = bin lower bound as string.
    # Sorted ascending by price for readability.
    distribution: dict[str, int]
