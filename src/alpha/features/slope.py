"""
Centralized EMA slope classification.

Policy version tracks the normalization formula and thresholds so historical
research records remain reproducible when thresholds are recalibrated.

Thresholds are UNCALIBRATED — placeholders pending replay distribution analysis.
Do not promote to hard setup gates before research validation.
"""

SLOPE_POLICY_VERSION = "norm3_v1"

# 1H ribbon policy version — bump when ribbon geometry or rolling-history
# definitions change so research outputs remain reproducible.
EMA_1H_RIBBON_POLICY_VERSION = "ema_1h_ribbon_v1"
EMA_1H_SLOPE_POLICY_VERSION = "ema_1h_norm3_v1"

# UNCALIBRATED dead-zone thresholds (ATR/bar).
# Values below this magnitude are treated as flat / noise.
# Pending calibration from replay slope distributions by timeframe and session phase.
SLOPE_FLAT_THRESHOLD_1M: float = 0.05   # ATR/bar — 1m EMA slopes
SLOPE_FLAT_THRESHOLD_5M: float = 0.03   # ATR/bar — 5m EMA slopes
SLOPE_FLAT_THRESHOLD_1H: float = 0.02   # ATR/bar — 1h EMA slopes; UNCALIBRATED

# UNCALIBRATED ribbon width percentile bounds for compressed / expanded classification.
# Adjust after reviewing ribbon width distributions across session types.
RIBBON_COMPRESSED_PERCENTILE: float = 20.0   # width <= p20 → compressed
RIBBON_EXPANDED_PERCENTILE: float = 80.0     # width >= p80 → expanded


def classify_slope(value: float | None, flat_threshold: float) -> str | None:
    """Return 'up', 'flat', or 'down' for a normalized slope value.

    Args:
        value: Normalized slope in ATR/bar units. None → None.
        flat_threshold: Magnitude below which the slope is considered flat.
            Must be positive. Use SLOPE_FLAT_THRESHOLD_1M or _5M.

    Returns:
        'up' if value > flat_threshold
        'down' if value < -flat_threshold
        'flat' if |value| <= flat_threshold
        None if value is None
    """
    if value is None:
        return None
    if value > flat_threshold:
        return "up"
    if value < -flat_threshold:
        return "down"
    return "flat"
