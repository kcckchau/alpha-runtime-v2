from enum import auto

from enum import StrEnum


class RuntimeMode(StrEnum):
    HISTORICAL_BACKFILL = "historical_backfill"
    REPLAY = "replay"
    PAPER = "paper"
    LIVE = "live"


class EngineState(StrEnum):
    CREATED = "created"
    INITIALIZING = "initializing"
    READY = "ready"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class DataSourceId(StrEnum):
    ALPACA = "alpaca"
    POLYGON = "polygon"
    INTERACTIVE_BROKERS = "interactive_brokers"
    TASTYTRADE = "tastytrade"
    CSV = "csv"
    JSON_FILE = "json_file"
    PARQUET = "parquet"
    SYNTHETIC = "synthetic"
    UNKNOWN = "unknown"


class AssetClass(StrEnum):
    EQUITY = "equity"
    ETF = "etf"
    OPTION = "option"
    FUTURE = "future"
    CRYPTO = "crypto"


class BarTimeframe(StrEnum):
    S1 = "1s"
    S5 = "5s"
    S10 = "10s"
    S15 = "15s"
    S30 = "30s"
    M1 = "1m"
    M2 = "2m"
    M3 = "3m"
    M5 = "5m"
    M10 = "10m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    D1 = "1d"


class EventType(StrEnum):
    BAR = "bar"
    TRADE = "trade"
    QUOTE = "quote"
    ORDER_BOOK = "order_book"
    MARKET_STATE = "market_state"
    SETUP = "setup"
    ORDER_UPDATE = "order_update"
    EXECUTION = "execution"
    SYSTEM = "system"


class SessionPhase(StrEnum):
    PRE_MARKET = "pre_market"
    OPENING_RANGE = "opening_range"   # first N minutes after open
    EARLY = "early"                   # 9:45–10:30
    MID = "mid"                       # 10:30–14:00
    POWER_HOUR = "power_hour"         # 14:00–15:30
    CLOSING = "closing"               # last 30 min
    AFTER_HOURS = "after_hours"
    CLOSED = "closed"


class TrendState(StrEnum):
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    CHOPPY = "choppy"
    TRANSITIONING = "transitioning"
    UNKNOWN = "unknown"


class DayType(StrEnum):
    TREND_UP = "trend_up"       # ORB breakout up + trending up — run longs, fade shorts
    TREND_DOWN = "trend_down"   # ORB breakdown + trending down — run shorts, fade longs
    RANGE = "range"             # Inside ORB or failed breakout — take profits quickly
    BALANCED = "balanced"       # ORB broke but trend not aligned — neutral targets
    UNKNOWN = "unknown"         # ORB not yet established


class DayTypeStatus(StrEnum):
    FORMING = "forming"              # not enough evidence yet (< 30 bars or no ORB)
    LOCKED_HEALTHY = "locked_healthy"  # locked and current price action confirms it
    STRESSED = "stressed"            # locked but contradictory signals appearing
    INVALIDATED = "invalidated"      # locked but completely contradicted by price action


class LiveBias(StrEnum):
    BULLISH = "bullish"                        # trending up + above VWAP
    BEARISH = "bearish"                        # trending down + below VWAP
    TRANSITIONING_BULLISH = "transitioning_bullish"  # trending up but below VWAP
    TRANSITIONING_BEARISH = "transitioning_bearish"  # trending down but above VWAP
    NEUTRAL = "neutral"                        # choppy
    UNKNOWN = "unknown"                        # no EMA data yet


class VWAPState(StrEnum):
    ABOVE = "above"
    BELOW = "below"
    RECLAIMING = "reclaiming"         # price crossed from below, testing from above
    REJECTING = "rejecting"           # price crossed from above, failing


class ORBState(StrEnum):
    INSIDE = "inside"
    BREAKOUT_UP = "breakout_up"
    BREAKOUT_DOWN = "breakout_down"
    FAILED_BREAKOUT_UP = "failed_breakout_up"
    FAILED_BREAKOUT_DOWN = "failed_breakout_down"
    NOT_SET = "not_set"               # opening range not yet established


class SetupType(StrEnum):
    VWAP_RECLAIM = "vwap_reclaim"
    VWAP_REJECTION = "vwap_rejection"
    VWAP_UNDERCUT_RECLAIM = "vwap_undercut_reclaim"   # shallow VWAP dip, quick reclaim (Grade A)
    ORB_BREAKOUT = "orb_breakout"
    ORB_BREAKDOWN = "orb_breakdown"
    SWEEP_RECLAIM = "sweep_reclaim"                   # OR low / structural level sweep + reclaim (Grade A)
    FAKE_BREAKDOWN = "fake_breakdown"                 # structural sweep + VWAP reclaim, strict (SSS)
    DEEP_EXHAUSTION_RECLAIM = "deep_exhaustion_reclaim"  # capitulation candle + no new low (Grade A-)
    HOD_BREAKOUT = "hod_breakout"
    TREND_PULLBACK = "trend_pullback"
    TREND_PULLBACK_SHORT = "trend_pullback_short"
    VWAP_FAILED_RECLAIM_SHORT = "vwap_failed_reclaim_short"
    RELATIVE_STRENGTH_BREAKOUT = "relative_strength_breakout"


class SetupState(StrEnum):
    FORMING = "forming"
    CONFIRMED = "confirmed"
    TRIGGERED = "triggered"
    FAILED = "failed"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"


class SetupGrade(StrEnum):
    SSS = "SSS"
    A_PLUS = "A+"
    A = "A"
    B = "B"
    C = "C"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class TimeInForce(StrEnum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    OPG = "opg"    # at open
    CLS = "cls"    # at close


class OrderStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    ACCEPTED = "accepted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TakerSide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class HealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class AccountType(StrEnum):
    DAY = "day"
    SWING = "swing"


class KillSwitchReason(StrEnum):
    DAILY_LOSS_LIMIT = "daily_loss_limit"
    PROFIT_PROTECTION = "profit_protection"
    MANUAL = "manual"
