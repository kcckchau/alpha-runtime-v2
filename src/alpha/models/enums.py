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
    DATABENTO = "databento"
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
    BAR_BUNDLE = "bar_bundle"           # BAR + sealed BarFlowContext from BarFlowAggregator
    PIPELINE_OUTPUT = "pipeline_output" # single authoritative output per M1 bar
    TRADE = "trade"
    QUOTE = "quote"
    ORDER_BOOK = "order_book"
    MARKET_STATE = "market_state"
    SETUP = "setup"
    THESIS = "thesis"
    ORDER_UPDATE = "order_update"
    EXECUTION = "execution"
    SYSTEM = "system"
    POSITION_SIGNAL = "position_signal"


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


class SetupFamily(StrEnum):
    """High-level setup family — the tradeable structure, independent of anchor or direction."""
    FAILED_AUCTION_REVERSAL = "failed_auction_reversal"  # sweep/wick failed to sustain; price recovers
    FAILED_RECLAIM          = "failed_reclaim"           # attempted level reclaim that was rejected back
    PULLBACK_CONTINUATION   = "pullback_continuation"    # with-trend pullback to a level, holds
    BREAKOUT_CONTINUATION   = "breakout_continuation"    # level breakout accepted on the other side
    RANGE_REVERSAL          = "range_reversal"           # reversal at a range boundary (double bottom/top)
    EXHAUSTION_REVERSAL     = "exhaustion_reversal"      # capitulation candle + recovery


class AnchorLevel(StrEnum):
    """The reference level / coordinate the setup is primarily organized around."""
    VWAP       = "vwap"
    ONL        = "onl"
    ONH        = "onh"
    PDL        = "pdl"
    PDH        = "pdh"
    ORL        = "orl"        # opening range low
    ORH        = "orh"        # opening range high
    EMA9       = "ema9"
    EMA21      = "ema21"
    HOD        = "hod"        # high of day
    LOD        = "lod"        # low of day
    STRUCTURAL = "structural" # generic structural level (no single named coordinate)


class LevelInteraction(StrEnum):
    """How price interacted with the anchor level — the primitive catalyst."""
    HOLD           = "hold"            # price tested level and held (bounced)
    REJECT         = "reject"          # approached level, was turned back
    RECLAIM        = "reclaim"         # lost level then closed back above/below it
    SWEEP          = "sweep"           # brief wick penetration, body recovered
    ACCEPT         = "accept"          # crossed level and is now accepting on the other side
    FAILED_RECLAIM = "failed_reclaim"  # attempted reclaim that was rejected back


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
    ONL_SWEEP_RECLAIM_LONG = "onl_sweep_reclaim_long"         # ONL sweep + reclaim (key-level failed breakdown)
    DOUBLE_BOTTOM_RECLAIM_LONG = "double_bottom_reclaim_long"  # two lows near same zone, second fails to extend


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
    # Pre-submission
    PENDING         = "pending"
    SUBMITTED       = "submitted"
    ACCEPTED        = "accepted"
    # Execution
    PARTIAL         = "partial"
    FILLED          = "filled"
    # Cancellation
    CANCEL_PENDING  = "cancel_pending"   # cancel requested, awaiting broker ack
    CANCELLED       = "cancelled"
    # Replacement
    REPLACE_PENDING = "replace_pending"  # modify requested, awaiting broker ack
    REPLACED        = "replaced"         # superseded by a new order (child of replace)
    # Terminal failures
    REJECTED        = "rejected"         # rejected before acknowledgement (local validation)
    BROKER_REJECTED = "broker_rejected"  # broker refused after acknowledgement
    EXPIRED         = "expired"
    # Reconciliation sentinel — never leave this state without querying the broker
    UNKNOWN         = "unknown"


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
    DAILY_LOSS_LIMIT          = "daily_loss_limit"
    PROFIT_PROTECTION         = "profit_protection"
    PROTECTIVE_STOP_REJECTED  = "protective_stop_rejected"  # stop child rejected after entry filled
    UNMANAGED_POSITION        = "unmanaged_position"        # position detected with no known stop
    RECONCILIATION_FAILURE    = "reconciliation_failure"    # could not confirm broker state
    MANUAL                    = "manual"


class ThesisState(StrEnum):
    WATCHING = "watching"
    BUILDING = "building"
    READY = "ready"
    ORDER_WORKING = "order_working"
    TRIGGERED = "triggered"
    INVALIDATED = "invalidated"
    FLIPPED = "flipped"
    EXPIRED = "expired"


class ThesisType(StrEnum):
    FAKE_BREAKDOWN_RECLAIM_LONG = "fake_breakdown_reclaim_long"
    VWAP_FAILED_RECLAIM_SHORT = "vwap_failed_reclaim_short"


class VWAPEpisodeOutcome(StrEnum):
    SWEPT = "swept"          # low < VWAP but close >= VWAP (approached from above)
    RECLAIMED = "reclaimed"  # crossed from below to above
    BROKEN = "broken"        # crossed from above to below
    REJECTED = "rejected"    # cross_up quickly followed by cross_down (failed reclaim)


class DataQualityState(StrEnum):
    CLEAN      = "clean"       # feed healthy, signals allowed
    DEGRADED   = "degraded"    # bar lag >250ms, silence >30s, or missed bar — signals blocked
    RECOVERING = "recovering"  # first clean bar seen after DEGRADED, waiting for confirmation
    FAILED     = "failed"      # silence >5min during RTH — signals blocked, needs investigation


# ── Execution subsystem ───────────────────────────────────────────────────────

class TradeAction(StrEnum):
    """Semantically unambiguous user intent — not just BUY/SELL."""
    OPEN_LONG     = "open_long"
    OPEN_SHORT    = "open_short"
    REDUCE_LONG   = "reduce_long"    # close part of long
    REDUCE_SHORT  = "reduce_short"   # close part of short
    FLATTEN       = "flatten"        # close entire position
    REVERSE_LONG  = "reverse_long"   # flip from short to long
    REVERSE_SHORT = "reverse_short"  # flip from long to short


class PriceMode(StrEnum):
    """Broker-agnostic execution policy — resolved to a concrete price by the backend."""
    JOIN_BID        = "join_bid"       # post at current bid (passive buy)
    JOIN_ASK        = "join_ask"       # post at current ask (passive sell)
    CROSS_ONE_TICK  = "cross_one_tick" # one tick through the market (buy at ask+1, sell at bid-1)
    MARKETABLE_LIMIT = "marketable_limit"  # limit at or through best opposite (aggressive, capped)
    MIDPOINT        = "midpoint"       # (bid+ask)/2
    PASSIVE_ONE_TICK = "passive_one_tick"  # one tick inside best (buy at bid+1, sell at ask-1)


class IntentStatus(StrEnum):
    """Full lifecycle of a TradeIntent through the execution subsystem."""
    RECEIVED              = "received"
    VALIDATING            = "validating"
    AWAITING_CONFIRMATION = "awaiting_confirmation"  # amber: warned, requires second keypress
    CONFIRMED             = "confirmed"
    PLANNING              = "planning"               # risk engine compiling TradePlan
    PLAN_READY            = "plan_ready"
    SUBMITTING            = "submitting"             # order API call in flight
    ACTIVE                = "active"                 # at least one child order acknowledged
    COMPLETED             = "completed"              # all orders terminal, position reconciled
    CANCELLED             = "cancelled"              # user cancelled or timeout
    EXPIRED               = "expired"                # confirmation window timed out
    REJECTED              = "rejected"               # hard risk or validation failure
    FAILED                = "failed"                 # unrecoverable: e.g. stop rejected after fill


class RiskDecision(StrEnum):
    APPROVED                  = "approved"
    APPROVED_WITH_MODIFICATIONS = "approved_with_modifications"  # requires confirmation
    REJECTED                  = "rejected"


class OrderRole(StrEnum):
    """Role of a PlannedOrder within a TradePlan bracket."""
    ENTRY  = "entry"
    STOP   = "stop"
    TARGET = "target"


class IntentSource(StrEnum):
    HOTKEY   = "hotkey"    # keyboard shortcut on chart
    API      = "api"       # external REST call
    STRATEGY = "strategy"  # runtime strategy (future)
    MANUAL   = "manual"    # direct override with explicit flag
