-- Alpha Runtime v2 — database initialization
-- Run once by docker-entrypoint-initdb.d on first container start.
-- Subsequent schema changes use Alembic migrations.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Bars ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bars (
    symbol      TEXT        NOT NULL,
    timeframe   TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    open        NUMERIC     NOT NULL,
    high        NUMERIC     NOT NULL,
    low         NUMERIC     NOT NULL,
    close       NUMERIC     NOT NULL,
    volume      BIGINT      NOT NULL,
    vwap        NUMERIC,
    trade_count INTEGER,
    source      TEXT        NOT NULL DEFAULT 'unknown',
    PRIMARY KEY (symbol, timeframe, ts)
);

SELECT create_hypertable('bars', 'ts', if_not_exists => TRUE);

-- ── Trades ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trades (
    symbol      TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    price       NUMERIC     NOT NULL,
    size        INTEGER     NOT NULL,
    exchange    TEXT,
    taker_side  TEXT,
    trade_id    TEXT,
    source      TEXT        NOT NULL DEFAULT 'unknown'
);

SELECT create_hypertable('trades', 'ts', if_not_exists => TRUE);

-- ── Market states ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_states (
    symbol          TEXT        NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    trend           TEXT        NOT NULL,
    trend_strength  FLOAT       NOT NULL,
    vwap_state      TEXT        NOT NULL,
    orb_state       TEXT        NOT NULL,
    session_phase   TEXT        NOT NULL,
    confidence      FLOAT       NOT NULL,
    data            JSONB,
    PRIMARY KEY (symbol, ts)
);

SELECT create_hypertable('market_states', 'ts', if_not_exists => TRUE);

-- ── Setups ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS setups (
    setup_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol          TEXT        NOT NULL,
    setup_type      TEXT        NOT NULL,
    state           TEXT        NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    triggered_at    TIMESTAMPTZ,
    invalidated_at  TIMESTAMPTZ,
    score           FLOAT,
    grade           TEXT,
    data            JSONB
);

CREATE INDEX IF NOT EXISTS setups_symbol_state ON setups (symbol, state, detected_at DESC);

-- ── Trade plans ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_plans (
    plan_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    setup_id        UUID        NOT NULL REFERENCES setups(setup_id),
    symbol          TEXT        NOT NULL,
    side            TEXT        NOT NULL,
    entry_price     NUMERIC     NOT NULL,
    stop_price      NUMERIC     NOT NULL,
    target_price    NUMERIC     NOT NULL,
    position_size   INTEGER     NOT NULL,
    risk_amount     NUMERIC     NOT NULL,
    reward_amount   NUMERIC     NOT NULL,
    rr_ratio        FLOAT       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Orders ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    broker_order_id TEXT,
    intent_id       UUID        NOT NULL,
    symbol          TEXT        NOT NULL,
    side            TEXT        NOT NULL,
    order_type      TEXT        NOT NULL,
    quantity        INTEGER     NOT NULL,
    filled_quantity INTEGER     NOT NULL DEFAULT 0,
    avg_fill_price  NUMERIC,
    status          TEXT        NOT NULL,
    submitted_at    TIMESTAMPTZ,
    filled_at       TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,
    reject_reason   TEXT,
    commission      NUMERIC     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Executions ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS executions (
    execution_id        UUID        NOT NULL DEFAULT gen_random_uuid(),
    order_id            UUID        NOT NULL REFERENCES orders(order_id),
    broker_execution_id TEXT,
    symbol              TEXT        NOT NULL,
    side                TEXT        NOT NULL,
    quantity            INTEGER     NOT NULL,
    price               NUMERIC     NOT NULL,
    commission          NUMERIC     NOT NULL DEFAULT 0,
    ts                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (execution_id, ts)
);

SELECT create_hypertable('executions', 'ts', if_not_exists => TRUE);
