# Alpha Runtime v2

Production intraday trading runtime built in Python 3.12.

See also: [ARCHITECTURE.md](ARCHITECTURE.md) for the runtime design rules and implementation priorities.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BootstrapEngine (0)                          │
│  config · symbol registry · calendar · clock · engine wiring        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ EventBus (async pub/sub)
          ┌─────────────────┼──────────────────────┐
          │                 │                      │
    Historical         Live Ingestion          Storage
    Data Engine (1)    Engine (3)              Engine (2)
    │                  │                      │
    │  BarEvent         │  BarEvent            │  persists all events
    │  TradeEvent       │  TradeEvent          │  Parquet + PostgreSQL
    │  QuoteEvent       │  QuoteEvent          │
    └──────────┬────────┘                      │
               │                               │
    ═══════════╪═══════ EventBus ══════════════╪═══════════
               │
          Feature Engine (4)
          │  consumes: BarEvent, QuoteEvent
          │  produces: BarSnapshot (stored in-engine)
          │
          Market State Engine (5)
          │  consumes: BarEvent → pulls BarSnapshot
          │  produces: MarketState + MarketStateEvent
          │
          Setup Engine (6)
          │  consumes: BarEvent → pulls BarSnapshot + MarketState
          │  produces: Setup lifecycle events (SetupEvent)
          │
          Scoring Engine (7)
          │  consumes: SetupEvent
          │  produces: scored + graded Setup
          │
          Risk Engine (8)
          │  consumes: SetupEvent (TRIGGERED)
          │  validates: daily limits · position sizing · stop quality
          │  produces: TradePlan → forwarded to OrderEngine
          │
          Order Engine (9)
          │  consumes: TradePlan (direct call from RiskEngine)
          │  submits: OrderIntent → BrokerAdapter
          │  produces: OrderUpdateEvent (on every lifecycle change)
          └──────────────────────────────────────────────────────────
```

---

## Core Architectural Rule

**All data — historical, replay, and live — flows through the same normalized event pipeline.**

Historical data sources and live feed adapters both produce `BarEvent`, `TradeEvent`, and
`QuoteEvent`. Downstream engines never know (or care) whether a bar came from a file or a
WebSocket. The only difference is `event.metadata.is_replay`.

---

## Runtime Modes

| Mode | Description |
|------|-------------|
| `HISTORICAL_BACKFILL` | Fetch and store raw data only. No signal detection. |
| `REPLAY` | Re-run historical events through the full pipeline at configurable speed. |
| `PAPER` | Catch-up from history, then live feed with paper order execution. |
| `LIVE` | Same as PAPER but routes orders to a real broker adapter. |

**Catch-up-then-live** (PAPER / LIVE): On startup, the historical engine loads
`catchup_lookback_days` of recent bars so the feature engine has warm indicators,
then the live feed takes over.

---

## Project Structure

```
alpha-runtime-v2/
├── src/alpha/
│   ├── config/          # pydantic-settings, env-based config
│   ├── models/          # all pydantic domain models + event contracts
│   ├── core/            # BaseEngine, EventBus, SymbolRegistry, Clock
│   ├── calendar/        # SessionCalendar abstraction + NYSE impl
│   ├── engines/
│   │   ├── bootstrap/   # Engine 0 — wiring + lifecycle orchestration
│   │   ├── historical/  # Engine 1 — historical fetch + normalize
│   │   │   └── sources/ # DataSource adapters (polygon, alpaca, csv…)
│   │   ├── storage/     # Engine 2 — Parquet + PostgreSQL persistence
│   │   ├── live/        # Engine 3 — streaming subscriptions
│   │   │   └── adapters/ # LiveFeedAdapter (alpaca, polygon…)
│   │   ├── feature/     # Engine 4 — VWAP, EMA, ATR, ORB, RVOL
│   │   ├── market_state/ # Engine 5 — trend, VWAP regime, ORB state
│   │   ├── setup/       # Engine 6 — setup detection state machine
│   │   ├── scoring/     # Engine 7 — confidence score + SSS/A+/B grade
│   │   ├── risk/        # Engine 8 — sizing, stop calc, daily limits
│   │   └── order/       # Engine 9 — order routing + lifecycle tracking
│   │       └── adapters/ # BrokerAdapter (alpaca, IB, paper…)
│   └── api/             # FastAPI — health + runtime inspection endpoints
├── tests/
│   └── unit/
├── scripts/
│   └── init_db.sql      # TimescaleDB schema
└── docker-compose.yml   # TimescaleDB + app
```

---

## Event Contracts

All inter-engine communication uses immutable Pydantic models:

| Event | Emitted By | Consumed By |
|-------|-----------|-------------|
| `BarEvent` | Historical / Live engines | Feature, Storage |
| `TradeEvent` | Historical / Live engines | Feature (microstructure), Storage |
| `QuoteEvent` | Historical / Live engines | Feature (spread), Storage |
| `OrderBookEvent` | Live engine | Feature (future) |
| `MarketStateEvent` | Market State Engine | Setup, Storage |
| `SetupEvent` | Setup Engine | Scoring, Risk, Storage |
| `OrderUpdateEvent` | Order Engine | Risk (P&L tracking), Storage |
| `SystemEvent` | Bootstrap Engine | All (session open/close, halt) |

---

## Key Abstractions

### `BaseEngine`
```python
await engine.initialize()  # acquire resources
await engine.start()       # begin processing
await engine.stop()        # graceful shutdown
health = await engine.health_check()
```

### `Clock` (replay parity)
```python
# Engine code — same in live and replay:
now = self._clock.now()
await self._clock.sleep_until(next_bar_time)
```

### `HistoricalDataSource` (multi-source)
Implement for each data vendor. Returns `AsyncIterator[BarEvent]` with
fully normalized events — no raw vendor formats leak downstream.

### `LiveFeedAdapter` (multi-source)
Implement for each streaming vendor. Calls registered handlers with
normalized events only.

### `BrokerAdapter` (paper / live parity)
Paper adapter simulates fills against live prices. Live adapter routes
to real broker. The `OrderEngine` never knows which it's using.

---

## Getting Started

```bash
# 1. Clone and install
pip install -e ".[dev]"
cp .env.example .env     # edit with your API keys

# 2. Start the database
make docker-up

# 3. Run migrations
make migrate

# 4. Start the runtime (PAPER mode by default)
alpha run

# or start just the API
alpha api
```

---

## Development

```bash
make lint          # ruff check + format check
make format        # auto-fix formatting
make type-check    # mypy --strict
make test          # pytest + coverage
make test-unit     # fast unit tests only
```

---

## Adding a Data Source

1. Create `src/alpha/engines/historical/sources/my_source.py`
2. Implement `HistoricalDataSource` — only `fetch_bars()` is required
3. Create `src/alpha/engines/live/adapters/my_adapter.py`
4. Implement `LiveFeedAdapter`
5. Register both in `BootstrapEngine._wire_engines()`

No other engine needs to change — the normalized event contracts handle everything.

---

## Adding a Setup Type

1. Add entry to `SetupType` enum in `models/enums.py`
2. Add detector method in `SetupEngine._scan_for_setups()`
3. Add scoring conditions in `ScoringEngine._score()`
4. Update `RiskEngine._infer_side()` if needed

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.12 |
| Async | asyncio |
| Models | Pydantic v2 |
| API | FastAPI + uvicorn |
| Time-series DB | PostgreSQL 16 + TimescaleDB |
| Columnar storage | Parquet (pyarrow) |
| Infra | Docker Compose |
| Testing | pytest + pytest-asyncio |
| Linting | ruff |
| Types | mypy (strict) |
