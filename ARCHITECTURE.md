# Alpha Runtime v2 Architecture

This document captures the architectural rules for `alpha-runtime-v2`.

The goal is to keep the runtime:

- source-agnostic
- replay/live consistent
- operationally simple
- easy to extend without coupling engines together

## Core Principles

### 1. Engines Do Not Own Storage

Engines detect, transform, classify, score, and route.
They do not write directly to PostgreSQL, Parquet, or any other storage backend.

Preferred pattern:

1. an engine emits a normalized event
2. `StorageEngine` subscribes to that event
3. `StorageEngine` persists it

Examples:

- `BarEvent`
- `QuoteEvent`
- `TradeEvent`
- `MarketStateEvent`
- `SetupEvent`
- `OrderUpdateEvent`

Why this matters:

- avoids direct engine-to-storage coupling
- keeps engines testable
- lets storage evolve independently
- makes replay and backfill behavior consistent

Anti-patterns to avoid:

- `FeatureEngine` calling Postgres directly
- `SetupEngine` writing Parquet directly
- `OrderEngine` deciding how persistence is modeled

### 2. Normalized Event Contracts Are Strict

Adapters normalize first.
Everything downstream uses normalized contracts only.

Good:

- `BarEvent`
- `QuoteEvent`
- `TradeEvent`

Bad inside engines:

- `IBKRTrade`
- `DatabentoQuote`
- `CSVBar`
- `ReplayTrade`

Why this matters:

- every downstream engine stays source-agnostic
- replay/live parity remains real
- new sources only affect adapters
- debugging stays tractable

Rule:

The only place vendor-specific payloads should exist is inside source/feed/broker adapters and their normalization layer.

### 3. Avoid a God Snapshot

Snapshots are useful, but they should not become giant bags of unrelated fields.

Preferred direction:

- `IndicatorSnapshot`
- `MicrostructureSnapshot`
- `SessionContext`
- `RelativeStrengthSnapshot`

Then compose those into a higher-level `BarSnapshot` only when needed.

Why this matters:

- smaller model surfaces
- easier testing
- clearer ownership of calculations
- easier evolution as more features are added

Current guidance:

The existing `BarSnapshot` is acceptable as an early integration point, but it should not keep expanding indefinitely.

### 4. Setup Engine Detects, It Does Not Trade

Responsibilities must stay separate:

- `SetupEngine`: detects opportunities
- `ScoringEngine`: evaluates quality
- `RiskEngine`: determines allowed exposure
- `OrderEngine`: executes and tracks order lifecycle

Why this matters:

- avoids v1-style responsibility collapse
- preserves auditability
- supports swapping scoring/risk logic without rewriting setup detection

Anti-pattern:

`SetupEngine` deciding entries, sizing, exposure, and execution routing.

### 5. Bootstrap Is the Runtime Heart

Bootstrap is not just startup glue.
It is the runtime recovery and orchestration layer.

Target startup flow:

1. load config
2. load symbols and calendars
3. initialize storage
4. recover prior runtime/session state
5. backfill missing data
6. rebuild features
7. rebuild market state
8. replay setup detection
9. start live stream
10. switch runtime to live operation

Why this matters:

- real trading runtimes recover state
- indicators should not start cold
- live transition should happen only after context is rebuilt

### 6. Replay/Live Parity Is a Core Advantage

Historical, replay, paper, and live modes should share the same normalized pipeline whenever possible.

Good:

- historical source emits `BarEvent`
- live feed emits `BarEvent`
- downstream engines do not care which one produced it

Bad:

- separate backtest logic and live logic that drift apart

Why this matters:

- strategy behavior is easier to trust
- debugging becomes much simpler
- replay becomes a real operational tool instead of a toy

### 7. Stay Single-Process Until Scale Forces Otherwise

Do not introduce distributed complexity early.

Not needed yet:

- Kafka
- microservices
- Kubernetes
- distributed queues

Preferred current architecture:

- single-process
- asyncio
- bounded queues
- clean engine boundaries
- strong contracts

Why this matters:

- much lower complexity
- easier local debugging
- faster iteration on strategy/runtime behavior

## Desired Engine Boundaries

### Bootstrap Engine

Owns:

- config loading
- registry population
- calendar selection
- engine wiring
- runtime mode orchestration
- recovery/backfill/live handoff

### Historical Data Engine

Owns:

- fetch
- normalize
- validate
- gap detection
- replay/backfill event emission

Should not own:

- persistence policy
- live trading decisions

### Storage Engine

Owns:

- event persistence
- storage backends
- typed read/write access

Should subscribe to normalized events rather than being called directly by business engines.

### Live Ingestion Engine

Owns:

- multi-symbol live subscriptions
- normalized live bars/trades/quotes/book events

Should not leak vendor payloads.

### Feature Engine

Owns:

- indicators
- session metrics
- microstructure state
- derived snapshots

### Market State Engine

Owns:

- trend/chop classification
- VWAP/ORB regime labeling
- higher-level market structure state

### Setup Engine

Owns:

- candidate detection
- setup lifecycle state machine

Should not own scoring, risk, or execution.

### Scoring Engine

Owns:

- quality assessment
- confidence
- reasons met/missing

### Risk Engine

Owns:

- sizing
- stop/target validation
- portfolio and daily-loss constraints

### Order Engine

Owns:

- broker-facing order intent
- order lifecycle tracking
- execution status normalization

## Current Gaps

The current codebase is directionally aligned with the architecture, but several important pieces are still incomplete.

Highest-signal gaps:

1. `StorageEngine` persistence is still placeholder logic.
2. `BootstrapEngine` catch-up, replay, and recovery orchestration are still partial.
3. `SetupEngine` detectors are mostly scaffolding.
4. `ScoringEngine` does not yet write enriched setup state back into the pipeline.
5. `RiskEngine` daily P&L feedback loop is incomplete.
6. The current snapshot model should eventually be decomposed before it grows much further.
7. Futures-aware session/calendar behavior should be added before treating futures as first-class strategy instruments.

## Recommended Next Steps

If we want the runtime to become operational fastest, the recommended order is:

1. Make `StorageEngine` real.
2. Implement bootstrap catch-up and context rebuild.
3. Add history endpoints for charts and runtime inspection.
4. Make session handling futures-aware where needed.
5. Implement one real setup path end-to-end.
6. Complete scoring feedback into emitted setup state.
7. Complete risk P&L and halt-state updates from order/execution events.
8. Add websocket/event-stream endpoints only after the event semantics are stable.

## Practical Review Checklist

When adding new code, ask:

1. Does this leak vendor-specific data past the adapter layer?
2. Does this make an engine depend directly on storage?
3. Does this push trading decisions into `SetupEngine`?
4. Does this make replay behave differently from live?
5. Does this add distributed complexity without current need?
6. Does this bloat a shared snapshot instead of creating a focused sub-model?

If the answer to any of those is yes, stop and redesign before merging the change.
