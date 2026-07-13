# Data Ingestion — Target Spec (Professional Standard)

Status: **proposed / target state**. This is the bar the ingestion path (`docs/ingestion_spec.md`)
should be measured against. Items already implemented are marked ✅; everything else is a gap
identified in the 2026-07 ingestion audit.

Scope: everything between "a vendor sends a byte" and "a normalized event is durably stored and
visible to an operator" — i.e. adapters → EventBus → StorageEngine → health/metrics surface.
Does not cover strategy/signal quality (setups, scoring) — only the data plane underneath it.

---

## 1. Service Level Objectives (SLOs)

Define ingestion health in terms an operator can act on, not just "is the process running."

| Dimension | SLO | Current state |
|---|---|---|
| **Freshness** | Completed M1 bar visible to downstream engines within 250ms of expected close, p99 < 1s | ✅ tracked (`IngestionMonitor` bar-lag check), not exported as a metric |
| **Completeness** | 0 unexplained bar gaps per symbol per session; every gap has a `GapRecord` or is explained by a documented session boundary | ✅ gap detection exists (historical + live); ❌ not aggregated into a per-day completeness score |
| **Correctness** | 0 duplicate rows per logical record (trade_id / setup_id / bar timestamp) in Parquet after any restart or backfill | ⚠️ partial — trades/setups dedup by key; quotes and bars use a different mechanism (`is_replay` suppression), not a uniform guarantee |
| **Availability** | Live feed reconnects within 60s of a transient outage; escalates to on-call if down > 5 min during RTH | ✅ reconnect exists; ❌ no cap on retry attempts, no external alert on sustained failure |
| **Durability** | No received-and-acked market data event is lost on process crash | ❌ not met — `EventBus` is in-memory only; events between adapter receipt and Parquet flush are unrecoverable on crash |
| **Backpressure safety** | A slow downstream subscriber never blocks live market data ingestion (adapter thread must never stall) | ✅ Databento record loop runs in its own thread and only blocks on bounded per-subscriber queues, not on adapter I/O |

---

## 2. Metrics — what a professional ingestion path exports

None of these currently exist as a scraped metrics surface (Prometheus, StatsD, OTel, or
equivalent) — today they exist only as Python attributes read into a JSON snapshot on request.
Target: emit all of the below as real metrics with cardinality by `symbol` and `source` where
noted, on a standard `/metrics` endpoint or push gateway.

### 2.1 Ingress (adapter boundary)

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `ingest_records_received_total` | counter | `source`, `schema`, `symbol` | raw record volume per vendor feed |
| `ingest_record_latency_ms` | histogram | `source`, `schema` | vendor `ts_event` → local receipt time (feed latency) — `IngressObserver` already computes this per-record; needs to be exported, not just logged |
| `ingest_inter_record_gap_ms` | histogram | `source`, `symbol` | time between consecutive records — detects vendor stalls before silence thresholds trip |
| `ingest_out_of_order_total` | counter | `source`, `symbol` | records with `ts_event` older than the last seen — currently logged by `IngressObserver`, not counted |
| `ingest_skipped_records_total` | counter | `source` | Databento `SKIPPED_RECORDS_AFTER_SLOW_READING` and equivalent vendor-side drops |
| `ingest_reconnects_total` | counter | `source` | count of adapter reconnect attempts (success + failure) |
| `ingest_reconnect_duration_seconds` | histogram | `source` | time spent disconnected before successful reconnect |
| `ingest_connection_state` | gauge (0/1) | `source` | 1 = connected, exported continuously, not just logged on transition |
| `ingest_outlier_trades_rejected_total` | counter | `symbol` | trades dropped by `_is_outlier_trade` — currently silent (`logger.debug` only); should be visible so a mis-calibrated filter band is detectable |
| `ingest_nonstandard_match_type_total` | counter | `symbol`, `match_type` | Databento block/EFRP/implied prints filtered out |

### 2.2 EventBus (fan-out layer)

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `eventbus_queue_depth` | gauge | `event_type`, `symbol` | live queue occupancy — `EventBus.queue_depths()` exists, needs continuous export |
| `eventbus_queue_full_total` | counter | `event_type`, `symbol` | how often a subscriber's queue hit capacity |
| `eventbus_dropped_events_total` | counter | `event_type`, `symbol` | events evicted under `drop_if_full=True` — data loss that is currently invisible unless someone reads `status.json` |
| `eventbus_publish_latency_ms` | histogram | `event_type` | time `publish()` blocks under backpressure (should be ~0; a nonzero p99 indicates a slow consumer) |
| `eventbus_handler_errors_total` | counter | `event_type` | exceptions caught in `_drain()` — currently only `logger.exception`, no counter |

### 2.3 Data quality (`IngestionMonitor`)

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `data_quality_state` | gauge (enum as int) | `symbol` | CLEAN=0 / RECOVERING=1 / DEGRADED=2 / FAILED=3 — direct Grafana panel |
| `data_quality_transitions_total` | counter | `symbol`, `from_state`, `to_state` | state machine churn — frequent CLEAN↔DEGRADED flapping is itself a signal |
| `bar_lag_ms` | histogram | `symbol` | already computed in `on_bar_received`, not exported |
| `bar_gap_seconds` | histogram | `symbol` | already computed, not exported |
| `silence_duration_seconds` | gauge | `symbol` | current silence duration, sampled every `check_health()` tick |
| `signals_blocked_total` | counter | `symbol`, `reason` | count of `is_signal_allowed() == False` checks that actually suppressed a would-be entry — ties data quality directly to trading impact |

### 2.4 Storage

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `storage_write_queue_depth` | gauge | — | `StorageEngine._write_queue.qsize()` |
| `storage_writes_dropped_total` | counter | `event_type` | `QueueFull` drops in `_on_event` |
| `storage_flush_duration_ms` | histogram | `data_type` | time per `_write_rows_sync` call — this is exactly the metric that will surface the O(n²) rewrite problem in production before it becomes an incident |
| `storage_flush_bytes_rewritten` | histogram | `data_type` | existing-file size read back in on every flush — direct visibility into write amplification |
| `storage_partition_row_count` | gauge | `data_type`, `symbol` | current day-file row count, for capacity planning |
| `storage_corrupted_partition_total` | counter | `data_type` | count of `pq.ParquetFile` read failures that fell back to "treat as missing / discard" — silent data-loss events today (`logger.warning` only) |

### 2.5 Historical / backfill

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `backfill_gap_records_total` | counter | `symbol`, `timeframe` | count of `GapRecord`s detected per run |
| `backfill_fetch_duration_seconds` | histogram | `source`, `schema` | vendor API call latency, for pacing-limit tuning (IBKR especially) |
| `backfill_rate_limit_hits_total` | counter | `source` | pacing violations / 429s from vendor APIs |

---

## 3. Alerting rules that should exist on top of the metrics above

| Condition | Severity | Rationale |
|---|---|---|
| `data_quality_state{symbol=*} == FAILED` for > 0s during RTH | page | trading is silently blocked; needs a human, not just a log line |
| `ingest_connection_state{source=primary} == 0` for > 60s during RTH | page | primary feed down |
| `eventbus_dropped_events_total` rate > 0 for `event_type=BAR` | page | bars must never drop (only quotes/trades are `drop_if_full`) — a bar drop indicates the backpressure policy itself is misconfigured or a consumer is stuck |
| `storage_flush_duration_ms` p99 > flush interval (2s) | warn → page if sustained | flush loop falling behind — the write path in section 4 below |
| `storage_writes_dropped_total` rate > 0 | warn | best-effort trade/quote drops are expected occasionally under load spikes but a sustained rate means the write path can't keep up |
| `ingest_outlier_trades_rejected_total` rate spikes (e.g. >5x rolling baseline) | warn | filter band may be miscalibrated, or a genuine vendor data-quality issue |
| `backfill_rate_limit_hits_total` rate > 0 | warn | pacing config too aggressive |
| `data_quality_transitions_total` rate high (flapping) | warn | thresholds (bar lag / silence) may be too tight for current conditions |

---

## 4. Reliability requirements

1. **Unify retry/backoff.** Replace the three independent hand-rolled backoff implementations
   (`IBKRConnection._connect_with_retry`, `DatabentoLiveFeedAdapter._record_loop`,
   `HistoricalIBKRSource` pacing) with one policy, built on the already-declared `tenacity`
   dependency (currently unused). Standard shape: exponential backoff with a multiplicative cap,
   **full jitter** (none of the current implementations have jitter — a shared outage causes
   synchronized reconnect storms), and a bounded number of attempts before escalating to FAILED
   state rather than retrying silently forever.
2. **Cap unattended retries.** `DatabentoLiveFeedAdapter._record_loop` retries forever with no
   ceiling. Target: after N consecutive failed reconnects (e.g. 10, ~time-boxed to a few minutes
   at capped backoff), stop retrying automatically, set `data_quality_state = FAILED` for all
   symbols on that source, and require an explicit operator action to resume — silent infinite
   retry loops hide sustained outages from anyone not actively watching logs.
3. **Durability at the ingress boundary.** At minimum, append raw normalized events to a
   lightweight append-only log (even a rotating flat file) before fan-out, so a crash between
   receipt and Parquet flush is recoverable by replay rather than requiring a vendor re-fetch
   (which may not be possible for tick data past a retention window).
4. **Idempotent persistence for every event type**, not just trades/setups. Every persisted row
   should have a stable dedup key and use upsert semantics uniformly — replace the two competing
   mechanisms (`is_replay` suppression for bars vs. dedup-key upsert for trades/setups vs.
   delete-before-rewrite for quotes) with one mechanism.
5. **Circuit breaker on vendor calls**, not just retry. Historical fetch calls (Databento
   `get_range`, IBKR `reqHistoricalData`) should trip a breaker after repeated failures within a
   window rather than retrying every single caller indefinitely, especially for on-demand paths
   like `date_replay.py` that can be triggered by user action.

---

## 5. Performance / storage requirements

1. **Bound write amplification.** The current Parquet write path (`ParquetStore.write`) performs
   a full read-modify-write of the entire day's partition on every flush (every 2s). Target:
   write amplification should not scale with the size of data already written today. Acceptable
   approaches: append distinct row-group files per flush and compact them on a schedule (e.g.
   hourly or end-of-day), or use a storage engine designed for incremental append (DuckDB,
   Arrow Feather append mode, or a dedicated tick store) for high-frequency `trades`/`quotes`,
   reserving full-file Parquet rewrite for low-frequency `bars`/`setups` where the current
   pattern is fine.
2. **Explicit backfill idempotency contract.** `alpha backfill` and `date_replay.py` should be
   safe to re-run for a date range that's already partially fetched, without relying on manual
   `clear_quotes()` calls — this should be automatic and part of the write path, not a caller
   responsibility.
3. **Capacity/retention policy.** No documented retention/rotation policy exists for tick-level
   Parquet data. Define one (e.g. N days of raw ticks, indefinite bars) before volume becomes a
   disk-management problem.

---

## 6. Data quality / correctness requirements

1. **Sequence-number gap detection** for Databento `trades` (sequence numbers are already
   captured as `trade_id` but never validated for continuity) — currently listed as deferred in
   `docs/ingestion_spec.md`; promote to required once trade data starts feeding real signal logic.
2. **Schema validation beyond Pydantic type coercion.** Pydantic catches type errors, not
   semantic ones (e.g. `high < low`, negative volume, price outside a sane multiple of the last
   N bars). Add a lightweight sanity-check layer at the adapter boundary that rejects or flags
   physically impossible bars/trades before they reach the EventBus.
3. **FAILED-state auto-recovery path.** Currently sticky, requiring a process restart. Add an
   explicit, audited "operator acknowledges and resets" API/CLI action distinct from a full
   restart, so recovery doesn't require losing the rest of the runtime's warm state.
4. **Signal-quality / ingestion audit log.** A structured JSONL (or equivalent) log of every
   quality-state transition and every blocked signal, with enough context (symbol, reason,
   duration, bar timestamps) to reconstruct "why didn't we trade this setup" after the fact —
   listed as deferred in the current spec; this is required for any serious post-mortem process.

---

## 7. Testing requirements

Ingestion/data-quality logic should have test coverage proportional to its blast radius (a bug
here silently corrupts every downstream decision, without necessarily throwing). Minimum bar:

1. `IngestionMonitor` state machine — every transition (CLEAN→DEGRADED→RECOVERING→CLEAN, any→FAILED,
   FAILED stickiness) covered by unit tests with synthetic timestamps.
2. `_is_outlier_trade` / `_update_partial_bar` — boundary tests around the adaptive band (mid ± band,
   fallback path with no quote yet, first-tick-of-minute reset).
3. `ParquetStore.write` dedup/upsert path — restart-then-rewrite scenarios per event type,
   including the corrupted-file-discard branch.
4. `EventBus` backpressure — `drop_if_full` eviction behavior and blocking behavior under a full
   queue, including concurrent publish/drain interleaving.
5. Reconnect logic — simulate transient failures and assert bounded retry count + correct backoff
   sequence (this becomes easy to test once retries are consolidated per §4.1).
6. Fix the currently-stale `tests/unit/test_backfill_engine.py`, which targets methods that have
   moved to `CatchupService`.

---

## 8. Non-goals (explicitly out of scope, consistent with `ARCHITECTURE.md` §7)

- Distributed message brokers (Kafka/NATS) — the durability gap in §4.3 can be addressed with a
  local append-only log; do not reach for a distributed queue until single-process scale is
  actually exceeded.
- Multi-process/multi-node ingestion — not needed at current symbol/venue count.
- Implementing Alpaca/Polygon sources — only in scope once actually needed; until then, their
  settings should be clearly marked unimplemented rather than implying parity with Databento/IBKR.
