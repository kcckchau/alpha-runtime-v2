# Alpha Runtime — 2026 Target Architecture & Language Decision

This is my recommended architecture if I were specifying alpha-runtime's data plane today, with
full freedom on language and topology. It is deliberately **not** a rewrite proposal — it's a
target shape reached by evolving the current codebase, because the current codebase's core
decisions (Python, asyncio, normalized event contracts, single process) are already correct for
this system's actual requirements. What changes is which pieces of infrastructure do the jobs
currently done by hand-rolled code, and where process boundaries go.

---

## 0. Decision summary

| Layer | Choice | Status vs. today |
|---|---|---|
| Language | **Python 3.13**, async/await | unchanged |
| Process topology | **Single core "brain" process** + one detachable **ingestion capture process** | evolution — currently fully single-process |
| System of record for market data | **TimescaleDB** (hypertables, native compression, continuous aggregates) | *already provisioned in `docker-compose.yml`, currently unused* — `PostgresStore` is a stub |
| Cold / portable storage | **Parquet**, generated as a periodic export *from* Timescale | today Parquet is hand-written directly and is the only store |
| In-process fan-out | **`asyncio.Queue`-based EventBus**, as-is | unchanged — it's well designed |
| Cross-process durability | Timescale insert acts as the write-ahead log; consumers resume from committed rows on restart | new — today there is no durability boundary at all |
| Resilience | **`tenacity`** (already a declared dependency, currently unused) for all retry/backoff | consolidation, not addition |
| Metrics | **`prometheus-client`** exposed on the existing FastAPI app | new |
| Dashboards / alerting | **Grafana** + **Prometheus** (+ **Loki** for logs) | new, but three more containers next to the two already in `docker-compose.yml` |
| Schema contracts | **Pydantic v2**, as-is, + a `schema_version` field and generated JSON Schema checked into the repo | unchanged + light discipline |
| Secrets | `.env` encrypted at rest with **SOPS + age**, decrypted at process start | new, no new server dependency |
| Deployment | **Docker Compose**, one host | unchanged |

Everything below explains *why*, in the same order.

---

## 1. Language: Python 3.13, no change

This is not a default-by-inertia choice — it's the correct one for what this system actually is.

**What this system is not**: colocated HFT. The existing SLA (`bar_lag` warn threshold = 250ms,
`_BAR_LAG_WARN_MS` in `monitor.py`) is a *look-slow-and-you're-degraded* threshold measured in
hundreds of milliseconds, not microseconds. There is no matching engine to race, no queue-position
game to win. The actual bottleneck in this system is never "Python is too slow to parse a tick" —
Databento and IBKR's own SDKs are the slow part, and they're Python already.

**What actually matters at this scale**: iteration speed on strategy logic, a rich quant/data
ecosystem (pandas/pyarrow/numpy), first-party vendor SDKs (`databento`, `ib_insync`), and a type
system expressive enough to keep event contracts honest (Pydantic v2, already in use and doing
its job well). Rewriting the ingestion path in Rust or Go would trade all of that for a latency
budget the system doesn't need to spend.

**The one place a systems language could earn its keep**: if a specific hot loop is *proven* by
profiling to be a bottleneck (e.g. DBN record parsing under a sustained tick storm on a much more
liquid instrument than MNQ), bind a narrow Rust extension via `PyO3`/`maturin` for that one
function and keep everything else in Python. Not needed today — there is no profiling evidence of
this being a problem, and introducing it speculatively is exactly the kind of premature complexity
`ARCHITECTURE.md` §7 already correctly rejects.

**Verdict: Python 3.13, unchanged.** Upgrading from 3.12 → 3.13 for the modest asyncio/typing
improvements is a nice-to-have, not a rewrite.

---

## 2. Process topology: split "capture" from "brain," durably

This is the one real structural change, and it's the direct fix for the durability gap identified
in `docs/ingestion_target_spec.md` §1 and §4.3 ("no received-and-acked event survives a crash").

**Today**: one process does everything — connects to vendors, normalizes, fans out over an
in-memory `EventBus`, runs strategy logic, calls the broker. A crash or an unhandled exception
anywhere in strategy/setup/scoring code takes the live market-data connection down with it, and
any event sitting in an `asyncio.Queue` at that moment is gone.

**Target**: two processes on the same host, still no distributed system:

```
┌─────────────────────────┐        ┌──────────────────────────┐
│   alpha-ingest           │        │   alpha-brain             │
│  (Databento/IBKR adapters│        │  (feature/setup/scoring/  │
│   + normalization)        │        │   risk/order engines +    │
│                            │        │   FastAPI/WS dashboard)   │
│  writes every normalized  │───────▶│  tails Timescale by       │
│  event to Timescale        │        │  committed timestamp,     │
│  (this IS the durability   │        │  resumes exactly where    │
│  boundary — see §3)        │        │  it left off after a      │
└─────────────────────────┘        │  crash/restart             │
                                     └──────────────────────────┘
```

- `alpha-ingest` is intentionally tiny and boring: adapter code, `IngestionMonitor`,
  `IngressObserver`, and a Timescale writer. Nothing in it should ever raise past a caught
  boundary that could crash the process — this is the one place "never go down" is the design goal.
- `alpha-brain` is everything else, unchanged in its internal design (same engines, same
  `EventBus`, same event contracts). It becomes a *consumer* of Timescale rather than the place
  events are first durably recorded.
- If `alpha-brain` crashes, restarts, or is redeployed, market data capture is uninterrupted and
  Timescale already has everything that happened while it was down; `alpha-brain` catches up by
  reading committed rows newer than its last-processed watermark, then re-attaches to the live
  feed the same way `catchup.py` already bridges historical→live today. **This is the existing
  catchup/live-handoff pattern (`docs/bootstrap-catchup.md`), just also used for
  process-crash-recovery, not only cold start.**
- Both processes stay on one host, started by the same `docker-compose.yml`. This is *not* a
  microservices architecture — it's one architecture with a crash-isolation boundary drawn in the
  one place that actually needs it (market-data capture must outlive strategy-logic bugs).

If this feels like too much for current scale, the fallback that captures most of the benefit for
near-zero cost is: **keep one process, but insert into Timescale synchronously before publishing
to the EventBus** (§3 below), and treat that insert as the recovery point rather than splitting
processes. Do the split only once `alpha-brain` restarts often enough (deploys, strategy
iteration) that losing live-feed continuity on every restart actually hurts.

---

## 3. System of record: TimescaleDB, not hand-rolled Parquet

`docker-compose.yml` already runs `timescale/timescaledb:latest-pg16` — it's provisioned and
unused. `PostgresStore` is `NotImplementedError`. This is the highest-leverage change available:
**finish wiring infrastructure you already pay for**, rather than adding something new.

Why this replaces the current Parquet-as-primary-store design:

- `ParquetStore.write()` does a full read-modify-write of the entire day's file on every 2-second
  flush (`docs/ingestion_target_spec.md` §5.1) — this is the single biggest scalability risk
  identified in the ingestion audit. Timescale's hypertables are chunk-based and designed
  precisely for high-frequency append workloads; the write-amplification problem disappears
  because it's the storage engine's job, not hand-rolled file rewriting.
- Timescale gives native compression policies, continuous aggregates (e.g. a `1m` bar
  materialized view derived from `trades`, for free, always up to date), and retention policies —
  all things §5.3 and §6 of the target spec called out as missing/undefined today.
- SQL queryability for ad hoc debugging ("show me every trade for MNQ between 14:32:00 and
  14:32:05") is materially better than opening Parquet files by hand.
- It is a WAL by construction: a committed `INSERT` is durable. This is what makes the
  process-split in §2 safe — a consumer can always ask "what's newer than the last row I
  processed" and get a correct, gap-free answer.

**Parquet doesn't go away — it changes role.** Backtest scripts (`scripts/backtest.py`,
`scripts/replay_day.py`) and any external tooling already expect Parquet, and it remains the right
format for **cold, portable, columnar analytics**. The change is that Parquet becomes a periodic
**export** (end-of-day, or hourly) generated by a single bulk `COPY ... TO 'file.parquet'` (via the
`pg_parquet` extension, or a scheduled `pyarrow` dump) — one write per partition per day, not
thousands. This keeps every downstream script working unmodified while fixing the write path that
feeds them.

Non-goal: **do not** additionally reach for DuckDB, ClickHouse, or any other new storage engine.
Timescale is already provisioned, already in your dependency tree (`asyncpg`, `sqlalchemy[asyncio]`
are already `pyproject.toml` dependencies), and is a boring, well-understood choice for a
single-node time-series workload at this scale. Adding a second storage engine on top of an
unused one already sitting in `docker-compose.yml` would be the wrong kind of complexity.

---

## 4. In-process fan-out: keep the existing `EventBus`

The current `asyncio.Queue`-per-subscriber design with explicit `drop_if_full` vs. block semantics
(`src/alpha/core/event_bus.py`) is genuinely good design — it correctly separates "must never
lose this" (bars, blocking) from "only the latest matters" (quotes, drop-oldest) at the type level,
and it's simple to reason about because the event loop is single-threaded. **Nothing here needs to
change.** The durability problem it seemed to have is actually solved one layer up, by making
Timescale the thing that's durable (§3) — the `EventBus` only ever needs to be correct *within* a
single process's lifetime, which it already is.

Explicitly rejected: replacing this with NATS JetStream, Redis Streams, or any broker. Once
Timescale is the durability boundary between `alpha-ingest` and `alpha-brain`, there is no
remaining problem a broker would solve that isn't better solved by Timescale, which you already
run. Introducing a broker here would be complexity added for a problem that no longer exists.

---

## 5. Resilience: consolidate onto `tenacity`

`tenacity>=8.3` has been a declared dependency with zero imports the entire time. Three different
hand-rolled backoff loops exist today (`IBKRConnection._connect_with_retry`,
`DatabentoLiveFeedAdapter._record_loop`, IBKR historical pacing), each with different caps, none
with jitter. Replace all three with one shared retry policy:

```python
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, before_sleep_log

vendor_retry = retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential_jitter(initial=1, max=60, jitter=5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
```

`stop_after_attempt` is the important addition: today's Databento reconnect loop retries forever
with no ceiling, which means a sustained multi-hour outage looks identical, from the logs, to a
5-second blip. After the cap is hit, transition the source to `DataQualityState.FAILED` (already
modeled) and require an explicit operator action to resume — this is already called out as a gap
in `docs/ingestion_target_spec.md` §4.2.

---

## 6. Observability: Prometheus + Grafana + Loki, on infrastructure you already run

The FastAPI app already exists (`src/alpha/api/app.py`). Add `prometheus-client` and expose
`/metrics` on it — zero new application infrastructure. Then:

- **Prometheus** container scrapes `/metrics` (the full catalog is in
  `docs/ingestion_target_spec.md` §2 — queue depths, bar lag, drop counts, flush duration, etc.)
- **Grafana** container visualizes it and holds the alert rules from target-spec §3
  (`data_quality_state == FAILED` during RTH → page, etc.)
- **Loki** (+ Promtail or Docker's native log driver) ships the existing `structlog` JSON output
  for correlation with metrics, without changing how logging is done today.

Three containers added to the existing two-service `docker-compose.yml`. Still one host, still no
Kubernetes, still consistent with `ARCHITECTURE.md` §7.

---

## 7. Schema discipline: Pydantic stays, add versioning

Keep Pydantic v2 as the single source of truth for event contracts — it's already doing this job
well and rewriting it as Protobuf/Avro would buy schema-registry rigor this single-consumer,
single-language system doesn't need. Two small additions:

1. Add `schema_version: int` to `EventMetadata` so that, once Timescale/Parquet holds months of
   history, a deliberate contract change (e.g. adding a field to `BarEvent`) can be migrated
   deterministically instead of silently producing rows with different shapes across time.
2. Generate JSON Schema from the Pydantic models (`model_json_schema()`) and check the output into
   the repo, diffed in CI — cheap, catches accidental breaking changes to event contracts in
   review, no new tooling required.

---

## 8. Secrets: SOPS + age instead of plaintext `.env`

Databento/IBKR credentials are already handled correctly at the code level (`SecretStr`
everywhere, never logged). The remaining gap is that `.env` itself is plaintext on disk. For a
solo/small-team system, running a full Vault server is the wrong tradeoff — instead, encrypt
`.env` at rest with [`sops`](https://github.com/getsops/sops) + `age` (a single static binary, no
server, no ops burden), decrypt to memory at process start. This is the standard 2026 "boring"
secrets pattern for small teams that don't want to operate Vault/Vault-agent infrastructure.

---

## 9. What I'm explicitly rejecting, and why

| Rejected | Why |
|---|---|
| Rust/Go/C++ rewrite of the ingestion path | No latency requirement (§1) justifies it; would trade ecosystem and iteration speed for a problem that doesn't exist |
| Kafka / Redpanda | Timescale already gives durability + replay at the one process boundary that needs it (§2, §4); a distributed log is solving an already-solved problem |
| NATS JetStream | Same reasoning as Kafka — smaller footprint, still an unnecessary addition once Timescale is the WAL |
| DuckDB / ClickHouse | Would be a *second* unused-then-adopted storage engine when Timescale is already provisioned and idle; solves nothing DuckDB uniquely would |
| Kubernetes / multi-node | Single host, single trader/small-team operational model — nothing about this workload needs orchestration |
| Protobuf/Avro schema registry | Pydantic + versioned JSON Schema in CI covers the actual risk (silent breaking changes) without a registry service |
| Full HashiCorp Vault | Right answer for a larger org with many services/secrets; `sops`+`age` gets the "no plaintext secrets at rest" property with zero new infrastructure |

---

## 10. Migration path (incremental, no big-bang rewrite)

1. Implement `PostgresStore` against the already-running Timescale container; make it the write
   target for `StorageEngine` (§3). Keep Parquet writing exactly as-is in parallel initially — run
   both for one review cycle to diff outputs before cutting Parquet over to export-only.
2. Once Timescale is authoritative and stable, convert the Parquet writer to a scheduled export
   job (§3) and delete the hand-rolled read-modify-write path in `parquet.py`.
3. Consolidate the three retry loops onto `tenacity` (§5) — independent of everything else, can
   land any time.
4. Add `prometheus-client` + `/metrics` + stand up Prometheus/Grafana containers (§6) — also
   independent, and immediately valuable even before the process split.
5. Only after 1–4 are stable and boring: split `alpha-ingest` out of the main process (§2). This is
   the highest-effort, highest-payoff step, and it's safe to do last because Timescale durability
   (step 1) is the prerequisite that makes the split low-risk instead of just moving the crash
   boundary around.
