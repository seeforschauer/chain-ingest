# Approach: chain-ingest

## Architecture Overview

```
                    ┌──────────────────┐
                    │   Redis          │
                    │  - Work queue    │
                    │  - Rate limiter  │
                    │  - Progress set  │
                    │  - Heartbeats    │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
  ┌─────▼─────┐       ┌─────▼─────┐       ┌─────▼─────┐
  │  Worker 1  │       │  Worker 2  │       │  Worker N  │
  │  (main.ts) │       │  (main.ts) │       │  (main.ts) │
  └─────┬──┬───┘       └─────┬──┬───┘       └─────┬──┬───┘
        │  │                 │  │                 │  │
        │  └─────────┬───────┘  └─────────┬───────┘  │
        │            │                    │           │
  ┌─────▼─────┐ ┌────▼─────┐                         │
  │ PostgreSQL│ │ RPC Pool │◄─────────────────────────┘
  │ (storage) │ │(failover)│
  └───────────┘ └──────────┘
```

## Design Decisions

### 1. Work Distribution — Redis List Queue

**Approach:** Block ranges are pushed to a Redis list (`pending`). Workers atomically pop tasks using `BRPOPLPUSH` into a `processing` list. On completion, tasks are removed from processing and individual blocks are marked in a sorted set.

**Why this over alternatives:**
- `BRPOPLPUSH` is atomic — no risk of two workers claiming the same task
- Blocking pop avoids busy polling
- Processing list enables stale task detection — if a worker crashes, its task stays in `processing` and can be reclaimed after a timeout
- Sorted set of completed blocks enables gap detection and resume-after-restart

**Queue initialization:** Only one worker seeds the queue (distributed lock via `SET NX EX`, TTL scales with block range). Others wait. Already-completed ranges are skipped.

**Dead-letter queue:** Tasks that fail more than 5 times are routed to `queue:dead` instead of being requeued indefinitely. Prevents poison blocks from consuming worker capacity.

### 2. Distributed Rate Limiting — Shared Adaptive Sliding Window

**Approach:** Lua script implementing a sliding window counter using a Redis sorted set. Each request adds a timestamped entry; entries outside the 1-second window are pruned.

**Adaptive behavior:** On 429 responses, the effective rate drops 25% (via atomic Lua script in Redis — all workers see the reduction simultaneously). After 10 consecutive healthy requests, rate recovers 10%, capped at the configured ceiling. Floors at 1 req/s to prevent complete stall.

**Why shared state in Redis:** Per-worker rate tracking would allow one worker to throttle while others continue over-submitting. Storing `effectiveRate` in Redis ensures all workers for a chain reduce together when any one receives a 429.

### 3. Retry with Backoff + Circuit Breaker

**Backoff:** `delay = min(1s * 2^attempt, 30s) + random(0-1s)`. Jitter prevents thundering herd.

**Circuit breaker:** Windowed failure rate tracking (20-call sliding window, trips at 60% failure rate). When open, rejects calls immediately for a cooldown period, then allows one probe call. 429 responses are explicitly excluded from the failure window — a 429 means the RPC is healthy but overloaded, and the rate limiter handles it.

**RPC pool failover:** When multiple endpoints are configured (`RPC_URLS`), requests are distributed round-robin. If one endpoint's circuit opens or retries exhaust, the pool automatically fails over to the next endpoint. Each endpoint maintains independent circuit breaker state.

### 4. Progress Persistence

**Three-layer persistence:**
1. **Redis sorted set** — tracks every completed block number. Entries below the PG watermark are evicted to bound Redis memory.
2. **Redis processing list** — tracks in-flight tasks. Stale tasks are reclaimed via pipelined heartbeat checks after 2 minutes.
3. **PostgreSQL** — source of truth. `ON CONFLICT DO NOTHING` makes inserts idempotent. Per-chain high-water mark in `raw.indexer_state` survives Redis flushes.

**Partial progress:** When a task partially completes and fails, only blocks confirmed durable in PostgreSQL (not just buffered) are marked as completed. The write buffer flush-before-complete ordering ensures no data gap between Redis progress and PG content.

**Resume behavior:** On restart, the init lock prevents re-seeding. Workers continue claiming tasks from the existing queue. Already-completed blocks are skipped within each task via a batch `ZRANGEBYSCORE` check.

### 5. Schema Design

Five tables in a `raw` schema, all data tables range-partitioned by `block_number` (1M blocks per partition):

| Table | Primary Key | Notable |
|-------|-----------|---------|
| `raw.blocks` | `block_number` | Partitioned, stores `indexed_by` worker ID for debugging |
| `raw.transactions` | `(block_number, tx_hash)` | Partitioned, B-tree on `from`/`to` for address lookups |
| `raw.receipts` | `(block_number, tx_hash)` | Partitioned, `block_number` denormalized for co-partitioning |
| `raw.logs` | `(block_number, id)` | Partitioned, B-tree on `topic0` + `address`, UNIQUE on `(block_number, tx_hash, log_index)` |
| `raw.indexer_state` | `chain_id` | High-water mark per chain, survives Redis flushes |

**Why BIGINT for gas, NUMERIC for value:** Gas values fit in BIGINT (max ~2^32), saving 40-60 bytes per row vs NUMERIC. Token values (`value` field) can exceed 2^64, so NUMERIC preserves full EVM uint256 precision.

**Why topic0 B-tree instead of GIN on full JSONB:** GIN indexes have the highest write amplification of any PG index type. `topic[0]` (event signature) covers ~90% of log queries. Extracted into a dedicated `topic0` column with a B-tree index — fast point lookups, minimal write overhead.

**Why no foreign keys:** PostgreSQL does not support FKs on partitioned tables. Data is always inserted atomically in one transaction, so referential integrity is application-enforced.

**Why idempotent inserts:** Workers may retry blocks. `ON CONFLICT DO NOTHING` ensures no duplicates without coordination overhead.

### 6. Write Buffer

**Approach:** Configurable batch accumulator (`FLUSH_SIZE`, `FLUSH_INTERVAL_MS`) that buffers N blocks before flushing to PostgreSQL in a single transaction. Reduces PG round-trips by `flushSize×`. Promise-chain serialization prevents concurrent flush races.

**Data safety:** Buffer is flushed before marking any task complete in Redis. On drain (SIGTERM), buffer is flushed before requeuing. On shutdown, `main.ts` performs a final flush before closing connections.

### 7. Observability

**Prometheus metrics:** Counters (blocks indexed, transactions, RPC calls, errors, storage flushes), gauges (current block, buffer size, queue depth, effective rate), and histograms (RPC duration, storage flush duration, block processing duration). All instrumented in the worker hot path. Served via HTTP on `METRICS_PORT` with `/healthz` for k8s probes.

**Structured logging:** JSON to stdout (info/debug/warn) and stderr (error). Level filtering via `LOG_LEVEL`. Per-worker metrics logged every 30 seconds (blocks/sec, uptime, error count).

## RPC Endpoint

Using Anvil (local Ethereum devnet):
```bash
cd anvil && docker compose up -d
# RPC at http://localhost:8545
```

Follows finalized chain tip by default (`eth_getBlockByNumber("finalized")`). Falls back to `latest` when `finalized` is not supported (Anvil, older nodes). Configurable via `END_BLOCK=finalized|latest|<number>`.

**Required RPC methods:** `eth_blockNumber`, `eth_getBlockByNumber` (with full transactions), `eth_getBlockReceipts`. Block + receipts are fetched in a single JSON-RPC batch call to halve network round-trips.

## Running

```bash
# Start infrastructure
docker compose up -d
cd anvil && docker compose up -d && cd ..

# Run migration
cd indexer && npm run migrate

# Seed queue only (Unix: separate seeder from workers)
SEED_ONLY=true npm start

# Start worker 1
npm start

# Start worker 2 (separate terminal)
WORKER_ID=worker-2 npm start

# Multi-RPC pool
RPC_URLS=http://rpc1:8545,http://rpc2:8545 npm start

# Batch mode with metrics
FLUSH_SIZE=50 FLUSH_INTERVAL_MS=5000 METRICS_PORT=9090 npm start

# Verbose logging
LOG_LEVEL=debug npm start
```

## Failure Scenarios

| Scenario | Handling |
|----------|---------|
| Worker crashes mid-task | Task stays in `processing` list, reclaimed after 2 min by any surviving worker via pipelined heartbeat checks |
| Worker crashes mid-block | PG transaction rolls back (no partial data). Block is re-processed on reclaim. Idempotent inserts handle duplicates |
| RPC returns 429 | Shared rate limiter cuts effective rate 25% across all workers (via Redis Lua). RPC client retries with exponential backoff |
| RPC endpoint goes down | Circuit breaker opens after 60% failure rate in 20-call window. RPC pool fails over to next endpoint if available |
| Redis goes down | Workers crash (by design — Redis is the coordination backbone). On Redis restart, persisted data survives (AOF). PG watermark enables recovery |
| PostgreSQL goes down | Insert fails, task is requeued. Idempotent inserts handle re-processing when PG recovers |
| Duplicate blocks | `ON CONFLICT DO NOTHING` on all tables — safe to re-process any block |
| Block reorgs | Using `finalized` block tag avoids reorgs entirely. `parentHash` chain check halts the task on discontinuity (requeued for retry via different RPC endpoint) |
| Poison block (always fails) | Dead-letter queue after 5 retries — task moved to `queue:dead` instead of infinite requeue |
| Slow shutdown | 25-second deadline timer forces exit before k8s SIGKILL (30s default grace period) |
| Write buffer data loss | Buffer flushed before task completion, before drain, and before shutdown. Only PG-confirmed blocks are marked in Redis |

## What I'd Add for Production

**Reorg handling:** Index at `latest` with a configurable confirmation depth (`CONFIRMATION_DEPTH=64`). Store the last N block hashes in `indexer_state` and detect `parentHash` breaks across task boundaries, not just within tasks. On reorg detection, `DELETE` orphaned blocks from PG and re-index from the fork point.

**Storage tiering:** Hot data in PostgreSQL (recent partitions), cold data exported to Parquet on S3 via a `TieringManager`. Detach and drop PG partitions after successful S3 upload. At 120 chains × years of history, this bounds PG disk usage.

**COPY protocol:** For historical backfill, replace multi-row INSERT with `COPY FROM STDIN` via staging tables + upsert. 3-10x faster for bulk immutable data.

**HTTP connection pooling:** Configure undici `Agent` with `maxSockets` matched to the rate limiter ceiling. Prevents TCP churn under timeouts.

**CI/CD:** GitHub Actions for lint/test/build. Docker multi-stage builds (Dockerfile included). Helm chart for Kubernetes deployment with per-chain Deployments and configurable resource limits.

## Scale Considerations

Table partitioning by `block_number` range (1M blocks per partition) — partitions pre-created up to `END_BLOCK` during migration to avoid DDL lock storms at runtime. Read replicas to separate indexing write load from API query load. BRIN indexes on timestamp columns (physically ordered data, ~100x smaller than B-tree). B-tree on `topic0` for ERC-20 Transfer filtering (the dominant query pattern) instead of GIN on full JSONB (highest write amplification in PG). Connection pooling via PgBouncer for 120-chain deployments where `PG_POOL_MAX × chain_count` would exceed PG `max_connections`.

## Token Terminal Context

This indexer represents the "ingestion" layer — analogous to TT's Go-based chain indexers that feed raw blockchain data into the pipeline. In the full pipeline:

```
Ingestion (this)  →  ETL (dbt)  →  OLAP (ClickHouse)  →  Warehouse (BigQuery)
    PG (OLTP)          Transform       Sub-second queries     Historical analytics
```

At 120 chains, each chain would run as a separate indexer instance with chain-specific config (RPC endpoints, block times, finality rules). The `indexer_state.high_water_mark` table provides the checkpoint that downstream CDC (Change Data Capture) or periodic ETL jobs use to replicate incremental data from PG into ClickHouse.

The patterns implemented here — idempotent inserts, distributed rate limiting, Redis work queues, circuit breakers, adaptive backpressure, dead-letter queues — translate directly to TT's scale where reliability across 120 chains matters more than raw speed on any single one.
