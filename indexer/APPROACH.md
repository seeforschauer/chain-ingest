# Approach: chain-ingest

## Architecture Overview

```
                    ┌──────────────────┐
                    │   Redis          │
                    │  - Work queue    │
                    │  - Rate limiter  │
                    │  - Progress set  │
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
  │ PostgreSQL│ │ RPC Node │◄─────────────────────────┘
  │ (storage) │ │ (Anvil)  │
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

**Queue initialization:** Only one worker seeds the queue (distributed lock via `SET NX EX`). Others wait. Already-completed ranges are skipped.

### 2. Distributed Rate Limiting — Sliding Window in Redis

**Approach:** Lua script implementing a sliding window counter using a Redis sorted set. Each request adds a timestamped entry; entries outside the 1-second window are pruned. If the count exceeds the limit, the worker waits ~20-50ms and retries.

**Why Lua script:** Atomic execution — no race conditions between check and increment. Multiple workers share the same sorted set, so the global budget is respected.

**Why not per-worker budgets:** With N workers each getting `limit/N` requests, adding or removing a worker requires reconfiguring all others. A shared global counter is simpler and auto-balances.

### 3. Retry with Backoff

**Approach:** Exponential backoff with jitter: `delay = min(1s * 2^attempt, 30s) + random(0-1s)`.

**Why jitter:** Prevents thundering herd when multiple workers hit the same transient error simultaneously.

**Max retries:** 5 per RPC call. If a block consistently fails within a task, the entire task is requeued for another worker to attempt (different network path, different timing).

### 4. Progress Persistence

**Approach:** Three-layer persistence:
1. **Redis sorted set** — tracks every completed block number. Survives worker restarts.
2. **Redis processing list** — tracks in-flight tasks. Stale tasks are reclaimed after 2 minutes.
3. **PostgreSQL** — source of truth. `ON CONFLICT DO NOTHING` makes inserts idempotent.

**Resume behavior:** On restart, the init lock prevents re-seeding. Workers continue claiming tasks from the existing queue. Already-completed blocks are skipped within each task.

### 5. Schema Design

Five tables in a `raw` schema:

| Table | Primary Key | Notable |
|-------|-----------|---------|
| `raw.blocks` | `block_number` | Partitioned by range, stores `indexed_by` worker ID for debugging |
| `raw.transactions` | `tx_hash` | No FK (partitioned tables), indexed on `from`/`to` |
| `raw.receipts` | `tx_hash` | Stores log count |
| `raw.logs` | `(block_number, id)` | Partitioned, JSONB topics with GIN index, UNIQUE on `(block_number, tx_hash, log_index)` |
| `raw.indexer_state` | `chain_id` | High-water mark for per-chain progress tracking, survives Redis flushes |

**Why NUMERIC for gas/value:** JavaScript loses precision above 2^53. Storing as NUMERIC (via BigInt.toString()) preserves full EVM precision.

**Why idempotent inserts:** Workers may retry blocks. `ON CONFLICT DO NOTHING` ensures no duplicates.

## RPC Endpoint

Using Anvil (local fork of Ethereum mainnet):
```bash
cd anvil && docker compose up -d
# RPC at http://localhost:8545
```

Follows finalized chain tip by default (falls back to latest for Anvil).

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

# Start worker for a different chain (keys are chain-scoped)
CHAIN_ID=137 RPC_URL=https://polygon-rpc.com npm start

# Verbose logging
LOG_LEVEL=debug npm start
```

## Failure Scenarios

| Scenario | Handling |
|----------|---------|
| Worker crashes mid-task | Task stays in `processing` list, reclaimed after 2 min by any surviving worker |
| RPC returns 429 | Rate limiter backs off; RPC client retries with exponential backoff |
| RPC timeout | Retry with backoff (up to 5 attempts per call) |
| Redis goes down | Workers crash (by design — Redis is the coordination backbone). On Redis restart, persisted data survives (AOF enabled) |
| PostgreSQL goes down | Insert fails, task is requeued. Idempotent inserts handle re-processing |
| Duplicate blocks | `ON CONFLICT DO NOTHING` on all tables |
| Block reorgs | Using `finalized` block tag avoids reorgs entirely |

## What I'd Add for Production

Reorg handling: index at `latest` with a reorg buffer — store the last N block hashes and detect `parentHash` breaks (the inline `parentHash` check in `indexBlock` already does this), then re-index affected blocks. The current `finalized` approach is safe but runs 12-15 minutes behind chain tip on Ethereum. Monitoring via Prometheus metrics (blocks/sec, RPC latency histograms, circuit breaker state) feeding Grafana dashboards with PagerDuty alerting on ingestion lag. CI/CD with GitHub Actions for lint/test/build and Docker multi-stage builds for minimal production images.

## Scale Considerations

Table partitioning by `block_number` range (1M blocks per partition) so the query planner only touches relevant partitions for range queries. Read replicas to separate indexing write load from API query load. BRIN indexes on `block_number` columns — blockchain data is physically ordered by insertion time, making BRIN ~100x smaller than B-tree with comparable lookup performance. Connection pooling via PgBouncer to handle 100+ concurrent worker connections without exhausting PostgreSQL's backend slots. A Redis caching layer for hot block data and recently decoded events, reducing read pressure on PostgreSQL.

## Token Terminal Context

This indexer represents the "ingestion" layer — analogous to TT's Go-based chain indexers that feed raw blockchain data into the pipeline. At 120 chains, each chain would run as a separate indexer instance with chain-specific config (RPC endpoints, block times, finality rules). TT uses ClickHouse for the serving/OLAP layer rather than PostgreSQL (OLTP) — the real pipeline would ETL from PG into ClickHouse for sub-second analytical queries, with BigQuery as the historical data warehouse and dbt for transforms. The patterns implemented here — idempotent inserts, distributed rate limiting, Redis work queues, circuit breakers — translate directly to TT's scale where reliability across 120 chains matters more than raw speed on any single one.
