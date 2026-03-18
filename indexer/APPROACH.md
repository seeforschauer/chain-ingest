# Approach: chain-ingest

## Architecture Overview

```
                    ┌──────────────────┐
                    │   Redis          │
                    │  - Task stream   │
                    │  - Rate limiter  │
                    │  - Bitmap        │
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

### 1. Work Distribution — Redis Streams

**Approach:** Block ranges are published as entries in a Redis Stream (`indexer:{chainId}:tasks`). A consumer group (`workers`) ensures each entry is delivered to exactly one worker via `XREADGROUP`. On completion, entries are acknowledged (`XACK`) and deleted (`XDEL`).

**Why Streams over Lists:**
- `XREADGROUP` delivers each entry to exactly one consumer — atomic claiming without `BLMOVE`
- Built-in pending entry list (PEL) tracks what's been delivered but not acknowledged — no custom heartbeat mechanism needed
- `XAUTOCLAIM` atomically reclaims idle entries from crashed workers — replaces ~150 lines of manual stale detection
- `XACK` is O(1) — no O(n) `LREM` scan

**Queue initialization:** Only one worker seeds the stream (distributed lock via `SET NX EX`, TTL scales with block range). Others wait. Already-completed ranges are skipped via bitmap checks.

**Dead-letter queue:** Tasks that fail more than 5 times are moved to a separate DLQ stream (`indexer:{chainId}:dlq`). Prevents poison blocks from consuming worker capacity.

**Requeue on failure:** Failed tasks are acknowledged in the original stream and re-added as new entries with `retryCount + 1`. This ensures any worker can pick them up (not just the failing one).

### 2. Distributed Rate Limiting — Shared Adaptive Sliding Window

**Approach:** Lua script implementing a sliding window counter using a Redis sorted set. Each request adds a timestamped entry; entries outside the 1-second window are pruned.

**Adaptive behavior:** On 429 responses, the effective rate drops 25% (via atomic Lua script in Redis — all workers see the reduction simultaneously). After 10 consecutive healthy requests, rate recovers via `max(current+1, ceil(current*1.10))`, capped at the configured ceiling. Floors at 1 req/s to prevent complete stall.

**Why shared state in Redis:** Per-worker rate tracking would allow one worker to throttle while others continue over-submitting. Storing `effectiveRate` and `healthyStreak` in Redis ensures all workers for a chain reduce together when any one receives a 429.

### 3. Retry with Backoff + Circuit Breaker

**Backoff:** `delay = min(1s * 2^attempt, 30s) + random(0-1s)`. Jitter prevents thundering herd.

**Circuit breaker:** Ring buffer failure rate tracking (20-call sliding window, trips at 60% failure rate). When open, rejects calls immediately for a cooldown period, then allows one probe call. 429 responses are explicitly excluded from the failure window — a 429 means the RPC is healthy but overloaded, and the rate limiter handles it.

**RPC pool failover:** When multiple endpoints are configured (`RPC_URLS`), requests are distributed round-robin. If one endpoint's circuit opens or retries exhaust, the pool automatically fails over to the next endpoint. Each endpoint maintains independent circuit breaker state.

### 4. Progress Persistence

**Three-layer persistence:**
1. **Redis bitmap** — 1 bit per completed block. At 20M blocks per chain = 2.5 MB (vs 1.28 GB with sorted sets). No eviction needed — fixed memory footprint.
2. **Redis Streams PEL** — tracks in-flight tasks via the pending entry list. Idle entries are reclaimed via `XAUTOCLAIM` after a configurable timeout.
3. **PostgreSQL** — source of truth. `ON CONFLICT DO NOTHING` makes inserts idempotent. Per-chain contiguous high-water mark in `raw.indexer_state` survives Redis flushes.

**Contiguous watermark:** The watermark only advances to the highest contiguous completed block, scanning the bitmap forward from the current watermark via Lua script and stopping at the first zero bit. This prevents the watermark from jumping past unfinished blocks when out-of-order tasks complete.

**Partial progress:** When a task partially completes and fails, only blocks confirmed durable in PostgreSQL (not just buffered) are marked as completed in the bitmap. The write buffer flush-before-complete ordering ensures no data gap between Redis progress and PG content.

**Resume behavior:** On restart, the init lock prevents re-seeding. Workers continue claiming entries from the existing stream. Already-completed blocks are skipped within each task via a Lua-based bitmap range scan.

### 5. Schema Design

Five tables in a `raw` schema, all data tables range-partitioned by `block_number` (1M blocks per partition). Hash and address columns use **BYTEA** for 50%+ storage savings at scale.

| Table | Primary Key | Notable |
|-------|-----------|---------|
| `raw.blocks` | `block_number` | Partitioned, BYTEA hashes, EIP-4844 columns (`blob_gas_used`, `excess_blob_gas`) |
| `raw.transactions` | `(block_number, tx_hash)` | Partitioned, BYTEA hashes/addresses, EIP-1559 (`max_fee_per_gas`, `max_priority_fee_per_gas`), EIP-4844 (`max_fee_per_blob_gas`, `blob_versioned_hashes`) |
| `raw.receipts` | `(block_number, tx_hash)` | Partitioned, `block_number` denormalized for co-partitioning |
| `raw.logs` | `(block_number, tx_hash, log_index)` | Partitioned, composite B-tree on `(address, topic0)` |
| `raw.indexer_state` | `chain_id` | Contiguous high-water mark per chain, survives Redis flushes |

**Why BYTEA:** Hashes (32 bytes as BYTEA vs 67 bytes as TEXT) and addresses (20 bytes vs 43 bytes) save 50%+ storage. At 120 chains, TEXT wastes 50-80 TB. `hexToBytes()` converts at the storage boundary.

**Why topic0 B-tree instead of GIN on full JSONB:** GIN indexes have the highest write amplification of any PG index type. `topic[0]` (event signature) covers ~90% of log queries. Extracted into a dedicated `topic0` BYTEA column with a B-tree index.

**Why no foreign keys:** PostgreSQL does not support FKs on partitioned tables. Data is always inserted atomically in one transaction.

**Staging-table bulk path:** For large batches (≥100 blocks), inserts go into unindexed temp tables first, then merge into real tables with `INSERT ... SELECT ... ON CONFLICT DO NOTHING`. ~2-3x faster than direct INSERT for backfill.

### 6. Per-Chain Finality (ChainProfile)

Different EVM chains have fundamentally different finality semantics. `ChainProfile` encapsulates these:

- **Ethereum**: `"finalized"` block tag (Casper FFG, ~12 min latency)
- **Polygon**: 128-block depth finality (Heimdall checkpoints)
- **Arbitrum/Optimism/Base**: L2 instant soft-finality
- **BSC**: 15-block depth finality

`resolveEndBlock()` dispatches to the correct strategy per chain. Unknown chains fall back to tag-based finality with conservative defaults.

### 7. Write Buffer

**Approach:** Configurable batch accumulator (`FLUSH_SIZE`, `FLUSH_INTERVAL_MS`) that buffers N blocks before flushing to PostgreSQL in a single transaction. Promise-chain serialization prevents concurrent flush races. On flush failure, the promise chain resets and the batch is restored to the buffer for retry.

**Data safety:** Buffer is flushed before marking any task complete in Redis. On drain (SIGTERM), buffer is flushed before requeuing. On shutdown, `main.ts` performs a final flush before closing connections.

### 8. Observability

**Prometheus metrics:** 5 counters, 7 gauges (including DLQ size), 3 histograms — all 15 metrics instrumented in the worker hot path. Served via HTTP on `METRICS_PORT` with `/healthz` for k8s probes.

**Structured logging:** JSON to stdout/stderr with level filtering. Per-worker metrics logged every 30 seconds.

## RPC Endpoint

Using Anvil (local Ethereum devnet):
```bash
cd anvil && docker compose up -d
# RPC at http://localhost:8545
```

Follows finalized chain tip by default, with per-chain finality resolution via `ChainProfile`. Configurable via `END_BLOCK=finalized|latest|<number>`.

**Required RPC methods:** `eth_blockNumber`, `eth_getBlockByNumber` (with full transactions), `eth_getBlockReceipts`. Block + receipts are fetched in a single JSON-RPC batch call.

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
| Worker crashes mid-task | Entry stays in Stream PEL; XAUTOCLAIM reclaims after idle timeout (default 2 min) |
| Worker crashes mid-block | PG transaction rolls back (no partial data). Block is re-processed on reclaim. Idempotent inserts handle duplicates |
| RPC returns 429 | Shared rate limiter cuts effective rate 25% across all workers (via Redis Lua). RPC client retries with exponential backoff |
| RPC endpoint goes down | Circuit breaker opens after 60% failure rate in 20-call ring buffer window. RPC pool fails over to next endpoint if available |
| Redis goes down | Workers crash (by design — Redis is the coordination backbone). On Redis restart, persisted data survives (AOF). PG watermark enables recovery |
| PostgreSQL goes down | Insert fails, task is requeued. Idempotent inserts handle re-processing when PG recovers |
| Duplicate blocks | `ON CONFLICT DO NOTHING` on all tables — safe to re-process any block |
| Block reorgs | Using `finalized` block tag (or depth-based finality per chain profile) avoids reorgs. `parentHash` chain check halts the task on discontinuity. Cross-task boundary verification uses block hash storage in Redis |
| Poison block (always fails) | Dead-letter queue stream after 5 retries |
| Slow shutdown | 25-second deadline timer forces exit before k8s SIGKILL (30s default grace period) |
| Write buffer data loss | Buffer flushed before task completion, before drain, and before shutdown. On flush failure, batch is restored to buffer for retry |

## What I'd Add for Production

**Reorg handling:** Index at `latest` with a configurable confirmation depth. On reorg detection, `DELETE` orphaned blocks from PG and re-index from the fork point.

**Storage tiering:** Hot data in PostgreSQL (recent partitions), cold data exported to Parquet on S3. Detach and drop PG partitions after successful upload.

**COPY protocol:** For historical backfill, swap staging-table INSERTs with `COPY FROM STDIN` via `pg-copy-streams`. 3-10x faster for bulk immutable data.

**HTTP connection pooling:** Configure undici `Agent` with `maxSockets` matched to the rate limiter ceiling.

**CI/CD:** GitHub Actions for lint/test/build. Helm chart for Kubernetes deployment with per-chain Deployments and configurable resource limits.

## Scale Considerations

Table partitioning by `block_number` range (1M blocks per partition). Read replicas to separate indexing write load from API query load. BRIN indexes on timestamp columns. B-tree on `topic0` for ERC-20 Transfer filtering. PgBouncer in docker-compose for connection multiplexing (2400 app connections → 100 PG connections). Redis Cluster hash tags (`{chainId}`) on all keys for sharding. Bitmap completion tracking (300 MB for 120 chains vs 153 GB with sorted sets).

## Token Terminal Context

This indexer represents the "ingestion" layer — analogous to TT's Go-based chain indexers that feed raw blockchain data into the pipeline. In the full pipeline:

```
Ingestion (this)  ->  ETL (dbt)  ->  OLAP (ClickHouse)  ->  Warehouse (BigQuery)
    PG (OLTP)          Transform       Sub-second queries     Historical analytics
```

At 120 chains, each chain runs as a separate indexer instance with chain-specific config (RPC endpoints, block times, finality rules via ChainProfile). The patterns implemented here — idempotent inserts, distributed rate limiting, Redis Streams work distribution, circuit breakers, adaptive backpressure, dead-letter queues, bitmap progress tracking — translate directly to that scale.
