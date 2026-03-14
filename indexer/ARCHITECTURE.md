# Architecture: chain-ingest

A high-level guide for humans reading or extending this codebase.

## System Overview

chain-ingest extracts raw blockchain data (blocks, transactions, receipts, logs) from EVM-compatible nodes via JSON-RPC, and stores it in PostgreSQL. Multiple worker processes coordinate through Redis to divide work, handle failures, and avoid duplicate effort.

```
                 ┌─────────────────────────────┐
                 │          Redis               │
                 │  ┌───────────┐ ┌───────────┐ │
                 │  │ Work Queue│ │Rate Limiter│ │
                 │  │ (lists)   │ │(sorted set)│ │
                 │  └───────────┘ └───────────┘ │
                 │  ┌───────────┐ ┌───────────┐ │
                 │  │ Heartbeats│ │ Completed  │ │
                 │  │ (keys+TTL)│ │  (zset)    │ │
                 │  └───────────┘ └───────────┘ │
                 └──────────┬──────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
    ┌─────▼─────┐     ┌────▼──────┐    ┌─────▼─────┐
    │  Worker 1  │     │  Worker 2  │    │  Worker N  │
    │            │     │            │    │            │
    │ coordinator│     │ coordinator│    │ coordinator│
    │ rpc client │     │ rpc client │    │ rpc client │
    │ storage    │     │ storage    │    │ storage    │
    └──┬─────┬───┘     └──┬─────┬───┘    └──┬─────┬───┘
       │     │            │     │            │     │
  ┌────▼──┐ ┌▼────────┐  │     │            │     │
  │  PG   │ │ RPC Node│◄─┘     │            │     │
  │       │ │ (Anvil) │◄───────┘            │     │
  │       │ │         │◄────────────────────┘     │
  └───────┘ └─────────┘◄─────────────────────────┘
```

## Module Map

| File | Responsibility | Key Abstraction |
|------|---------------|-----------------|
| `main.ts` | Entry point, wiring, graceful shutdown, seed-only mode | Process lifecycle |
| `config.ts` | Env var parsing with strict validation | `Config` interface |
| `coordinator.ts` | Chain-scoped Redis work distribution | `Coordinator` class |
| `worker.ts` | Task execution loop with chain awareness | `Worker` class |
| `rpc.ts` | JSON-RPC client with retry + circuit breaker | `RpcClient` class |
| `rate-limiter.ts` | Distributed sliding window rate limiter | `RateLimiter` interface |
| `storage.ts` | PostgreSQL schema, chunked batch inserts, partitioning | `Storage` class |
| `logger.ts` | Structured JSON logging with level filtering | `log()` + `setLogLevel()` |
| `migrate.ts` | Standalone migration script | — |

## Data Flow

```
1. Worker claims task from Redis (BRPOPLPUSH — atomic)
2. For each block in the task range:
   a. Check if block already completed (Redis ZSCORE)
   b. Rate-limit check (Lua script — atomic across workers)
   c. Fetch block + receipts in single HTTP request (JSON-RPC batch)
   d. Verify parentHash chain continuity
   e. Batch INSERT into PostgreSQL (one multi-row INSERT per table)
3. Mark task complete (LREM from processing, ZADD completed blocks)
4. Update high-water mark in PostgreSQL
```

## Design Decisions & Reasoning

### Why Redis Lists over Streams?

Redis Streams (`XREADGROUP`) would provide consumer groups, pending entry lists (PEL) for crash recovery, and `XACK` for completion — ideal for Token Terminal's 120-chain scale. For this scope, `BRPOPLPUSH` gives atomic task claiming with simpler semantics and fewer moving parts. The trade-off: we manage crash recovery ourselves via the stale task reclamation loop rather than relying on Stream's built-in PEL.

### Why Chain-Scoped Redis Keys?

All Redis keys include the chain ID: `indexer:{chainId}:queue:pending`, `indexer:{chainId}:completed_blocks`, etc. Without this, deploying a second chain stomps the first chain's queue — a non-starter at 120-chain scale. Each chain is an independent coordination domain sharing the same Redis instance.

### Why Separate TASK_META from Queue Entries?

The coordinator stores assignment metadata (worker ID, timestamp) in a Redis hash rather than modifying the queue entry in `QUEUE_PROCESSING`. This is critical because `LREM` matches by exact string equality — if we mutated the JSON after `BRPOPLPUSH`, we'd need to track both the original and modified forms. Keeping queue entries immutable makes `LREM` deterministic.

### Why BRIN Indexes Instead of B-tree?

Blockchain data arrives in natural block-number order, which means `block_number` columns are physically correlated with row insertion order on disk. BRIN (Block Range Index) exploits this correlation — it stores min/max values per range of physical pages instead of indexing every row. Result: ~100x smaller index with comparable scan performance for range queries. B-tree is reserved for random-access patterns like address lookups.

### Why NUMERIC for Gas/Value Columns?

EVM values (gas, wei amounts) can exceed 2^256. JavaScript's `Number` loses precision above 2^53 (9 quadrillion). We convert via `BigInt(hex).toString()` and store as PostgreSQL `NUMERIC`, which handles arbitrary precision. This avoids silent data corruption at the storage layer.

### Why Partitioned Tables?

At Token Terminal's scale (120 chains, years of history), unpartitioned tables grow without bound. All four data tables (`raw.blocks`, `raw.transactions`, `raw.receipts`, `raw.logs`) are range-partitioned by `block_number` (1M blocks per partition), which lets PostgreSQL:
- **Prune partitions** — queries on recent blocks only scan recent partitions
- **Drop old data** — detach a partition instead of `DELETE FROM` (instant vs. hours)
- **Parallelize scans** — each partition can be scanned by a separate worker process

Receipts include `block_number` explicitly (denormalized from the transaction) so they can be co-partitioned with the other tables. Without it, receipts would be orphaned from the physical data layout — no range pruning, no partition drops, no efficient joins with partitioned blocks.

Primary keys on partitioned tables must include the partition key: `(block_number, tx_hash)` instead of just `(tx_hash)`. This is a PostgreSQL requirement, and `ON CONFLICT` clauses reference the full PK.

Foreign keys were intentionally dropped on partitioned tables — they add write overhead and complicate partition management, with minimal benefit when data is always inserted atomically in a single transaction.

### Why a Circuit Breaker?

When an RPC endpoint goes down, retrying aggressively wastes the rate limit budget and generates log noise. The circuit breaker (Michael Nygard, "Release It!") pauses all calls for a cooldown period after N consecutive failures, then probes with a single call to detect recovery. At 120-chain scale, this prevents one unhealthy chain from impacting the retry budget of the other 119.

Critically, the circuit breaker does **not** count HTTP 429 (rate limited) responses as failures. A 429 means the RPC is healthy but overloaded — the rate limiter handles this by cutting the effective rate 25%. Counting 429s toward the circuit breaker would cause false circuit opens at 120-chain scale, creating cascading stalls where a healthy-but-throttled endpoint triggers a 30-second cooldown. The distinction is tracked via a scoped boolean flag (not string matching on error messages, which is fragile).

### Why Adaptive Rate Limiting?

A fixed rate limit can't respond to provider-side throttling. The adaptive approach (inspired by Google SRE's client-side throttling):
- Drops effective rate by 25% on each 429 response
- Recovers 10% after 10 consecutive healthy requests
- Never exceeds the configured ceiling
- Floors at 1 req/s to avoid stalling

The Lua script ensures atomicity across workers — check-and-increment happens in a single Redis operation, so the per-chain budget is respected even with N concurrent workers. The rate limiter key is chain-scoped (`indexer:{chainId}:rate_limit`) alongside all other Redis keys — each chain has its own RPC endpoint with its own provider-imposed limit.

### Why Write-Before-Delete in Queue Moves?

Every operation that moves a task between queues follows the same discipline: **write to the destination first, then remove from the source**. If the process crashes between steps, the task appears in both queues (safe — idempotent consumers skip duplicates via `isBlockCompleted`). The opposite order (delete first, then write) loses the task entirely on crash — silent data loss with no recovery path.

This applies uniformly across all four queue-move callsites: `completeTask` (zadd → lrem), `requeueTask` (rpush → lrem), and both reclaim paths in `reclaimStaleTasks` (rpush → lrem).

### Why Immutable Queue + Requeue on Failure?

Failed tasks are requeued rather than retried in-place. This has three benefits:
1. **Different worker** — the task may succeed on a worker with a different network path or timing
2. **No head-of-line blocking** — other tasks continue while the failed one waits in the queue
3. **Natural backoff** — the task sits in the queue while other work proceeds, creating implicit delay

### Why Heartbeats for Stale Task Detection?

A task's timestamp alone can't distinguish "worker is slow" from "worker crashed." The heartbeat mechanism (Redis key with TTL) provides a definitive liveness signal. A task is only reclaimed if **both** conditions hold: the assignment timestamp exceeds the stale threshold AND the worker's heartbeat has expired. This prevents stealing work from slow-but-alive workers processing large blocks.

### Why Graceful Drain Instead of Force-Kill?

On `SIGTERM`, the worker finishes indexing its current block, marks completed blocks in the sorted set, and requeues the remaining portion of the task. This avoids:
- Partial block data in PostgreSQL (mid-transaction rollback handles this, but drain is cleaner)
- Duplicate work on restart (already-completed blocks are skipped)
- Lost progress tracking (sorted set accurately reflects what was indexed)

### Why Seed-Only Mode?

Unix philosophy: the seeder and workers are different concerns. `SEED_ONLY=true` populates the queue and exits. Workers then drain it independently. In production, the seeder might run as a Kubernetes Job while workers are a Deployment. Separating them means you can re-seed without restarting workers, and scale workers without re-seeding.

### Why Cache Partition Existence?

`ensurePartition` was querying `pg_class` on every block insert. With 20M blocks across ~20 partitions, that's 19,999,980 redundant catalog queries. The `knownPartitions` Set caches the answer after the first check or creation — turning O(N) catalog round-trips into O(partitions). At 520 GB/sec sustained, every per-block overhead is multiplied millions of times.

### Why Batch Completion Checks?

The worker used to call `isBlockCompleted` (one Redis `ZSCORE`) per block in a task. For a 10-block task, that's 10 Redis round-trips. `getCompletedBlocksInRange` fetches the full set with a single `ZRANGEBYSCORE`, and the worker checks membership locally via `Set.has()`. Same principle as pipelining Redis calls in loops — never do N round-trips when 1 will do.

### Why Mark Partial Progress on Error?

When a task partially completes (blocks 0-5 succeed, block 6 fails), the worker marks blocks 0-5 as completed in the sorted set before requeueing the task. Without this, the next worker re-processes all blocks from scratch — PG inserts are idempotent (`ON CONFLICT DO NOTHING`), so there's no data corruption, but the wasted RPC calls add up. At 45 PB/day across 120 chains, even a 1% error rate means thousands of redundant RPC fetches per hour. The fix is one `markBlocksCompleted` call in the error handler — same primitive the drain path already uses.

### Why Throttle Stats Reporting?

`getStats()` makes 3 Redis calls (`LLEN` + `LLEN` + `ZCARD`). Calling it after every task completion is pure overhead at scale. Time-gating it to every 30 seconds (same pattern as `reclaimStaleTasks`) trades per-task precision for bounded Redis load.

### Why Chunked Batches (PG and Redis)?

The same principle applies at two layers:

**PostgreSQL**: max 65,535 parameters per query. L2s like Arbitrum produce 10K-tx blocks. At 10 params/row, that's 100K parameters — exceeding the limit. Chunked at 5,000 rows per INSERT.

**Redis pipelines**: ioredis buffers all pipeline commands in memory before sending. For Ethereum mainnet (20M blocks, batch 10), an unchunked pipeline builds 2M commands ≈ 500MB RAM. For BSC (45M blocks), ~1.2GB — OOM on default Node.js heap. Pipeline execution is chunked at 10,000 commands per round-trip (~5MB), trading 200 round-trips for bounded memory.

Same bug class, same fix pattern: never assume a batch fits in memory.

## Testing Strategy

Tests use in-memory mocks (no Redis or PostgreSQL required) to verify:
- **Config validation** — boundary conditions (zero, negative, NaN)
- **Coordinator logic** — task claiming, completion, requeue, stale reclamation, heartbeats
- **Rate limiter** — throttle/recovery cycle, floor behavior
- **RPC client** — retry logic, circuit breaker state machine, batch parsing, null guards
- **Storage** — SQL construction patterns, idempotency, hex conversion precision
- **Worker** — task processing flow, drain behavior, error handling

Integration tests (against real Redis/PG) would live in a separate `tests/integration/` directory, run in CI with Docker Compose.

## Token Terminal Context

This indexer is the "ingestion" layer — analogous to TT's Go-based chain indexers. In the full pipeline:

```
Ingestion (this)  →  ETL (dbt)  →  OLAP (ClickHouse)  →  Warehouse (BigQuery)
    PG (OLTP)          Transform       Sub-second queries     Historical analytics
```

At 120 chains, each chain runs as a separate indexer instance with chain-specific config (RPC endpoints, block times, finality rules). The patterns here — idempotent inserts, distributed rate limiting, work queues, circuit breakers — translate directly to that scale.
