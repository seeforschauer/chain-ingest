# chain-ingest

Distributed EVM blockchain ingestion service. Multiple worker processes coordinate through Redis to extract blocks, transactions, receipts, and logs from JSON-RPC endpoints into partitioned PostgreSQL.

```
              ┌───────────────────────────────┐
              │            Redis              │
              │  task stream · rate limiter   │
              │  completion bitmap            │
              └──────────────┬────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────▼─────┐
   │  Worker 1  │      │  Worker 2  │      │  Worker N  │
   └──┬──────┬──┘      └──┬──────┬──┘      └──┬──────┬──┘
      │      │            │      │            │      │
 ┌────▼──┐ ┌─▼────────┐  │      │            │      │
 │  PG   │ │ RPC Node  │◄─┘     │            │      │
 │       │ │           │◄────────┘            │      │
 └───────┘ └───────────┘◄─────────────────────┘─────┘
```

Built for scale: chain-scoped coordination (120+ chains), Redis Streams with consumer groups, bitmap completion tracking, BYTEA hash storage, EIP-1559/4844 schema, range-partitioned tables, adaptive rate limiting, circuit breaker, per-chain finality profiles, PgBouncer connection pooling, and graceful drain on SIGTERM.

## Quick start

```bash
# Infrastructure (from repo root)
docker compose up -d          # Redis + PostgreSQL + PgBouncer
cd anvil && docker compose up -d  # Local Anvil RPC

# Install, migrate, run
cd indexer
npm install
npm run migrate
npm start
```

## Running multiple workers

```bash
# Terminal 1 — seed queue and run first worker
npm start

# Terminal 2 — second worker on the same chain
WORKER_ID=worker-2 npm start

# Terminal 3 — separate chain (keys are chain-scoped with hash tags)
CHAIN_ID=137 RPC_URL=https://polygon-rpc.com npm start
```

Seed-only mode separates queue population from processing (Unix: different concerns, different lifecycles):

```bash
SEED_ONLY=true npm start     # Populate queue, exit
npm start                    # Workers drain independently
```

## Configuration

All configuration via environment variables — no config files to manage across deployments.

| Variable | Default | Description |
|----------|---------|-------------|
| `CHAIN_ID` | `1` | Chain identifier (scopes all Redis keys via `{chainId}` hash tags) |
| `RPC_URL` | `http://localhost:8545` | JSON-RPC endpoint |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `POSTGRES_URL` | `postgresql://indexer:indexer_password@localhost:5432/indexer` | PostgreSQL connection (via PgBouncer: port 6432) |
| `BATCH_SIZE` | `10` | Blocks per work unit |
| `RATE_LIMIT` | `50` | Max RPC requests/second (global, per chain) |
| `MAX_RETRIES` | `5` | RPC retry attempts per call |
| `START_BLOCK` | `0` | First block to index |
| `END_BLOCK` | `finalized` | Last block (`finalized`, `latest`, or number) — resolved per chain profile |
| `WORKER_ID` | auto-generated | Unique worker identifier |
| `SEED_ONLY` | `false` | Seed queue and exit |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `RPC_URLS` | (falls back to `RPC_URL`) | Comma-separated list of RPC endpoints for round-robin pool |
| `FLUSH_SIZE` | `1` | Write buffer: flush after N blocks (1 = no buffering) |
| `FLUSH_INTERVAL_MS` | `0` | Write buffer: periodic flush interval (0 = disabled) |
| `PG_POOL_MAX` | `20` | PostgreSQL connection pool size (reduce to 5 when using PgBouncer) |
| `METRICS_PORT` | (disabled) | Prometheus `/metrics` + `/healthz` HTTP port |

All numeric values are strictly validated (`Number()` + `Number.isInteger()`). No silent truncation from `parseInt`.

## Schema

Five tables in the `raw` schema, all data tables range-partitioned by `block_number` (1M blocks per partition). Hash and address columns use **BYTEA** (50% storage savings vs TEXT at scale). EIP-1559/4844 fields are included as nullable columns.

| Table | Partition key | Indexes | Notes |
|-------|--------------|---------|-------|
| `raw.blocks` | `block_number` | BRIN(timestamp) | EIP-4844: `blob_gas_used`, `excess_blob_gas` |
| `raw.transactions` | `block_number` | B-tree(from_address), B-tree(to_address), partial(tx_type) | EIP-1559: `max_fee_per_gas`, `max_priority_fee_per_gas`; EIP-4844: `max_fee_per_blob_gas`, `blob_versioned_hashes` |
| `raw.receipts` | `block_number` | — | `block_number` denormalized for co-partitioning |
| `raw.logs` | `block_number` | B-tree(address, topic0) | PK: `(block_number, tx_hash, log_index)` — natural composite, no BIGSERIAL |
| `raw.indexer_state` | — | — | Per-chain contiguous high-water mark (survives Redis flushes) |

All inserts are idempotent (`ON CONFLICT DO NOTHING`). Batch inserts are chunked at 5,000 rows to stay under PostgreSQL's 65,535 parameter limit. Logs are chunked independently from transactions (1:N fanout — DeFi transactions emit 5-50 logs each).

## Testing

```bash
npm test              # 137 tests, ~3s
npm run test:watch    # Watch mode
npm run typecheck     # TypeScript strict check
```

9 test files covering config validation (30), chain profile (13), coordinator logic (17), rate limiter (5), RPC client (15), storage (17), worker behavior (13), write buffer (7), and highload scenarios (9). All tests use in-memory mocks — no Redis or PostgreSQL required.

## Reliability

| Mechanism | Implementation |
|-----------|---------------|
| **Atomic task claiming** | `XREADGROUP` consumer groups — no double-assignment |
| **Write-before-delete** | All queue moves write destination first; crash = item in both queues (safe, deduplicated) |
| **Circuit breaker** | Ring buffer failure rate tracking pauses RPC after sustained failures, probes to detect recovery. 429s excluded — rate limiter handles those |
| **Adaptive rate limiting** | Lua sliding window. Cuts 25% on 429, recovers 10% after 10 healthy requests, floors at 1 req/s |
| **Stream-based reclaim** | XAUTOCLAIM reclaims idle entries — no heartbeat infrastructure needed |
| **Partial progress** | On error, marks completed blocks in bitmap before requeue — avoids redundant RPC on retry |
| **Graceful drain** | SIGTERM -> finish current block -> mark progress -> requeue remainder -> clean exit |
| **Idempotent storage** | `ON CONFLICT DO NOTHING` on all tables; safe to re-process any block |
| **RPC pool failover** | Round-robin across endpoints; per-endpoint circuit breaker; automatic failover on failure |
| **Write buffer** | Configurable batch flush with promise-chain serialization; flush-before-complete ordering |
| **Shared rate adaptation** | Throttle/recovery state stored in Redis via Lua — all workers reduce together on 429 |
| **Bounded Redis memory** | Bitmap: 1 bit per block (2.5 MB/chain vs 1.28 GB with sorted sets). No eviction needed |
| **Per-chain finality** | ChainProfile dispatches to tag/depth/instant finality per chain (ETH/Polygon/Arbitrum/BSC/etc.) |
| **Health check** | `GET /healthz` on metrics port for k8s liveness probes |

## Module map

| File | Responsibility |
|------|---------------|
| `main.ts` | Entry point, wiring, lifecycle, SEED_ONLY mode |
| `config.ts` | Strict env var parsing with boundary validation |
| `chain-profile.ts` | Per-chain finality semantics (tag/depth/instant), block times, RPC quirks |
| `coordinator.ts` | Redis Streams task distribution, bitmap completion tracking, XAUTOCLAIM stale reclaim |
| `worker.ts` | Task execution loop, drain, partial progress |
| `rpc.ts` | JSON-RPC batching, retry, circuit breaker, EIP-1559/4844 types |
| `rpc-pool.ts` | Round-robin multi-endpoint pool with per-endpoint circuit breaker failover |
| `rate-limiter.ts` | Distributed sliding window (Lua) with adaptive backpressure |
| `storage.ts` | PostgreSQL schema (BYTEA, EIP-1559/4844), partitioned tables, staging-table bulk path |
| `write-buffer.ts` | Accumulates blocks and flushes to storage in configurable batches |
| `metrics.ts` | Prometheus registry with counters, gauges, and histograms per worker |
| `metrics-server.ts` | HTTP server exposing `/metrics` for Prometheus scraping |
| `logger.ts` | Structured JSON logging, level filtering, stdout/stderr split |
| `migrate.ts` | Standalone migration script (`npm run migrate`) |

See [ARCHITECTURE.md](ARCHITECTURE.md) for design decisions and expert board review.
See [APPROACH.md](APPROACH.md) for failure scenarios and scale considerations.
See [REVIEW_CHECKLIST.md](REVIEW_CHECKLIST.md) for the distributed systems review checklist extracted from this project.
