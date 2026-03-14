# chain-ingest

Distributed EVM blockchain ingestion service. Multiple worker processes coordinate through Redis to extract blocks, transactions, receipts, and logs from JSON-RPC endpoints into partitioned PostgreSQL.

```
              ┌───────────────────────────────┐
              │            Redis              │
              │  work queue · rate limiter    │
              │  heartbeats · completion set  │
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

Built for scale: chain-scoped coordination (120+ chains), range-partitioned tables, adaptive rate limiting, circuit breaker, and graceful drain on SIGTERM.

## Quick start

```bash
# Infrastructure (from repo root)
docker compose up -d          # Redis + PostgreSQL
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

# Terminal 3 — separate chain (keys are chain-scoped)
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
| `CHAIN_ID` | `1` | Chain identifier (scopes all Redis keys) |
| `RPC_URL` | `http://localhost:8545` | JSON-RPC endpoint |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `POSTGRES_URL` | `postgresql://indexer:indexer_password@localhost:5432/indexer` | PostgreSQL connection |
| `BATCH_SIZE` | `10` | Blocks per work unit |
| `RATE_LIMIT` | `50` | Max RPC requests/second (global, per chain) |
| `MAX_RETRIES` | `5` | RPC retry attempts per call |
| `START_BLOCK` | `0` | First block to index |
| `END_BLOCK` | `finalized` | Last block (`finalized`, `latest`, or number) |
| `WORKER_ID` | auto-generated | Unique worker identifier |
| `SEED_ONLY` | `false` | Seed queue and exit |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

All numeric values are strictly validated (`Number()` + `Number.isInteger()`). No silent truncation from `parseInt`.

## Schema

Five tables in the `raw` schema, all data tables range-partitioned by `block_number` (1M blocks per partition):

| Table | Partition key | Indexes | Notes |
|-------|--------------|---------|-------|
| `raw.blocks` | `block_number` | BRIN(timestamp) | Stores `indexed_by` for debugging |
| `raw.transactions` | `block_number` | BRIN(block_number), B-tree(from, to) | PK: `(block_number, tx_hash)` |
| `raw.receipts` | `block_number` | BRIN(block_number) | `block_number` denormalized for co-partitioning |
| `raw.logs` | `block_number` | BRIN(block_number), B-tree(address), GIN(topics) | Highest volume table |
| `raw.indexer_state` | — | — | Per-chain high-water mark (survives Redis flushes) |

All inserts are idempotent (`ON CONFLICT DO NOTHING`). Batch inserts are chunked at 5,000 rows to stay under PostgreSQL's 65,535 parameter limit. Logs are chunked independently from transactions (1:N fanout — DeFi transactions emit 5-50 logs each).

## Testing

```bash
npm test              # 67 tests, ~3s
npm run test:watch    # Watch mode
npm run typecheck     # TypeScript strict check
```

7 test files covering config validation (18), coordinator logic (13), rate limiter (5), RPC client (8), storage (6), worker behavior (8), and highload scenarios (9).

All tests use in-memory mocks — no Redis or PostgreSQL required.

## Reliability

| Mechanism | Implementation |
|-----------|---------------|
| **Atomic task claiming** | `BRPOPLPUSH` — no double-assignment |
| **Write-before-delete** | All queue moves write destination first; crash = item in both queues (safe, deduplicated) |
| **Circuit breaker** | Pauses RPC after N failures, probes to detect recovery. 429s excluded — rate limiter handles those |
| **Adaptive rate limiting** | Lua sliding window. Cuts 25% on 429, recovers 10% after 10 healthy requests, floors at 1 req/s |
| **Heartbeat-aware reclaim** | Stale tasks only reclaimed when worker heartbeat has expired (not just old timestamp) |
| **Partial progress** | On error, marks completed blocks before requeue — avoids redundant RPC on retry |
| **Graceful drain** | SIGTERM → finish current block → mark progress → requeue remainder → clean exit |
| **Idempotent storage** | `ON CONFLICT DO NOTHING` on all tables; safe to re-process any block |

## Module map

| File | Responsibility |
|------|---------------|
| `main.ts` | Entry point, wiring, lifecycle, SEED_ONLY mode |
| `config.ts` | Strict env var parsing with boundary validation |
| `coordinator.ts` | Chain-scoped Redis work distribution |
| `worker.ts` | Task execution loop, drain, partial progress |
| `rpc.ts` | JSON-RPC batching, retry, circuit breaker |
| `rate-limiter.ts` | Distributed sliding window (Lua) with adaptive backpressure |
| `storage.ts` | PostgreSQL schema, partitioned tables, chunked batch inserts |
| `logger.ts` | Structured JSON logging, level filtering, stdout/stderr split |

See [ARCHITECTURE.md](ARCHITECTURE.md) for design decisions and rationale.
See [APPROACH.md](APPROACH.md) for failure scenarios and scale considerations.
