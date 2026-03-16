# Testing Guide

## 1. Unit tests (no infrastructure needed)

```bash
cd indexer
npm install
npm test              # 121 tests, ~3s
npm run typecheck     # TypeScript strict check
```

## 2. Integration test with Docker (Redis + PostgreSQL + Anvil)

**What it tests:** Full pipeline end-to-end — RPC fetch, PG insert, Redis coordination, schema migration.
**Critical:** Proves the system actually indexes real blocks, not just passes mocked tests.

```bash
# Start infrastructure from repo root
docker compose up -d              # Redis + PostgreSQL
cd anvil && docker compose up -d  # Local Anvil RPC node
cd ..

# Run migration
cd indexer
npm run migrate

# Single worker — basic end-to-end
npm start

# Verify data landed in PostgreSQL
docker exec -it $(docker ps -q -f name=postgres) psql -U indexer -c \
  "SELECT count(*) FROM raw.blocks; SELECT count(*) FROM raw.transactions;"
```

**Expected:** Block count grows. Transaction count > 0. No errors in logs.

## 3. Multi-worker test

**What it tests:** Work distribution — two workers claim different tasks, no block duplication.
**Critical:** Proves BLMOVE atomic claiming prevents duplicate work. Validates the coordination layer works under concurrency.

```bash
# Terminal 1 — seed + worker 1
SEED_ONLY=true npm start
npm start

# Terminal 2 — worker 2
WORKER_ID=worker-2 npm start

# Check both workers processed blocks (look at indexed_by column)
docker exec -it $(docker ps -q -f name=postgres) psql -U indexer -c \
  "SELECT indexed_by, count(*) FROM raw.blocks GROUP BY indexed_by;"
```

**Expected:** Two rows in output — each worker indexed different blocks. Sum equals total blocks. No block_number appears twice.

## 4. Multi-RPC endpoint pool

**What it tests:** RPC pool round-robin — requests distributed across endpoints. Circuit breaker per endpoint.
**Critical:** Proves the system handles multi-endpoint failover. If one endpoint dies, the other takes over.

```bash
# Two Anvil instances (or same URL twice for testing round-robin)
RPC_URLS=http://localhost:8545,http://localhost:8545 npm start
```

**Expected:** Logs show alternating RPC endpoints. No errors.

## 5. Write buffer (batch mode)

**What it tests:** Buffered writes — blocks accumulate in memory, flush to PG in batches.
**Critical:** Proves the write buffer reduces PG round-trips (50 blocks per flush instead of 1). Validates flush-on-threshold and flush-on-timer.

```bash
# Buffer 50 blocks before flushing to PG
FLUSH_SIZE=50 FLUSH_INTERVAL_MS=5000 npm start
```

**Expected:** Logs show "Flushed N blocks" every ~50 blocks or every 5s. PG block count increases in jumps of 50, not 1-by-1.

## 6. Prometheus metrics

**What it tests:** Observability — all counters, gauges, histograms exposed at `/metrics`.
**Critical:** Proves monitoring works. In production, this is how you detect stalls, rate-limit pressure, and PG latency.

```bash
METRICS_PORT=9191 npm start
# In another terminal:
curl http://localhost:9191/metrics
```

**Expected:** Prometheus text output with `blocks_indexed_total`, `rpc_duration_seconds`, `rate_limiter_effective_rate`, `queue_pending`, etc.

## 7. Crash recovery

**What it tests:** Worker crash mid-task — stale task reclamation picks up the unfinished work.
**Critical:** Proves the system recovers from hard kills without data loss or duplication. Validates heartbeat expiry + stale reclaim + ON CONFLICT DO NOTHING idempotency.

```bash
# Start worker, let it process a few blocks, then kill -9
npm start &
sleep 5
kill -9 $!

# Restart — resumes from where it left off
npm start

# Verify: no duplicate blocks in PostgreSQL
docker exec -it $(docker ps -q -f name=postgres) psql -U indexer -c \
  "SELECT block_number, count(*) FROM raw.blocks GROUP BY block_number HAVING count(*) > 1;"
```

**Expected:** Zero rows returned (no duplicates). Worker resumes from the last completed block, not from scratch.

## 8. Graceful drain (SIGTERM)

**What it tests:** Clean shutdown — finishes current block, flushes buffer, marks progress, requeues remainder.
**Critical:** Proves Kubernetes pod termination doesn't lose data. Validates the drain flag, buffer flush, retryCount=0 on requeue.

```bash
npm start &
sleep 3
kill $!    # sends SIGTERM
# Logs show "Drain requested" then "Worker stopped"
```

**Expected:** Logs show: "Drain requested — finishing current block" → "Flushed" → "Worker stopped". No error. Next worker picks up remaining blocks.

## 9. Seed-only mode

**What it tests:** Queue population without processing — Unix philosophy separation of concerns.
**Critical:** Proves the seeder and workers are independent. Seeder can run as K8s Job, workers as Deployment.

```bash
SEED_ONLY=true npm start
# Seeds the Redis queue and exits immediately
# Then start workers separately:
npm start
```

**Expected:** Seed logs "Queue seeded" and exits with code 0. Worker picks up tasks from the pre-populated queue.

## 10. Verbose logging

**What it tests:** Debug visibility — per-block indexing, rate limiter state, flush events, circuit breaker transitions.
**Critical:** For debugging in development. Not for production (too much output at 120 chains).

```bash
LOG_LEVEL=debug npm start
# Shows per-block indexing, rate limiter state, flush events
```

**Expected:** Verbose JSON logs for every block, every rate-limit acquire, every flush.

## Cleanup between test runs

```bash
# Reset PostgreSQL (drop and recreate tables)
docker exec -it $(docker ps -q -f name=postgres) psql -U indexer -c \
  "DROP SCHEMA IF EXISTS raw CASCADE;"
npm run migrate

# Reset Redis (flush all keys)
docker exec -it $(docker ps -q -f name=redis) redis-cli FLUSHALL
```

## Quick verification commands

```bash
# Block count + tx count
docker exec indexer-postgres psql -U indexer -c \
  "SELECT count(*) as blocks FROM raw.blocks; SELECT count(*) as txs FROM raw.transactions;"

# Redis queue stats
docker exec indexer-redis redis-cli LLEN indexer:1:queue:pending
docker exec indexer-redis redis-cli LLEN indexer:1:queue:processing
docker exec indexer-redis redis-cli ZCARD indexer:1:completed_blocks

# Check for duplicates (should return 0 rows)
docker exec indexer-postgres psql -U indexer -c \
  "SELECT block_number, count(*) FROM raw.blocks GROUP BY block_number HAVING count(*) > 1;"

# Check which workers indexed what
docker exec indexer-postgres psql -U indexer -c \
  "SELECT indexed_by, count(*) FROM raw.blocks GROUP BY indexed_by;"

# Watermark
docker exec indexer-postgres psql -U indexer -c \
  "SELECT * FROM raw.indexer_state;"
```
