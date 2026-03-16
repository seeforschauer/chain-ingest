# Testing Guide

## 1. Unit tests (no infrastructure needed)

```bash
cd indexer
npm install
npm test              # 89 tests, ~3s
npm run typecheck     # TypeScript strict check
```

## 2. Integration test with Docker (Redis + PostgreSQL + Anvil)

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

## 3. Multi-worker test

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

## 4. Multi-RPC endpoint pool

```bash
# Two Anvil instances (or same URL twice for testing round-robin)
RPC_URLS=http://localhost:8545,http://localhost:8545 npm start
```

## 5. Write buffer (batch mode)

```bash
# Buffer 50 blocks before flushing to PG
FLUSH_SIZE=50 FLUSH_INTERVAL_MS=5000 npm start
```

## 6. Prometheus metrics

```bash
METRICS_PORT=9090 npm start
# In another terminal:
curl http://localhost:9090/metrics
```

## 7. Crash recovery

```bash
# Start worker, let it process a few blocks, then kill -9
npm start &
sleep 5
kill -9 $!

# Restart — should resume from where it left off
npm start

# Verify: no duplicate blocks in PostgreSQL
docker exec -it $(docker ps -q -f name=postgres) psql -U indexer -c \
  "SELECT block_number, count(*) FROM raw.blocks GROUP BY block_number HAVING count(*) > 1;"
```

## 8. Graceful drain (SIGTERM)

```bash
npm start &
sleep 3
kill $!    # sends SIGTERM
# Logs should show "Drain requested" then "Worker stopped"
```

## 9. Seed-only mode

```bash
SEED_ONLY=true npm start
# Should seed the Redis queue and exit immediately
# Then start workers separately:
npm start
```

## 10. Verbose logging

```bash
LOG_LEVEL=debug npm start
# Shows per-block indexing, rate limiter state, flush events
```

## Cleanup between test runs

```bash
# Reset PostgreSQL (drop and recreate tables)
docker exec -it $(docker ps -q -f name=postgres) psql -U indexer -c \
  "DROP SCHEMA IF EXISTS raw CASCADE;"
npm run migrate

# Reset Redis (flush all keys)
docker exec -it $(docker ps -q -f name=redis) redis-cli FLUSHALL
```
