# Prompt: Enterprise-Grade Distributed Indexer — Opus 4.6

**Model:** Claude Opus 4.6 (`claude-opus-4-6`)
**When to run:** After Docker infra is up and basic indexer works

---

## Prompt

```
Context: Token Terminal blockchain-homework-assignment is a distributed blockchain
indexer at ~/tt-homework/indexer/ using TypeScript, Redis (coordination/rate-limiting),
PostgreSQL (storage), and Anvil (local EVM RPC). The current setup uses a basic
sequential-insert-per-block approach, not batch/streaming pipelines. The codebase
has 6 source files in src/ (main, worker, coordinator, rpc, storage, rate-limiter,
config, logger). This is a hiring test for Token Terminal — a team that processes
1+ PB of transaction data daily across 120+ blockchains, serves Bloomberg/Morgan Stanley,
and values small-team-high-leverage engineering.

Task: Elevate this indexer from "homework that works" to "enterprise-ready system
that signals petabyte-scale thinking." The code must remain clean and within the
3-4 hour scope impression, but every design choice should demonstrate awareness of
what breaks at 120-chain, 4PB scale.

Approach: Use 3 parallel task agents (one per concern), then orchestrate.

---

I need work done across 3 repos areas today. Use separate task agents for each:

1) **Agent: Data Pipeline & Storage** (~/tt-homework/indexer/src/storage.ts + new files)
   Context: PostgreSQL storage is running on localhost using pg with Pool. The current
   setup uses individual INSERT per row, not batch/COPY/streaming inserts. The schema
   uses raw.* tables with basic B-tree indexes, not partitioned tables.
   Task: Make the storage layer enterprise-grade for petabyte-scale blockchain data.
   Approach: Apply these specific optimizations:

   - **Batch inserts**: Replace per-row INSERT with multi-row VALUES or pg COPY protocol
     for 10-50x throughput. Token Terminal processes millions of blocks — row-at-a-time
     is a bottleneck. Reference: Craig Kerstiens (Crunchy Data) batch insert patterns.
   - **Table partitioning**: Partition raw.blocks by block_number range (e.g., 1M blocks
     per partition). At 120 chains × millions of blocks, unpartitioned tables hit
     performance cliffs. Add raw.logs partitioning by block_number too (highest volume table).
   - **JSONB for extensibility**: Transaction input_data and log topics are already partially
     JSONB — ensure the schema supports efficient GIN indexing for event filtering
     (critical for Token Terminal's Uniswap/DeFi analytics).
   - **Connection pool tuning**: Size pool to match worker count, add statement_timeout
     and idle_in_transaction_session_timeout for resilience.
   - **BRIN indexes** on block_number columns (physically ordered data → BRIN is 100x
     smaller than B-tree, better for range scans on sequential blockchain data).
     Reference: Christophe Pettus (PostgreSQL Experts) on BRIN vs B-tree for time-series.
   - **Prepared statements** for hot-path inserts — avoid repeated query planning.
   - Add a `raw.indexer_state` table tracking per-chain watermarks (high_water_mark,
     last_indexed_at) — this is how production indexers track progress across restarts
     without relying solely on Redis.

   DO NOT change the file structure. Edit existing files. Keep it clean — no over-abstraction.

2) **Agent: Coordination & Performance** (~/tt-homework/indexer/src/coordinator.ts,
   rate-limiter.ts, worker.ts)
   Context: Redis coordination is running on localhost using ioredis. The current setup
   uses BRPOPLPUSH for task claiming, not Redis Streams. Rate limiter uses a sorted-set
   sliding window Lua script.
   Task: Harden the coordination layer for multi-chain, high-throughput production use.
   Approach: Apply these specific optimizations:

   - **Redis Streams** (XREADGROUP): Replace list-based queue with a consumer group stream.
     Streams give: automatic consumer tracking, pending entry list (PEL) for crash recovery,
     XACK for completion, XCLAIM for stale reclaim — all built-in, no custom Lua needed.
     Reference: Salvatore Sanfilippo's Redis Streams intro (antirez.com).
     HOWEVER: If the current BRPOPLPUSH approach is cleaner and more readable, keep it
     and add a comment explaining why Streams were considered but list queue was preferred
     (simplicity, assignment scope). The interviewer will value the awareness.
   - **Adaptive rate limiting**: Instead of fixed 50 req/s, implement adaptive backpressure:
     start at the configured rate, reduce on 429s, increase slowly when healthy. This is
     how production RPC clients work (Token Terminal hits rate limits across 120 chains).
     Reference: Google SRE Book — adaptive throttling (client-side).
   - **Pipeline RPC calls**: For blocks with many transactions, batch JSON-RPC requests
     (send multiple calls in one HTTP body). Anvil and most providers support this.
     This is a major throughput multiplier at scale.
   - **Worker heartbeat**: Add a periodic heartbeat to Redis (SET with EX) per worker.
     Stale task reclaim should check heartbeat, not just assignment timestamp — a slow
     task is different from a dead worker.
   - **Metrics**: Add in-memory counters exposed via log at intervals:
     blocks/sec, rpc_calls/sec, errors/sec, queue_depth. No Prometheus dependency needed —
     just structured JSON log lines that a production system would ship to Grafana.
   - **Graceful drain**: On SIGTERM, finish current block (not task) and exit cleanly.
     Don't requeue a half-done task — mark partial progress.

   Keep the code readable. Complexity should be in the design, not the syntax.

3) **Agent: RPC Client & Data Integrity** (~/tt-homework/indexer/src/rpc.ts + APPROACH.md)
   Context: RPC client is running on localhost:8545 using native fetch. The current setup
   uses individual RPC calls with retry, not batched JSON-RPC or streaming.
   Task: Make the RPC layer production-grade and update APPROACH.md to reflect
   enterprise thinking.
   Approach: Apply these specific optimizations:

   - **JSON-RPC batching**: Send multiple calls in a single HTTP request:
     `[{method: "eth_getBlockByNumber"}, {method: "eth_getBlockReceipts"}]`
     This halves network round-trips per block. At scale, this is the difference between
     keeping up with chain tip and falling behind.
   - **Data validation**: Verify block.hash matches expected (parentHash chain).
     At petabyte scale, silent data corruption is catastrophic. Add a lightweight
     integrity check: `block[N].parentHash === block[N-1].hash`.
   - **Reorg awareness**: Document (in APPROACH.md) how the system handles reorgs.
     Current approach: use "finalized" tag. But note: finalized is 12-15 minutes behind
     on Ethereum. For Token Terminal's real-time product, you'd index at "latest" with
     a reorg buffer (store last N block hashes, detect parentHash breaks, re-index).
     Implement the finalized approach but document the production approach.
   - **Request timeout tuning**: 30s is too generous for most calls. Use 10s for
     eth_blockNumber, 15s for block fetch, 30s for receipts (large blocks).
   - **Circuit breaker pattern**: After N consecutive failures, pause the worker
     briefly instead of burning through retries. Prevents cascading failures when
     RPC is down. Reference: Michael Nygard — "Release It!" circuit breaker pattern.
   - **Update APPROACH.md**: Add sections on:
     - "What I'd add for production" (multi-chain, reorg handling, monitoring, CI/CD)
     - "Scale considerations" (partitioning, read replicas, caching layer)
     - "Token Terminal context" (how this maps to their 120-chain pipeline)
     Keep it concise — 1 paragraph each. Show you understand their scale.

   Do NOT over-engineer the code. The goal is showing awareness, not building a framework.

---

## RULES FOR ALL AGENTS

### Quality bar
This is for Token Terminal — a team that:
- Processes 1+ PB daily across 120+ blockchains
- Uses Go for ingestion, ClickHouse for serving, BigQuery for warehouse
- Values Unix-philosophy tools (small, focused, composable)
- Has engineers from Google, Vercel, Amazon
- Jarmo (Co-CTO) built ZEIT Gateway and migrated from subgraphs to direct node ingestion

The code should look like it was written by someone who has operated data pipelines at
scale, not someone who read a tutorial. But it should ALSO look like a clean 3-4 hour
homework — not a bloated overengineered mess. The sweet spot: simple code with comments
that reveal deep knowledge.

### Comment style
Add concise comments where design choices reference production patterns:
```typescript
// Batch insert: at TT's 120-chain scale, row-at-a-time inserts are the bottleneck.
// COPY protocol would be ideal for bulk loads; multi-row VALUES is the pragmatic middle.
```
Don't add obvious comments. Only comment WHY, not WHAT.

### Evidence rule
Every optimization must reference a real technique used by a known engineer/team:
- Craig Kerstiens (Crunchy Data) — PostgreSQL batch patterns
- Salvatore Sanfilippo (antirez) — Redis Streams design
- Google SRE Book — adaptive throttling
- Michael Nygard — circuit breaker
- Token Terminal's own architecture (from public docs and Jarmo interview)

### Concurrency & file safety
- Agent 1 owns: storage.ts, migrate.ts
- Agent 2 owns: coordinator.ts, rate-limiter.ts, worker.ts
- Agent 3 owns: rpc.ts, APPROACH.md
- Shared: config.ts, logger.ts, main.ts — orchestrator edits these AFTER agents complete
- No agent touches another agent's files

### 30% context rule
If context reaches 30% remaining, stop and write current changes to disk immediately.

### After all 3 agents complete
Orchestrator pass:
1. Read all modified files
2. Update main.ts if new components need wiring
3. Update config.ts if new env vars were added
4. Run `npx tsc --noEmit` to verify clean compile
5. Report: what changed, what was kept simple, what comments signal scale awareness
```

---

## Execution Notes

- Run all 3 agents in parallel (`run_in_background: true`)
- Model: Opus 4.6 for all agents (this needs judgment, not speed)
- After agents complete, compile check + manual review before Docker test
- Total time budget for execution: ~15 minutes
- The goal is NOT to build a production system — it's to write homework that makes
  Jarmo think "this person has seen real scale"
