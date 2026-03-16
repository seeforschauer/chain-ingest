# Code Review Checklist — Distributed Systems

Applicable to any distributed data pipeline. 32 design decisions documented in ARCHITECTURE.md; this checklist covers the review methodology.

## 1. Operation Ordering in Distributed State

- [ ] **Write before delete**: When moving state between stores (e.g., queue A to queue B), always write to the destination FIRST, then remove from the source. If you crash between steps, the item appears in both (safe, deduplicated via idempotent writes) instead of neither (data loss).
  - Bad: `lrem(processing)` then `lpush(pending)` — crash between = task vanishes
  - Good: `lpush(pending)` then `lrem(processing)` — crash between = task in both queues, `isBlockCompleted` skips duplicates
  - Use `lpush` (not `rpush`) for requeue — places failed tasks at the back of the claim order, preventing poison blocks from starving healthy work
- [ ] **Apply the pattern uniformly**: If you fix this in `completeTask`, audit `requeueTask`, `reclaimStaleTasks`, and every other method that moves state between stores. Same bug class, same fix.

## 2. Resource Isolation (Multi-Tenant Keys)

- [ ] **Scope ALL shared-resource keys**: If Redis keys are scoped per tenant/chain, verify that EVERY key follows the convention — not just the obvious ones. A single global key in a sea of scoped keys breaks isolation silently.
- [ ] **Grep for hardcoded key prefixes**: After adding scoping, search for any remaining bare `"indexer:"` literals that lack the scope variable.

## 3. Strict Input Parsing

- [ ] **Never use `parseInt` for user-facing config**: `parseInt("10.5", 10)` silently returns `10`. Use `Number(val)` + `Number.isInteger()` for strict validation. Silent truncation is a production incident waiting to happen.
- [ ] **Apply validation consistently**: If you strict-validate `BATCH_SIZE`, also strict-validate `END_BLOCK`, `CHAIN_ID`, and every other numeric config. Inconsistency means one parser has a bug you already fixed in the others.

## 4. Unbounded Batch Sizes (PG and Redis)

- [ ] **Know your resource ceilings**: PostgreSQL max 65,535 parameters per query. Redis pipelines buffer all commands in Node.js memory before sending. Both will blow up on large inputs.
  - PG: Arbitrum 10K txs * 10 params = 100K — exceeds limit, worker stalls permanently. Fix: chunk at 5,000 rows.
  - Redis: 20M blocks / batch 10 = 2M pipeline commands ~ 500MB RAM. BSC (45M blocks) = OOM. Fix: chunk at 10,000 commands per pipeline.
- [ ] **Apply the fix at every layer**: If you chunk PG inserts, also chunk Redis pipelines. Same bug class, same fix. Audit every `pipeline()` and every multi-row INSERT for unbounded loops.
- [ ] **Chunk by OUTPUT row count, not INPUT row count**: When parent and child tables have 1:N relationships (transactions -> logs), chunking by parent count doesn't bound child count. A chunk of 5,000 txs can produce 50,000 logs on DeFi-heavy chains (Uniswap swaps emit 5-10 logs). Logs must be chunked independently.

## 5. Batch/RPC Response Validation

- [ ] **Filter and validate batch responses**: JSON-RPC batch responses may contain null entries, missing IDs, or fewer items than requested. Always filter, validate count, then sort — never assume response order or completeness.

## 6. Null Guards on External Data

- [ ] **Guard optional return values from external APIs**: If an RPC can return `null` for receipts (some providers do for pending blocks), guard with `?? []` or explicit null check. Don't rely on types alone — runtime data is untyped.

## 7. Hot-Path Performance

- [ ] **Pipeline Redis calls in loops**: Sequential `await redis.command()` inside a loop is O(N) round-trips. Pipeline all calls, execute once, iterate results. This is the single biggest perf win for Redis-heavy code.
- [ ] **Throttle periodic maintenance**: Operations like `reclaimStaleTasks` and `getStats` don't need to run every loop iteration. Time-gate them (e.g., every 30s) to avoid per-iteration Redis overhead.
- [ ] **Cache immutable lookups in memory**: If the answer doesn't change (e.g., partition existence after CREATE TABLE), cache it in a `Set` or `Map`. Don't query the database on every iteration to re-confirm what you already know.
- [ ] **Batch range queries instead of per-item lookups**: When checking multiple items against the same data source, fetch the full range once and check locally.
- [ ] **Share expensive fetches across multiple consumers**: If two code paths in the same scope both need the same data, fetch once and pass the result to both.
- [ ] **Mark partial progress on failure**: When a task partially completes (blocks 0-5 succeed, block 6 fails), mark progress before requeueing. Without this, the next worker re-processes all blocks from scratch (PG idempotent, but wastes RPC calls).

## 8. Chain Continuity / Data Integrity

- [ ] **Verify `parentHash` chain**: Within a task's block range, each block's `parentHash` should match the previous block's `hash`. Log violations — they indicate reorgs, RPC data corruption, or skipped blocks.
- [ ] **Use `finalized` block tag**: Avoids reorg handling entirely (at the cost of ~12-15 minute lag on Ethereum). Document the trade-off.

## 9. Numeric Precision

- [ ] **Use arbitrary-precision types for blockchain values**: EVM values can exceed 2^256. JavaScript loses precision above 2^53. Convert via `BigInt(hex).toString()` and store as PostgreSQL `NUMERIC`.
- [ ] **Never store hex values as JS `Number`**: Intermediate `parseInt(hex, 16)` is fine for block numbers (max ~2^32), but not for gas/value/balance fields.

## 10. Schema Design at Scale

- [ ] **Partition ALL data tables by range**: Every data table must be partitioned — not just blocks and logs. An unpartitioned transactions or receipts table grows without bound: can't drop old partitions, B-tree indexes bloat, range queries degrade.
- [ ] **Include partition key in all related tables**: Receipts need `block_number` even though it's "redundant" with the transaction. Without it, receipts can't be co-partitioned, can't be range-pruned, and are orphaned from the physical layout.
- [ ] **PK must include partition key**: PostgreSQL requires the partition key in PRIMARY KEY on partitioned tables. Use `(block_number, tx_hash)` not just `(tx_hash)`. Update `ON CONFLICT` clauses to match.
- [ ] **Drop foreign keys on partitioned tables**: FKs add write overhead and complicate partition management. When data is always inserted atomically in one transaction, referential integrity is guaranteed by the application.
- [ ] **Use `ON CONFLICT DO NOTHING` everywhere**: Workers may retry blocks. Idempotent inserts prevent duplicates without coordination overhead.

## 11. Unix Philosophy / Composability

- [ ] **Separate seeder from worker**: `SEED_ONLY=true` populates the queue and exits. Workers drain independently. Different scaling, different lifecycle, different failure domains.
- [ ] **Structured JSON logging with level filtering**: `LOG_LEVEL=debug|info|warn|error`. At 120 chains, unfiltered debug logs are a firehose. Programs write to stdout — aggregation is a separate concern.
- [ ] **Env-var config with strict defaults**: Every tunable exposed as an env var with a safe default. No config files to manage across 120 deployments.

## 12. Graceful Shutdown

- [ ] **Drain, don't kill**: On SIGTERM, finish the current unit of work (block), mark progress, requeue the remainder. Avoids partial state, duplicate work on restart, and lost progress tracking.
- [ ] **Clean up timers and connections**: Clear intervals, close pools, disconnect clients. Leaked timers prevent clean process exit.

## 13. Circuit Breaker

- [ ] **Don't burn retries when downstream is down**: After a high failure rate (e.g., 60% in a 20-call sliding window), open the circuit (reject immediately) for a cooldown period. Probe with a single call to detect recovery. Windowed tracking is more robust than consecutive counting — a single success should not reset the failure signal.
- [ ] **One unhealthy chain shouldn't impact others**: At multi-chain scale, circuit breakers prevent retry storms on one chain from consuming the error budget of the other 119.
- [ ] **429 is NOT an infrastructure failure**: HTTP 429 (rate limited) means the RPC is healthy but overloaded. The rate limiter handles 429s (cut rate 25%). The circuit breaker must NOT count 429s — false circuit opens at 120-chain scale cause cascading stalls where a throttled-but-healthy endpoint triggers 30s cooldown.
- [ ] **Use typed errors or flags, not string matching**: Distinguishing error classes by parsing `error.message` is fragile. Use a boolean flag scoped to the attempt, or a typed error class. Message text is for humans, not control flow.

## 14. Connection Lifecycle

- [ ] **Graceful disconnect — send QUIT before TCP close**: `redis.quit()` sends QUIT and waits for acknowledgment. `redis.disconnect()` drops TCP without handshake. At 120 chains x N workers, ungraceful disconnects leak server-side connection state and can exhaust Redis's connection tracking.
- [ ] **Pipeline independent Redis calls at every callsite**: If two Redis commands are independent of each other (e.g., `lrem` + `hdel` cleanup, or `hset` + `set` post-claim setup), pipeline them into a single round-trip. Audit every method for sequential `await redis.x(); await redis.y()` where neither depends on the other's result.

## 15. Test Coverage Patterns

- [ ] **Config boundary tests**: zero, negative, NaN, floats, valid, edge cases (startBlock=0)
- [ ] **Mock at the interface boundary**: Mock Redis/PG, not internal methods. Tests verify behavior, not implementation.
- [ ] **Test the failure paths**: Requeue on error, stale task reclaim, circuit breaker state transitions, drain mid-task

## 16. Systematic Bug-Class Auditing

- [ ] **After fixing any bug, grep for the same pattern across the entire codebase**: The write-before-delete bug can appear in multiple callsites. Fixing it in one place while missing three others is worse than not fixing it at all — it shows you understood the problem but didn't apply it systematically.
  - Method: `grep lrem` (or whatever the operation is) -> audit every callsite
- [ ] **Same principle for scoping/isolation**: After scoping one Redis key by chainId, grep for all remaining bare key literals.
- [ ] **Same principle for strict parsing**: After fixing `parseInt` -> `Number()` in one parser, audit every other `parseInt` in config.

## 17. Shared State in Multi-Worker Systems

- [ ] **Local counters diverge under concurrency**: If a counter controls shared behavior (rate recovery, circuit breaker state), it must live in the shared store (Redis), not per-worker memory. N workers with local counters = N independent views of the same system.
- [ ] **Read shared state inside atomic scripts**: Don't pass cached local values as arguments to Lua scripts. Read from Redis inside the script: `redis.call('GET', key)`. One extra GET per call, but all workers agree on the same value.
- [ ] **Use redis.time() for timestamps, not Date.now()**: Workers on different machines have skewed clocks. Redis's monotonic clock is the single source of truth for stale detection, heartbeats, and rate-limit windows.

## 18. Unbounded Redis Memory

- [ ] **Every Redis structure must have an eviction path**: Sorted sets, hashes, and lists that grow with data volume will OOM Redis. For each structure: what triggers eviction? Is eviction paired with the data lifecycle?
- [ ] **Hash eviction via HSCAN+HDEL**: When evicting a sorted set (ZREMRANGEBYSCORE), also evict any associated hash entries in the same watermark range. Forgetting one half creates asymmetric growth.
- [ ] **Redis Cluster compatibility**: Multi-key Lua scripts require all keys to hash to the same slot. Use hash tags: `indexer:{chainId}:*` where `{chainId}` is the tag.

## 19. Database Schema Decisions at Scale

- [ ] **Natural composite PK over synthetic BIGSERIAL**: If the data has a natural unique key (block_number + tx_hash + log_index), use it as PK. BIGSERIAL adds: sequence contention, non-deterministic IDs on re-index, and double index maintenance.
- [ ] **Composite indexes for dominant query patterns**: If 90% of queries filter on (A, B), a composite index `(A, B)` is one scan instead of bitmap merge on two single-column indexes.
- [ ] **Don't index what partitioning already handles**: BRIN on the partition key inside a range-partitioned table is redundant — partition pruning already restricts the scan. Save the write amplification.
- [ ] **Filter pg_class by schema**: `SELECT FROM pg_class WHERE relname = ...` without a pg_namespace join matches tables in ANY schema. In multi-schema deployments, this silently skips partition creation.
- [ ] **escapeIdentifier on all DDL**: Table names interpolated into DDL strings are injection vectors. Use `escapeIdentifier` from pg, even if current inputs are controlled — it costs nothing and prevents future regressions.

## 20. RPC Resilience

- [ ] **Receipt method fallback**: `eth_getBlockReceipts` is non-standard. When it returns -32601, fall back to per-tx `eth_getTransactionReceipt`. Cache the capability flag per client — probe once, not per block.
- [ ] **Chunk large RPC batches**: L2 blocks can have 1000+ transactions. Per-tx receipt fallback must chunk (e.g., 100/batch) to stay within provider limits.
- [ ] **Explicit finality fallback**: Only catch -32601 for method-not-found. All other errors (timeout, 500) must fail loudly — a silent fallback from `finalized` to `latest` permanently degrades data guarantees.
- [ ] **Redact credentials in logs**: RPC URLs often contain API keys in the path. Log `new URL(url).host` only.

## 21. O(1) Hot-Path Data Structures

- [ ] **Ring buffer over array for sliding windows**: `Array.shift()` is O(n), `Array.filter()` allocates. A fixed-size ring buffer with head pointer and incremental counter is O(1) for both insert and query. At 6000+ RPC calls/sec across 120 chains, this matters.
- [ ] **Rate recovery must make progress at floor**: `math.floor(1 * 1.10) = 1` — recovery stuck forever. Use `math.max(current + 1, math.ceil(current * increase))` to guarantee at least +1 per recovery cycle.

## 22. Docker & DevOps

- [ ] **Healthchecks must use tools available in the image**: Minimal images (Alpine, distroless, Foundry) don't have `curl`. Use image-native tools (`cast` for Foundry, `pg_isready` for Postgres, `redis-cli ping` for Redis).
- [ ] **Env vars in healthchecks**: `pg_isready -U ${POSTGRES_USER:-indexer}`, not hardcoded `pg_isready -U indexer`.

## 23. Documentation as Present State

- [ ] **Describe what the code DOES, not what was fixed**: Readers care about current behavior. "The watermark tracks contiguous completeness" — not "BUG 1 was found and fixed."
- [ ] **Every design decision gets a "Why?"**: If you made a choice, document the alternative you rejected and why. 32 is better than 10 — thoroughness signals depth.
- [ ] **Self-review culture**: Score your own work honestly. Document what breaks at scale. The delta between current state and production-ready is more valuable than claiming everything is perfect.
