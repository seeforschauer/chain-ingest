// Work distribution via Redis Streams, completion tracking via Bitmaps.
//
// Redis Streams provide: consumer groups (XREADGROUP), pending entry tracking
// (XPENDING), automatic stale reclamation (XAUTOCLAIM), and O(1) acknowledgment
// (XACK). This replaces ~300 lines of manual heartbeat, metadata hash, and LREM
// coordination that BLMOVE required.
//
// Bitmaps track completed blocks at 1 bit per block (2.5 MB for 20M blocks)
// instead of 64 bytes per entry in a sorted set (1.28 GB for 20M blocks).
//
// All keys use hash tags {chainId} for Redis Cluster slot affinity.

import type Redis from "ioredis";
import { log } from "./logger.js";

const MAX_TASK_RETRIES = 5;
const CONSUMER_GROUP = "workers";

// Lua: batch SETBIT for a range of blocks
export const BATCH_SETBIT_SCRIPT = `
  local key = KEYS[1]
  local from = tonumber(ARGV[1])
  local to = tonumber(ARGV[2])
  for i = from, to do
    redis.call('SETBIT', key, i, 1)
  end
  return to - from + 1
`;

// Lua: find completed blocks in a range (returns array of block numbers)
export const GET_COMPLETED_RANGE_SCRIPT = `
  local key = KEYS[1]
  local from = tonumber(ARGV[1])
  local to = tonumber(ARGV[2])
  local completed = {}
  for i = from, to do
    if redis.call('GETBIT', key, i) == 1 then
      table.insert(completed, i)
    end
  end
  return completed
`;

// Lua: scan bitmap from watermark, find first zero bit (contiguous watermark)
export const CONTIGUOUS_WATERMARK_SCRIPT = `
  local key = KEYS[1]
  local from = tonumber(ARGV[1])
  local limit = tonumber(ARGV[2])
  for i = from, from + limit - 1 do
    if redis.call('GETBIT', key, i) == 0 then
      return i - 1
    end
  end
  return from + limit - 1
`;

export interface BlockTask {
  startBlock: number;
  endBlock: number; // inclusive
  assignedTo?: string;
  retryCount?: number;
  messageId?: string; // Stream entry ID for XACK
}

export class Coordinator {
  private readonly STREAM: string;
  private readonly DLQ: string;
  private readonly BITMAP: string;
  private readonly INIT_LOCK: string;
  private readonly SEED_COMPLETE: string;
  private readonly BLOCK_HASH: string;

  constructor(
    private readonly redis: Redis,
    chainId: number = 1
  ) {
    // Hash tags {chainId} ensure all keys for one chain map to the same
    // Redis Cluster slot — required for multi-key Lua scripts.
    const p = `indexer:{${chainId}}`;
    this.STREAM = `${p}:tasks`;
    this.DLQ = `${p}:dlq`;
    this.BITMAP = `${p}:completed`;
    this.INIT_LOCK = `${p}:init_lock`;
    this.SEED_COMPLETE = `${p}:seed_complete`;
    this.BLOCK_HASH = `${p}:block_hash`;
  }

  /** Ensure consumer group exists (idempotent). */
  private async ensureGroup(): Promise<void> {
    try {
      await this.redis.xgroup("CREATE", this.STREAM, CONSUMER_GROUP, "0", "MKSTREAM");
    } catch (err: any) {
      // BUSYGROUP = group already exists — safe to ignore
      if (!err.message?.includes("BUSYGROUP")) throw err;
    }
  }

  async initQueue(
    fromBlock: number,
    toBlock: number,
    batchSize: number
  ): Promise<void> {
    const lockTtl = 300 + Math.ceil((toBlock - fromBlock) / 100_000);
    const acquired = await this.redis.set(this.INIT_LOCK, "1", "EX", lockTtl, "NX");
    if (!acquired) {
      log("info", "Queue already initialized by another worker");
      return;
    }

    await this.ensureGroup();

    // Seed-complete flag prevents re-seeding after a successful seed.
    // Unlike checking streamLen > 0 (which breaks partial-seed crash recovery),
    // this flag is only set AFTER the full range is seeded.
    const seedComplete = await this.redis.get(this.SEED_COMPLETE);
    if (seedComplete) {
      log("info", "Queue already fully seeded, skipping init");
      return;
    }

    log("info", "Seeding work queue (Streams)", { fromBlock, toBlock, batchSize });

    // Pipeline XADD in chunks to bound memory
    const PIPELINE_CHUNK = 10_000;
    let taskCount = 0;

    for (let chunkBase = fromBlock; chunkBase <= toBlock; chunkBase += batchSize * PIPELINE_CHUNK) {
      // Single Lua scan for the entire chunk range — O(1) round-trip instead of
      // O(N_blocks) sequential getbit calls per task slot.
      const chunkEnd = Math.min(chunkBase + batchSize * PIPELINE_CHUNK - 1, toBlock);
      const completedInChunk = new Set<number>(
        await this.redis.eval(
          GET_COMPLETED_RANGE_SCRIPT, 1,
          this.BITMAP, chunkBase, chunkEnd
        ) as number[]
      );

      const pipe = this.redis.pipeline();
      let pushCount = 0;

      for (let start = chunkBase; start <= toBlock && pushCount < PIPELINE_CHUNK; start += batchSize) {
        const end = Math.min(start + batchSize - 1, toBlock);

        let allCompleted = true;
        for (let b = start; b <= end; b++) {
          if (!completedInChunk.has(b)) { allCompleted = false; break; }
        }
        if (allCompleted) continue;

        const task: BlockTask = { startBlock: start, endBlock: end };
        pipe.xadd(this.STREAM, "*", "task", JSON.stringify(task));
        pushCount++;
      }

      if (pushCount > 0) {
        await pipe.exec();
        taskCount += pushCount;
      }
    }

    // Mark seed as complete — prevents re-seeding on next init.
    // Set AFTER the full range is processed, so a partial-seed crash re-seeds correctly.
    await this.redis.set(this.SEED_COMPLETE, "1");

    log("info", "Queue seeded", { taskCount, fromBlock, toBlock });
  }

  async claimTask(
    workerId: string,
    timeoutSec: number = 5
  ): Promise<BlockTask | null> {
    await this.ensureGroup();

    const result = await this.redis.xreadgroup(
      "GROUP", CONSUMER_GROUP, workerId,
      "COUNT", 1,
      "BLOCK", timeoutSec * 1000,
      "STREAMS", this.STREAM, ">"
    );

    if (!result || result.length === 0) return null;

    const [, entries] = result[0] as [string, [string, string[]][]];
    if (!entries || entries.length === 0) return null;

    const [messageId, fields] = entries[0]!;
    // fields = ["task", "{...json...}"]
    const taskIdx = fields.indexOf("task");
    if (taskIdx === -1) return null;

    const task: BlockTask = JSON.parse(fields[taskIdx + 1]!);
    task.assignedTo = workerId;
    task.messageId = messageId;

    return task;
  }

  async completeTask(task: BlockTask): Promise<void> {
    // Mark blocks completed in bitmap — O(n) SETBIT via Lua
    await this.redis.eval(
      BATCH_SETBIT_SCRIPT, 1,
      this.BITMAP, task.startBlock, task.endBlock
    );

    // Acknowledge + delete from stream — O(1) each
    if (task.messageId) {
      const pipe = this.redis.pipeline();
      pipe.xack(this.STREAM, CONSUMER_GROUP, task.messageId);
      pipe.xdel(this.STREAM, task.messageId);
      await pipe.exec();
    }
  }

  async requeueTask(task: BlockTask, incrementRetry: boolean = true): Promise<void> {
    const retryCount = incrementRetry ? (task.retryCount ?? 0) + 1 : (task.retryCount ?? 0);
    const cleanTask: BlockTask = {
      startBlock: task.startBlock,
      endBlock: task.endBlock,
      retryCount,
    };

    if (retryCount > MAX_TASK_RETRIES) {
      log("error", "Task exceeded max retries — moving to dead-letter queue", {
        startBlock: task.startBlock,
        endBlock: task.endBlock,
        retryCount,
      });
      // XADD to DLQ, then ACK+DEL from main stream
      // MAXLEN ~ 10000 caps DLQ memory — oldest dead tasks are evicted
      await (this.redis as any).xadd(this.DLQ, "MAXLEN", "~", "10000", "*", "task", JSON.stringify(cleanTask));
    } else {
      // XADD new entry with incremented retryCount
      await this.redis.xadd(this.STREAM, "*", "task", JSON.stringify(cleanTask));
    }

    // ACK + DEL the original message
    if (task.messageId) {
      const pipe = this.redis.pipeline();
      pipe.xack(this.STREAM, CONSUMER_GROUP, task.messageId);
      pipe.xdel(this.STREAM, task.messageId);
      await pipe.exec();
    }
  }

  /**
   * Reclaim stale tasks via XAUTOCLAIM.
   * Streams tracks idle time per consumer — no heartbeats needed.
   * XAUTOCLAIM atomically transfers ownership of idle entries.
   */
  async reclaimStaleTasks(staleMs: number = 120_000): Promise<number> {
    await this.ensureGroup();

    const result = await (this.redis as any).xautoclaim(
      this.STREAM, CONSUMER_GROUP, "_reclaimer_",
      staleMs, "0-0", "COUNT", 100
    ) as [string, [string, string[]][], string[]];

    if (!result || !result[1] || result[1].length === 0) return 0;

    const claimed = result[1];
    if (claimed.length === 0) return 0;

    // Parse all claimed entries
    const entries: Array<{ messageId: string; task: BlockTask }> = [];
    for (const [messageId, fields] of claimed) {
      const taskIdx = fields.indexOf("task");
      if (taskIdx === -1) continue;
      entries.push({ messageId, task: JSON.parse(fields[taskIdx + 1]!) });
    }

    if (entries.length === 0) return 0;

    // Phase 1: write-before-delete — all requeues in a single pipeline
    const requeuePipe = this.redis.pipeline();
    for (const { task } of entries) {
      const retryCount = task.retryCount ?? 0;
      if (retryCount >= MAX_TASK_RETRIES) {
        (requeuePipe as any).xadd(this.DLQ, "MAXLEN", "~", "10000", "*", "task", JSON.stringify({ ...task, retryCount: retryCount + 1 }));
        log("error", "Reclaimed task exceeded max retries — DLQ", {
          startBlock: task.startBlock, endBlock: task.endBlock,
        });
      } else {
        requeuePipe.xadd(this.STREAM, "*", "task", JSON.stringify({ ...task, retryCount: retryCount + 1 }));
        log("warn", "Reclaimed stale task", {
          startBlock: task.startBlock, endBlock: task.endBlock,
        });
      }
    }
    await requeuePipe.exec();

    // Phase 2: ACK + DEL all originals in a single pipeline
    const cleanupPipe = this.redis.pipeline();
    for (const { messageId } of entries) {
      cleanupPipe.xack(this.STREAM, CONSUMER_GROUP, messageId);
      cleanupPipe.xdel(this.STREAM, messageId);
    }
    await cleanupPipe.exec();

    return entries.length;
  }

  async getStats(): Promise<{
    pending: number;
    processing: number;
    completed: number;
    dlqSize: number;
  }> {
    const streamLen = await this.redis.xlen(this.STREAM);
    const pendingInfo = await this.redis.xpending(this.STREAM, CONSUMER_GROUP);
    const processing = (pendingInfo as any)?.[0] ?? 0;
    const completed = await this.redis.bitcount(this.BITMAP);
    const dlqSize = await this.redis.xlen(this.DLQ);
    return {
      pending: Math.max(0, streamLen - processing),
      processing,
      completed,
      dlqSize,
    };
  }

  /** Mark blocks completed in bitmap (for partial progress). */
  async markBlocksCompleted(fromBlock: number, toBlock: number): Promise<void> {
    await this.redis.eval(
      BATCH_SETBIT_SCRIPT, 1,
      this.BITMAP, fromBlock, toBlock
    );
  }

  async isBlockCompleted(blockNumber: number): Promise<boolean> {
    const bit = await this.redis.getbit(this.BITMAP, blockNumber);
    return bit === 1;
  }

  /** Batch check via Lua — single round-trip instead of N GETBIT calls. */
  async getCompletedBlocksInRange(
    fromBlock: number,
    toBlock: number
  ): Promise<Set<number>> {
    const result = await this.redis.eval(
      GET_COMPLETED_RANGE_SCRIPT, 1,
      this.BITMAP, fromBlock, toBlock
    ) as number[];
    return new Set(result);
  }

  async getLastBlockHash(blockNumber: number): Promise<string | null> {
    return this.redis.hget(this.BLOCK_HASH, blockNumber.toString());
  }

  async setLastBlockHash(blockNumber: number, hash: string): Promise<void> {
    await this.redis.hset(this.BLOCK_HASH, blockNumber.toString(), hash);
  }

  /**
   * Contiguous watermark via Lua bitmap scan.
   * Scans from currentWatermark, finds first 0 bit.
   * O(gap_size), bounded by PAGE_SIZE per call.
   */
  async getContiguousWatermark(currentWatermark: number): Promise<number> {
    const PAGE_SIZE = 100_000;
    return await this.redis.eval(
      CONTIGUOUS_WATERMARK_SCRIPT, 1,
      this.BITMAP, currentWatermark, PAGE_SIZE
    ) as number;
  }

  /**
   * Eviction is a no-op with bitmaps.
   * 20M blocks = 2.5 MB per chain (vs 1.28 GB with sorted sets).
   * At 120 chains = 300 MB total — no eviction needed.
   */
  async evictCompletedBelow(watermark: number): Promise<number> {
    // Bitmap memory is fixed — no eviction needed.
    // Block hashes still need cleanup though.
    if (watermark <= 0) return 0;

    const SCAN_BATCH = 1000;
    let cursor = "0";
    let evicted = 0;
    do {
      const [nextCursor, fields] = await this.redis.hscan(
        this.BLOCK_HASH, cursor, "COUNT", SCAN_BATCH
      );
      cursor = nextCursor;

      const toDelete: string[] = [];
      for (let i = 0; i < fields.length; i += 2) {
        const blockNum = Number(fields[i]);
        if (blockNum < watermark) toDelete.push(fields[i]!);
      }

      if (toDelete.length > 0) {
        const pipe = this.redis.pipeline();
        for (const field of toDelete) pipe.hdel(this.BLOCK_HASH, field);
        await pipe.exec();
        evicted += toDelete.length;
      }
    } while (cursor !== "0");

    return evicted;
  }
}
