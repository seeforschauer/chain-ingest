// Work distribution and progress tracking via Redis.
//
// Block ranges are pushed to a Redis list as JSON tasks.
// Workers atomically claim tasks with BRPOPLPUSH (pending → processing).
// Completed blocks are tracked in a sorted set for gap detection.
// Crashed workers' tasks are reclaimed via heartbeat expiry.
//
// Redis Streams would be ideal at TT's 120-chain scale, but
// BRPOPLPUSH gives atomic claiming with simpler semantics for this scope.

import type Redis from "ioredis";
import { log } from "./logger.js";

const HEARTBEAT_TTL_SEC = 30;

export interface BlockTask {
  startBlock: number;
  endBlock: number; // inclusive
  assignedTo?: string;
  assignedAt?: number;
}

export class Coordinator {
  private readonly QUEUE_PENDING: string;
  private readonly QUEUE_PROCESSING: string;
  private readonly COMPLETED_SET: string;
  private readonly TASK_META: string;
  private readonly INIT_LOCK: string;
  private readonly HEARTBEAT_PREFIX: string;

  constructor(
    private readonly redis: Redis,
    chainId: number = 1
  ) {
    // Chain-scoped keys — each chain is an isolated coordination domain
    const p = `indexer:${chainId}`;
    this.QUEUE_PENDING = `${p}:queue:pending`;
    this.QUEUE_PROCESSING = `${p}:queue:processing`;
    this.COMPLETED_SET = `${p}:completed_blocks`;
    this.TASK_META = `${p}:task_meta`;
    this.INIT_LOCK = `${p}:init_lock`;
    this.HEARTBEAT_PREFIX = `${p}:heartbeat:`;
  }

  async initQueue(
    fromBlock: number,
    toBlock: number,
    batchSize: number
  ): Promise<void> {
    const acquired = await this.redis.set(this.INIT_LOCK, "1", "EX", 300, "NX");
    if (!acquired) {
      log("info", "Queue already initialized by another worker");
      return;
    }

    const pendingLen = await this.redis.llen(this.QUEUE_PENDING);
    const processingLen = await this.redis.llen(this.QUEUE_PROCESSING);
    if (pendingLen > 0 || processingLen > 0) {
      log("info", "Queue already has tasks, skipping init", {
        pending: pendingLen,
        processing: processingLen,
      });
      return;
    }

    const completedCount = await this.redis.zcard(this.COMPLETED_SET);
    log("info", "Seeding work queue", {
      fromBlock,
      toBlock,
      batchSize,
      alreadyCompleted: completedCount,
    });

    // Pipeline chunks bound memory — 20M blocks / batch 10 = 2M entries ≈ 500MB without this
    const PIPELINE_CHUNK = 10_000;
    let taskCount = 0;

    for (let chunkBase = fromBlock; chunkBase <= toBlock; chunkBase += batchSize * PIPELINE_CHUNK) {
      const checkPipe = this.redis.pipeline();
      const chunkRanges: Array<{ start: number; end: number }> = [];

      for (let start = chunkBase; start <= toBlock && chunkRanges.length < PIPELINE_CHUNK; start += batchSize) {
        const end = Math.min(start + batchSize - 1, toBlock);
        chunkRanges.push({ start, end });
        checkPipe.zcount(this.COMPLETED_SET, start, end);
      }

      const results = await checkPipe.exec();

      const pushPipe = this.redis.pipeline();
      let pushCount = 0;
      for (let i = 0; i < chunkRanges.length; i++) {
        const { start, end } = chunkRanges[i]!;
        const completedInRange = (results?.[i]?.[1] as number) ?? 0;
        if (completedInRange === end - start + 1) continue;

        const task: BlockTask = { startBlock: start, endBlock: end };
        pushPipe.lpush(this.QUEUE_PENDING, JSON.stringify(task));
        pushCount++;
      }

      if (pushCount > 0) {
        await pushPipe.exec();
        taskCount += pushCount;
      }
    }

    log("info", "Queue seeded", { taskCount, fromBlock, toBlock });
  }

  async claimTask(
    workerId: string,
    timeoutSec: number = 5
  ): Promise<BlockTask | null> {
    const raw = await this.redis.brpoplpush(
      this.QUEUE_PENDING,
      this.QUEUE_PROCESSING,
      timeoutSec
    );

    if (!raw) return null;

    const task: BlockTask = JSON.parse(raw);
    task.assignedTo = workerId;
    task.assignedAt = Date.now();

    // Metadata stored in separate hash — never mutate the queue entry,
    // so lrem always matches the original JSON string.
    const taskKey = `${task.startBlock}:${task.endBlock}`;
    const setup = this.redis.pipeline();
    setup.hset(
      this.TASK_META,
      taskKey,
      JSON.stringify({ assignedTo: workerId, assignedAt: task.assignedAt })
    );
    setup.set(
      `${this.HEARTBEAT_PREFIX}${workerId}`,
      Date.now().toString(),
      "EX",
      HEARTBEAT_TTL_SEC
    );
    await setup.exec();

    return task;
  }

  async completeTask(task: BlockTask): Promise<void> {
    // Mark blocks FIRST — on crash, task stays in processing and gets reclaimed
    const pipeline = this.redis.pipeline();
    for (let b = task.startBlock; b <= task.endBlock; b++) {
      pipeline.zadd(this.COMPLETED_SET, b, b.toString());
    }
    await pipeline.exec();

    // Cleanup after write is confirmed
    const clean: BlockTask = { startBlock: task.startBlock, endBlock: task.endBlock };
    const cleanup = this.redis.pipeline();
    cleanup.lrem(this.QUEUE_PROCESSING, 1, JSON.stringify(clean));
    cleanup.hdel(this.TASK_META, `${task.startBlock}:${task.endBlock}`);
    await cleanup.exec();
  }

  async requeueTask(task: BlockTask): Promise<void> {
    const clean: BlockTask = {
      startBlock: task.startBlock,
      endBlock: task.endBlock,
    };
    const raw = JSON.stringify(clean);

    // Write before delete — crash between = task in both queues (safe, deduplicated)
    await this.redis.rpush(this.QUEUE_PENDING, raw);

    const cleanup = this.redis.pipeline();
    cleanup.lrem(this.QUEUE_PROCESSING, 1, raw);
    cleanup.hdel(this.TASK_META, `${task.startBlock}:${task.endBlock}`);
    await cleanup.exec();
  }

  async updateHeartbeat(workerId: string): Promise<void> {
    await this.redis.set(
      `${this.HEARTBEAT_PREFIX}${workerId}`,
      Date.now().toString(),
      "EX",
      HEARTBEAT_TTL_SEC
    );
  }

  async isWorkerAlive(workerId: string): Promise<boolean> {
    const val = await this.redis.get(`${this.HEARTBEAT_PREFIX}${workerId}`);
    return val !== null;
  }

  /**
   * Reclaim tasks from crashed workers.
   * Only reclaims when BOTH: assignment is stale AND heartbeat expired.
   * Prevents stealing from slow-but-alive workers.
   */
  async reclaimStaleTasks(staleMs: number = 120_000): Promise<number> {
    const processing = await this.redis.lrange(this.QUEUE_PROCESSING, 0, -1);
    const now = Date.now();
    let reclaimed = 0;

    for (const raw of processing) {
      const task: BlockTask = JSON.parse(raw);
      const taskKey = `${task.startBlock}:${task.endBlock}`;

      const metaRaw = await this.redis.hget(this.TASK_META, taskKey);
      if (!metaRaw) {
        // Orphaned — worker crashed between BRPOPLPUSH and HSET
        log("warn", "Reclaiming orphaned task (no metadata)", {
          startBlock: task.startBlock,
          endBlock: task.endBlock,
        });
        await this.redis.rpush(this.QUEUE_PENDING, raw);
        await this.redis.lrem(this.QUEUE_PROCESSING, 1, raw);
        reclaimed++;
        continue;
      }

      const meta: { assignedTo: string; assignedAt: number } = JSON.parse(metaRaw);
      if (now - meta.assignedAt <= staleMs) continue;

      const alive = await this.isWorkerAlive(meta.assignedTo);
      if (alive) {
        log("debug", "Task old but worker alive — skipping reclaim", {
          startBlock: task.startBlock,
          assignedTo: meta.assignedTo,
        });
        continue;
      }

      log("warn", "Reclaiming stale task", {
        startBlock: task.startBlock,
        endBlock: task.endBlock,
        assignedTo: meta.assignedTo,
        staleSec: Math.round((now - meta.assignedAt) / 1000),
      });
      // Write before delete
      await this.redis.rpush(this.QUEUE_PENDING, raw);

      const cleanup = this.redis.pipeline();
      cleanup.lrem(this.QUEUE_PROCESSING, 1, raw);
      cleanup.hdel(this.TASK_META, taskKey);
      await cleanup.exec();
      reclaimed++;
    }

    return reclaimed;
  }

  async getStats(): Promise<{
    pending: number;
    processing: number;
    completed: number;
  }> {
    const [pending, processing, completed] = await Promise.all([
      this.redis.llen(this.QUEUE_PENDING),
      this.redis.llen(this.QUEUE_PROCESSING),
      this.redis.zcard(this.COMPLETED_SET),
    ]);
    return { pending, processing, completed };
  }

  /** Mark blocks completed without touching the processing queue (for partial progress). */
  async markBlocksCompleted(fromBlock: number, toBlock: number): Promise<void> {
    const pipeline = this.redis.pipeline();
    for (let b = fromBlock; b <= toBlock; b++) {
      pipeline.zadd(this.COMPLETED_SET, b, b.toString());
    }
    await pipeline.exec();
  }

  async isBlockCompleted(blockNumber: number): Promise<boolean> {
    const score = await this.redis.zscore(
      this.COMPLETED_SET,
      blockNumber.toString()
    );
    return score !== null;
  }

  /** Batch check — single ZRANGEBYSCORE replaces N per-block ZSCORE calls. */
  async getCompletedBlocksInRange(
    fromBlock: number,
    toBlock: number
  ): Promise<Set<number>> {
    const members = await this.redis.zrangebyscore(
      this.COMPLETED_SET,
      fromBlock,
      toBlock
    );
    return new Set(members.map(Number));
  }
}
