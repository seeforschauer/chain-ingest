// Worker: claims tasks, fetches blocks via RPC, persists to PostgreSQL.

import type { Coordinator, BlockTask } from "./coordinator.js";
import type { RpcEndpoint } from "./rpc.js";
import type { RateLimiter } from "./rate-limiter.js";
import type { Storage } from "./storage.js";
import type { WriteBuffer } from "./write-buffer.js";
import type { Metrics } from "./metrics.js";
import { log } from "./logger.js";

interface WorkerMetrics {
  blocksIndexed: number;
  rpcCalls: number;
  errors: number;
  startedAt: number;
}

export class Worker {
  private running = false;
  private draining = false;
  private currentBlock: number | null = null;
  private lastBlockHash: string | null = null;
  private readonly metrics: WorkerMetrics;
  private metricsTimer: ReturnType<typeof setInterval> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private lastReclaimAt = 0;
  private lastStatsAt = 0;

  constructor(
    private readonly workerId: string,
    private readonly coordinator: Coordinator,
    private readonly rpc: RpcEndpoint,
    private readonly storage: Storage,
    private readonly chainId: number = 1,
    private readonly writeBuffer?: WriteBuffer,
    private readonly promMetrics?: Metrics,
    private readonly rateLimiter?: RateLimiter
  ) {
    this.metrics = {
      blocksIndexed: 0,
      rpcCalls: 0,
      errors: 0,
      startedAt: Date.now(),
    };
  }

  async start(): Promise<void> {
    this.running = true;
    this.draining = false;
    log("info", "Worker started", { workerId: this.workerId });

    this.metricsTimer = setInterval(() => this.logMetrics(), 30_000);

    this.heartbeatTimer = setInterval(() => {
      this.coordinator.updateHeartbeat(this.workerId).catch((err) => {
        log("warn", "Heartbeat update failed", {
          workerId: this.workerId,
          error: err instanceof Error ? err.message : String(err),
        });
      });
    }, 10_000);

    await this.coordinator.updateHeartbeat(this.workerId);

    let emptyPolls = 0;

    while (this.running) {
      if (this.draining) {
        log("info", "Drain complete — no active block", { workerId: this.workerId });
        break;
      }

      // Throttled stale reclaim — avoid Redis overhead on every iteration
      if (Date.now() - this.lastReclaimAt > 30_000) {
        const reclaimed = await this.coordinator.reclaimStaleTasks(120_000);
        if (reclaimed > 0) {
          log("info", "Reclaimed stale tasks", { count: reclaimed });
        }
        this.lastReclaimAt = Date.now();
      }

      const task = await this.coordinator.claimTask(this.workerId, 3);

      if (!task) {
        emptyPolls++;

        // Single getStats() serves both idle logging and completion detection
        if (emptyPolls >= 3) {
          const stats = await this.coordinator.getStats();

          if (stats.pending === 0 && stats.processing === 0) {
            log("info", "No more work — all tasks completed", {
              workerId: this.workerId,
              totalCompleted: stats.completed,
            });
            break;
          }

          if (emptyPolls % 5 === 0) {
            log("info", "Queue stats (idle)", {
              workerId: this.workerId,
              ...stats,
            });
          }
        }

        continue;
      }

      emptyPolls = 0;
      await this.processTask(task);
    }

    this.cleanup();
    this.logMetrics();
    log("info", "Worker stopped", { workerId: this.workerId });
  }

  private async processTask(task: BlockTask): Promise<void> {
    const { startBlock, endBlock } = task;
    log("info", "Processing task", {
      workerId: this.workerId,
      startBlock,
      endBlock,
      blocks: endBlock - startBlock + 1,
    });

    // Cross-task chain integrity: load stored hash from Redis for the block
    // preceding this task's range. If startBlock > 0, the previous task should
    // have stored its last block's hash. This closes the gap where lastBlockHash
    // was reset to null at every task boundary, silently accepting reorgs.
    if (startBlock > 0) {
      this.lastBlockHash = await this.coordinator.getLastBlockHash(startBlock - 1);
    } else {
      this.lastBlockHash = null;
    }
    let lastFlushedBlock = startBlock - 1;

    try {
      const completedBlocks = await this.coordinator.getCompletedBlocksInRange(
        startBlock,
        endBlock
      );

      for (let blockNum = startBlock; blockNum <= endBlock; blockNum++) {
        // Drain: finish current block, mark progress, requeue remainder
        if (this.draining && this.currentBlock === null) {
          log("info", "Draining — splitting task at block boundary", {
            workerId: this.workerId,
            completedUpTo: blockNum - 1,
            remainingFrom: blockNum,
            remainingTo: endBlock,
          });

          await this.writeBuffer?.flush();
          lastFlushedBlock = blockNum - 1;
          if (blockNum > startBlock) {
            await this.coordinator.markBlocksCompleted(startBlock, blockNum - 1);
          }
          // Drain split is not a failure — reset retryCount so the remainder
          // doesn't inherit accumulated retries from the original task.
          await this.coordinator.requeueTask({ ...task, retryCount: 0 });

          this.running = false;
          return;
        }

        if (completedBlocks.has(blockNum)) continue;

        this.currentBlock = blockNum;
        await this.indexBlock(blockNum);
        this.currentBlock = null;

        // Without buffering, each block is durable after insertBlock returns
        if (!this.writeBuffer) {
          lastFlushedBlock = blockNum;
        }
      }

      await this.writeBuffer?.flush();
      lastFlushedBlock = endBlock;
      await this.coordinator.completeTask(task);

      // Compute contiguous watermark — scan from current PG watermark upward,
      // stop at the first gap. This prevents the watermark from jumping past
      // unprocessed blocks when tasks complete out of order.
      const currentWatermark = await this.storage.getWatermark(this.chainId);
      const contiguousWatermark = await this.coordinator.getContiguousWatermark(currentWatermark);
      if (contiguousWatermark >= currentWatermark) {
        await this.storage.updateWatermark(this.chainId, contiguousWatermark, this.workerId);
      }

      // Evict completed entries below watermark to bound Redis memory growth.
      // Use the known value — no need to re-read from PG.
      const safeEvictPoint = contiguousWatermark >= currentWatermark ? contiguousWatermark : currentWatermark;
      await this.coordinator.evictCompletedBelow(safeEvictPoint);

      // Throttle stats — getStats() is 3 Redis calls per invocation
      const now = Date.now();
      if (now - this.lastStatsAt > 30_000) {
        const stats = await this.coordinator.getStats();
        if (this.promMetrics) {
          this.promMetrics.queuePending.set(stats.pending);
          this.promMetrics.queueProcessing.set(stats.processing);
          this.promMetrics.queueCompleted.set(stats.completed);
          if (this.rateLimiter?.effectiveRate !== undefined) {
            this.promMetrics.rateLimiterEffectiveRate.set(this.rateLimiter.effectiveRate);
          }
        }
        log("info", "Task completed", {
          workerId: this.workerId,
          startBlock,
          endBlock,
          ...stats,
        });
        this.lastStatsAt = now;
      } else {
        log("info", "Task completed", {
          workerId: this.workerId,
          startBlock,
          endBlock,
        });
      }
    } catch (err) {
      this.metrics.errors++;
      this.promMetrics?.rpcErrorsTotal.inc({ error_type: "task_failure" });
      this.currentBlock = null;

      // Only mark blocks confirmed durable in PG — never mark buffered-but-unflushed
      if (lastFlushedBlock >= startBlock) {
        try {
          await this.coordinator.markBlocksCompleted(startBlock, lastFlushedBlock);
        } catch {
          // Best-effort — if Redis is down, the next worker re-processes (idempotent)
        }
      }

      const msg = err instanceof Error ? err.message : String(err);
      log("error", "Task failed — requeueing", {
        workerId: this.workerId,
        startBlock,
        endBlock,
        error: msg,
      });

      await this.coordinator.requeueTask(task);
    }
  }

  private async indexBlock(blockNumber: number): Promise<void> {
    const blockStart = Date.now();
    this.promMetrics?.currentBlock.set(blockNumber);

    const rpcStart = Date.now();
    const { block, receipts } = await this.rpc.getBlockWithReceipts(blockNumber);
    this.promMetrics?.rpcDurationSeconds.observe((Date.now() - rpcStart) / 1000);
    this.promMetrics?.rpcCallsTotal.inc({ method: "getBlockWithReceipts" });
    this.metrics.rpcCalls += 2;

    if (this.lastBlockHash && block.parentHash !== this.lastBlockHash) {
      throw new Error(
        `Chain integrity violation at block ${blockNumber}: ` +
        `expected parentHash ${this.lastBlockHash}, got ${block.parentHash}`
      );
    }
    this.lastBlockHash = block.hash;

    // Persist block hash to Redis for cross-task chain integrity verification.
    // The next task starting at blockNumber+1 will read this hash and verify
    // its first block's parentHash matches — closing the cross-task gap.
    await this.coordinator.setLastBlockHash(blockNumber, block.hash);

    const storageStart = Date.now();
    if (this.writeBuffer) {
      await this.writeBuffer.add(block, receipts, this.workerId);
      this.promMetrics?.bufferSize.set(this.writeBuffer.pending);
    } else {
      await this.storage.insertBlock(block, receipts, this.workerId);
      this.promMetrics?.storageFlushesTotal.inc();
    }
    this.promMetrics?.storageFlushDurationSeconds.observe((Date.now() - storageStart) / 1000);
    this.promMetrics?.blocksIndexedTotal.inc();
    this.promMetrics?.transactionsIndexedTotal.inc(block.transactions.length);
    this.metrics.blocksIndexed++;

    this.promMetrics?.blockProcessingDurationSeconds.observe((Date.now() - blockStart) / 1000);

    log("debug", "Indexed block", {
      block: blockNumber,
      txCount: block.transactions.length,
      receiptCount: receipts.length,
    });
  }

  /** Graceful drain: finish current block, then stop. */
  stop(): void {
    if (this.draining) return;
    this.draining = true;
    log("info", "Drain requested — finishing current block", {
      workerId: this.workerId,
      currentBlock: this.currentBlock,
    });

    if (this.currentBlock === null) {
      this.running = false;
    }
  }

  private cleanup(): void {
    if (this.metricsTimer) {
      clearInterval(this.metricsTimer);
      this.metricsTimer = null;
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.running = false;
  }

  private logMetrics(): void {
    const uptimeSec = (Date.now() - this.metrics.startedAt) / 1000;
    const blocksPerSec =
      uptimeSec > 0
        ? (this.metrics.blocksIndexed / uptimeSec).toFixed(2)
        : "0.00";

    log("info", "Worker metrics", {
      workerId: this.workerId,
      blocksIndexed: this.metrics.blocksIndexed,
      rpcCalls: this.metrics.rpcCalls,
      errors: this.metrics.errors,
      uptimeSec: Math.round(uptimeSec),
      blocksPerSec: parseFloat(blocksPerSec),
    });
  }
}
