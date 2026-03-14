// Worker: claims tasks, fetches blocks via RPC, persists to PostgreSQL.

import type { Coordinator, BlockTask } from "./coordinator.js";
import type { RpcClient } from "./rpc.js";
import type { Storage } from "./storage.js";
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
    private readonly rpc: RpcClient,
    private readonly storage: Storage,
    private readonly chainId: number = 1
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

    this.lastBlockHash = null;
    let lastProcessedBlock = startBlock - 1;

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

          if (blockNum > startBlock) {
            await this.coordinator.markBlocksCompleted(startBlock, blockNum - 1);
          }
          await this.coordinator.requeueTask(task);

          this.running = false;
          return;
        }

        if (completedBlocks.has(blockNum)) continue;

        this.currentBlock = blockNum;
        await this.indexBlock(blockNum);
        this.currentBlock = null;
        lastProcessedBlock = blockNum;
      }

      await this.coordinator.completeTask(task);
      await this.storage.updateWatermark(this.chainId, endBlock, this.workerId);

      // Throttle stats — getStats() is 3 Redis calls per invocation
      const now = Date.now();
      if (now - this.lastStatsAt > 30_000) {
        const stats = await this.coordinator.getStats();
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
      this.currentBlock = null;

      // Mark partial progress so the next worker skips already-indexed blocks
      if (lastProcessedBlock >= startBlock) {
        try {
          await this.coordinator.markBlocksCompleted(startBlock, lastProcessedBlock);
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
    const { block, receipts } = await this.rpc.getBlockWithReceipts(blockNumber);
    this.metrics.rpcCalls += 2;

    // Verify parentHash chain continuity within a task
    if (this.lastBlockHash && block.parentHash !== this.lastBlockHash) {
      log("error", "Block chain integrity violation — possible reorg", {
        block: blockNumber,
        expectedParentHash: this.lastBlockHash,
        actualParentHash: block.parentHash,
      });
    }
    this.lastBlockHash = block.hash;

    await this.storage.insertBlock(block, receipts, this.workerId);
    this.metrics.blocksIndexed++;

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
