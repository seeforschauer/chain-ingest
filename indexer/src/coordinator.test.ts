import { describe, it, expect, vi, beforeEach } from "vitest";
import { Coordinator, type BlockTask } from "./coordinator.js";
import { createMockRedis } from "./test-utils.js";

describe("Coordinator", () => {
  let redis: ReturnType<typeof createMockRedis>;
  let coordinator: Coordinator;

  beforeEach(() => {
    redis = createMockRedis();
    coordinator = new Coordinator(redis);
  });

  describe("initQueue", () => {
    it("seeds the queue with block range tasks", async () => {
      await coordinator.initQueue(0, 29, 10);
      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(3);
    });

    it("only seeds once (distributed lock)", async () => {
      await coordinator.initQueue(0, 19, 10);
      const first = await coordinator.getStats();
      expect(first.pending).toBe(2);

      await coordinator.initQueue(0, 19, 10);
      const second = await coordinator.getStats();
      expect(second.pending).toBe(2);
    });

    it("skips already-completed ranges", async () => {
      for (let b = 0; b <= 9; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      await coordinator.initQueue(0, 19, 10);
      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1);
    });
  });

  describe("claimTask / completeTask / requeueTask", () => {
    it("claims a task and moves it to processing", async () => {
      await coordinator.initQueue(100, 109, 10);
      const task = await coordinator.claimTask("w1", 0);

      expect(task).not.toBeNull();
      expect(task!.startBlock).toBe(100);
      expect(task!.endBlock).toBe(109);
      expect(task!.assignedTo).toBe("w1");

      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(0);
      expect(stats.processing).toBe(1);
    });

    it("returns null when queue is empty", async () => {
      const task = await coordinator.claimTask("w1", 0);
      expect(task).toBeNull();
    });

    it("completeTask marks blocks as completed", async () => {
      await coordinator.initQueue(0, 4, 5);
      const task = await coordinator.claimTask("w1", 0);
      await coordinator.completeTask(task!);

      const stats = await coordinator.getStats();
      expect(stats.processing).toBe(0);
      expect(stats.completed).toBe(5);

      expect(await coordinator.isBlockCompleted(0)).toBe(true);
      expect(await coordinator.isBlockCompleted(4)).toBe(true);
      expect(await coordinator.isBlockCompleted(5)).toBe(false);
    });

    it("requeueTask returns task to pending", async () => {
      await coordinator.initQueue(0, 9, 10);
      const task = await coordinator.claimTask("w1", 0);
      await coordinator.requeueTask(task!);

      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1);
      expect(stats.processing).toBe(0);
    });
  });

  describe("markBlocksCompleted", () => {
    it("marks individual blocks without touching processing queue", async () => {
      await coordinator.initQueue(0, 9, 10);
      await coordinator.claimTask("w1", 0);

      await coordinator.markBlocksCompleted(0, 4);

      const stats = await coordinator.getStats();
      expect(stats.processing).toBe(1);

      expect(await coordinator.isBlockCompleted(0)).toBe(true);
      expect(await coordinator.isBlockCompleted(4)).toBe(true);
      expect(await coordinator.isBlockCompleted(5)).toBe(false);
    });
  });

  describe("reclaimStaleTasks", () => {
    it("reclaims tasks with expired heartbeat", async () => {
      await coordinator.initQueue(0, 9, 10);
      const task = await coordinator.claimTask("dead-worker", 0);

      const taskKey = `${task!.startBlock}:${task!.endBlock}`;
      await redis.hset(
        "indexer:1:task_meta",
        taskKey,
        JSON.stringify({ assignedTo: "dead-worker", assignedAt: Date.now() - 300_000 })
      );
      redis._store.keys.delete("indexer:1:heartbeat:dead-worker");

      const reclaimed = await coordinator.reclaimStaleTasks(120_000);
      expect(reclaimed).toBe(1);

      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1);
      expect(stats.processing).toBe(0);
    });

    it("does NOT reclaim tasks from alive workers", async () => {
      await coordinator.initQueue(0, 9, 10);
      const task = await coordinator.claimTask("slow-worker", 0);

      const taskKey = `${task!.startBlock}:${task!.endBlock}`;
      await redis.hset(
        "indexer:1:task_meta",
        taskKey,
        JSON.stringify({ assignedTo: "slow-worker", assignedAt: Date.now() - 300_000 })
      );
      await coordinator.updateHeartbeat("slow-worker");

      const reclaimed = await coordinator.reclaimStaleTasks(120_000);
      expect(reclaimed).toBe(0);

      const stats = await coordinator.getStats();
      expect(stats.processing).toBe(1);
    });

    it("reclaims orphaned tasks with no metadata", async () => {
      await coordinator.initQueue(0, 9, 10);
      await coordinator.claimTask("crashed-worker", 0);

      await redis.hdel("indexer:1:task_meta", "0:9");

      const reclaimed = await coordinator.reclaimStaleTasks(120_000);
      expect(reclaimed).toBe(1);

      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1);
      expect(stats.processing).toBe(0);
    });
  });

  describe("heartbeat", () => {
    it("marks worker as alive after heartbeat", async () => {
      await coordinator.updateHeartbeat("w1");
      expect(await coordinator.isWorkerAlive("w1")).toBe(true);
    });

    it("reports missing heartbeat as dead", async () => {
      expect(await coordinator.isWorkerAlive("ghost")).toBe(false);
    });
  });

  describe("evictCompletedBelow", () => {
    it("removes completed entries below watermark", async () => {
      for (let b = 0; b < 10; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      const removed = await coordinator.evictCompletedBelow(5);
      expect(removed).toBe(5);
      expect(await coordinator.isBlockCompleted(4)).toBe(false);
      expect(await coordinator.isBlockCompleted(5)).toBe(true);
    });

    it("evicts BLOCK_HASH entries below watermark", async () => {
      // Store hashes for blocks 0-9
      for (let b = 0; b < 10; b++) {
        await coordinator.setLastBlockHash(b, `0xhash_${b}`);
      }
      // Also add completed blocks so zremrangebyscore has something to remove
      for (let b = 0; b < 10; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }

      await coordinator.evictCompletedBelow(5);

      // Hashes below watermark should be deleted
      expect(await coordinator.getLastBlockHash(0)).toBeNull();
      expect(await coordinator.getLastBlockHash(4)).toBeNull();
      // Hashes at and above watermark should remain
      expect(await coordinator.getLastBlockHash(5)).toBe("0xhash_5");
      expect(await coordinator.getLastBlockHash(9)).toBe("0xhash_9");
    });
  });

  describe("block hash storage (cross-task chain integrity)", () => {
    it("stores and retrieves block hash", async () => {
      await coordinator.setLastBlockHash(100, "0xabc123");
      const hash = await coordinator.getLastBlockHash(100);
      expect(hash).toBe("0xabc123");
    });

    it("returns null for non-existent block hash", async () => {
      const hash = await coordinator.getLastBlockHash(999);
      expect(hash).toBeNull();
    });

    it("overwrites existing block hash", async () => {
      await coordinator.setLastBlockHash(100, "0xoriginal");
      await coordinator.setLastBlockHash(100, "0xreorged");
      const hash = await coordinator.getLastBlockHash(100);
      expect(hash).toBe("0xreorged");
    });

    it("stores hashes independently per block number", async () => {
      await coordinator.setLastBlockHash(10, "0xhash_10");
      await coordinator.setLastBlockHash(20, "0xhash_20");
      await coordinator.setLastBlockHash(30, "0xhash_30");

      expect(await coordinator.getLastBlockHash(10)).toBe("0xhash_10");
      expect(await coordinator.getLastBlockHash(20)).toBe("0xhash_20");
      expect(await coordinator.getLastBlockHash(30)).toBe("0xhash_30");
      expect(await coordinator.getLastBlockHash(15)).toBeNull();
    });

    it("uses chain-scoped Redis keys", async () => {
      const redis2 = createMockRedis();
      const coord1 = new Coordinator(redis2, 1);
      const coord42 = new Coordinator(redis2, 42);

      await coord1.setLastBlockHash(100, "0xchain1_hash");
      await coord42.setLastBlockHash(100, "0xchain42_hash");

      expect(await coord1.getLastBlockHash(100)).toBe("0xchain1_hash");
      expect(await coord42.getLastBlockHash(100)).toBe("0xchain42_hash");
    });
  });

  describe("getContiguousWatermark", () => {
    it("returns contiguous high point from watermark", async () => {
      // Blocks 0-9 all completed
      for (let b = 0; b <= 9; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(9);
    });

    it("stops at first gap", async () => {
      // Blocks 0-4 completed, block 5 missing, blocks 6-9 completed
      for (let b = 0; b <= 4; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      for (let b = 6; b <= 9; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(4);
    });

    it("returns currentWatermark - 1 when first block is missing", async () => {
      // Blocks 5-9 completed but block 0 (the starting point) is missing
      for (let b = 5; b <= 9; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(-1);
    });

    it("works with non-zero starting watermark", async () => {
      // Blocks 5-12 completed, scan from watermark=5
      for (let b = 5; b <= 12; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      const watermark = await coordinator.getContiguousWatermark(5);
      expect(watermark).toBe(12);
    });

    it("handles gap right after starting watermark", async () => {
      // Watermark at 5, block 5 missing, blocks 6-9 completed
      for (let b = 6; b <= 9; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      const watermark = await coordinator.getContiguousWatermark(5);
      expect(watermark).toBe(4);
    });

    it("handles empty completed set", async () => {
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(-1);
    });

    it("prevents the out-of-order watermark bug", async () => {
      // Scenario from BUG 1: Worker C completes [30-39] before Worker A completes [0-9]
      // Blocks 30-39 completed, blocks 0-29 NOT completed
      for (let b = 30; b <= 39; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      // Starting from watermark 0, contiguous should be -1 (block 0 not completed)
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(-1);

      // Now Worker A completes [0-9]
      for (let b = 0; b <= 9; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      // Contiguous should now advance to 9 (gap at 10)
      const watermark2 = await coordinator.getContiguousWatermark(0);
      expect(watermark2).toBe(9);

      // Worker B completes [10-29], closing the gap
      for (let b = 10; b <= 29; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      // All blocks 0-39 contiguous, watermark should be 39
      const watermark3 = await coordinator.getContiguousWatermark(0);
      expect(watermark3).toBe(39);
    });
  });
});
