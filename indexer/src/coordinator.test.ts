import { describe, it, expect, vi, beforeEach } from "vitest";
import { Coordinator, type BlockTask } from "./coordinator.js";
import { createMockRedis } from "./test-utils.js";

describe("Coordinator (Streams + Bitmaps)", () => {
  let redis: ReturnType<typeof createMockRedis>;
  let coordinator: Coordinator;

  beforeEach(() => {
    redis = createMockRedis();
    coordinator = new Coordinator(redis);
  });

  describe("initQueue", () => {
    it("seeds the stream with block range tasks", async () => {
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
      // Mark blocks 0-9 as completed in bitmap
      for (let b = 0; b <= 9; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      await coordinator.initQueue(0, 19, 10);
      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1);
    });
  });

  describe("claimTask / completeTask / requeueTask", () => {
    it("claims a task from the stream", async () => {
      await coordinator.initQueue(100, 109, 10);
      const task = await coordinator.claimTask("w1", 0);

      expect(task).not.toBeNull();
      expect(task!.startBlock).toBe(100);
      expect(task!.endBlock).toBe(109);
      expect(task!.assignedTo).toBe("w1");
      expect(task!.messageId).toBeDefined();
    });

    it("returns null when stream is empty", async () => {
      // Need to create group first
      await coordinator.initQueue(0, 0, 1);
      const task1 = await coordinator.claimTask("w1", 0);
      await coordinator.completeTask(task1!);
      const task2 = await coordinator.claimTask("w1", 0);
      expect(task2).toBeNull();
    });

    it("completeTask marks blocks in bitmap and acks stream entry", async () => {
      await coordinator.initQueue(0, 4, 5);
      const task = await coordinator.claimTask("w1", 0);
      await coordinator.completeTask(task!);

      // Blocks should be marked in bitmap
      expect(await coordinator.isBlockCompleted(0)).toBe(true);
      expect(await coordinator.isBlockCompleted(4)).toBe(true);
      expect(await coordinator.isBlockCompleted(5)).toBe(false);

      const stats = await coordinator.getStats();
      expect(stats.completed).toBe(5);
    });

    it("requeueTask adds new stream entry with incremented retryCount", async () => {
      await coordinator.initQueue(0, 9, 10);
      const task = await coordinator.claimTask("w1", 0);
      await coordinator.requeueTask(task!);

      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1); // new entry added
    });
  });

  describe("markBlocksCompleted", () => {
    it("marks blocks in bitmap without affecting stream", async () => {
      await coordinator.markBlocksCompleted(0, 4);

      expect(await coordinator.isBlockCompleted(0)).toBe(true);
      expect(await coordinator.isBlockCompleted(4)).toBe(true);
      expect(await coordinator.isBlockCompleted(5)).toBe(false);
    });
  });

  describe("reclaimStaleTasks", () => {
    it("reclaims idle tasks via XAUTOCLAIM", async () => {
      await coordinator.initQueue(0, 9, 10);
      const task = await coordinator.claimTask("dead-worker", 0);
      expect(task).not.toBeNull();

      // Simulate stale: manually set deliveredAt to past
      const stream = redis._store.streams.get(`indexer:{1}:tasks`);
      const group = stream?.groups.get("workers");
      if (group && task!.messageId) {
        const pending = group.pending.get(task!.messageId);
        if (pending) pending.deliveredAt = Date.now() - 300_000;
      }

      const reclaimed = await coordinator.reclaimStaleTasks(120_000);
      expect(reclaimed).toBe(1);

      // Original entry should be removed, new entry added
      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1);
    });
  });


  describe("evictCompletedBelow", () => {
    it("evicts BLOCK_HASH entries below watermark", async () => {
      for (let b = 0; b < 10; b++) {
        await coordinator.setLastBlockHash(b, `0xhash_${b}`);
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

    it("uses chain-scoped Redis keys with hash tags", async () => {
      const redis2 = createMockRedis();
      const coord1 = new Coordinator(redis2, 1);
      const coord42 = new Coordinator(redis2, 42);

      await coord1.setLastBlockHash(100, "0xchain1_hash");
      await coord42.setLastBlockHash(100, "0xchain42_hash");

      expect(await coord1.getLastBlockHash(100)).toBe("0xchain1_hash");
      expect(await coord42.getLastBlockHash(100)).toBe("0xchain42_hash");
    });
  });

  describe("getContiguousWatermark (bitmap scan)", () => {
    it("returns contiguous high point from watermark", async () => {
      for (let b = 0; b <= 9; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(9);
    });

    it("stops at first gap", async () => {
      for (let b = 0; b <= 4; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      // block 5 missing
      for (let b = 6; b <= 9; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(4);
    });

    it("returns currentWatermark - 1 when first block is missing", async () => {
      for (let b = 5; b <= 9; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(-1);
    });

    it("works with non-zero starting watermark", async () => {
      for (let b = 5; b <= 12; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      const watermark = await coordinator.getContiguousWatermark(5);
      expect(watermark).toBe(12);
    });

    it("handles empty bitmap", async () => {
      const watermark = await coordinator.getContiguousWatermark(0);
      expect(watermark).toBe(-1);
    });

    it("prevents the out-of-order watermark bug", async () => {
      // Worker C completes [30-39] before Worker A completes [0-9]
      for (let b = 30; b <= 39; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      expect(await coordinator.getContiguousWatermark(0)).toBe(-1);

      // Worker A completes [0-9]
      for (let b = 0; b <= 9; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      expect(await coordinator.getContiguousWatermark(0)).toBe(9);

      // Worker B completes [10-29], closing the gap
      for (let b = 10; b <= 29; b++) {
        await redis.setbit(`indexer:{1}:completed`, b, 1);
      }
      expect(await coordinator.getContiguousWatermark(0)).toBe(39);
    });
  });

  describe("getCompletedBlocksInRange (bitmap)", () => {
    it("returns completed blocks in range", async () => {
      await redis.setbit(`indexer:{1}:completed`, 3, 1);
      await redis.setbit(`indexer:{1}:completed`, 7, 1);
      const completed = await coordinator.getCompletedBlocksInRange(0, 9);
      expect(completed).toEqual(new Set([3, 7]));
    });

    it("returns empty set when no blocks completed", async () => {
      const completed = await coordinator.getCompletedBlocksInRange(0, 9);
      expect(completed).toEqual(new Set());
    });
  });
});
