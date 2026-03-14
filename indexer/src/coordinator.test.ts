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
  });
});
