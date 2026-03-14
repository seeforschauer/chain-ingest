import { describe, it, expect, vi, beforeEach } from "vitest";
import { Coordinator, type BlockTask } from "./coordinator.js";

// Minimal Redis mock — implements only what Coordinator actually calls
function createMockRedis() {
  const store = {
    lists: new Map<string, string[]>(),
    hashes: new Map<string, Map<string, string>>(),
    sets: new Map<string, Map<string, number>>(), // sorted sets: member -> score
    keys: new Map<string, string>(),
  };

  const pipeline = () => {
    const ops: Array<() => unknown> = [];
    return {
      lpush: (key: string, val: string) => {
        ops.push(() => {
          const list = store.lists.get(key) ?? [];
          list.unshift(val);
          store.lists.set(key, list);
          return list.length;
        });
      },
      zadd: (key: string, score: number, member: string) => {
        ops.push(() => {
          const set = store.sets.get(key) ?? new Map();
          set.set(member, score);
          store.sets.set(key, set);
          return 1;
        });
      },
      zcount: (key: string, min: number, max: number) => {
        ops.push(() => {
          const set = store.sets.get(key);
          if (!set) return 0;
          let count = 0;
          for (const [, score] of set) {
            if (score >= min && score <= max) count++;
          }
          return count;
        });
      },
      lrem: (key: string, _count: number, val: string) => {
        ops.push(() => {
          const list = store.lists.get(key) ?? [];
          const idx = list.indexOf(val);
          if (idx !== -1) list.splice(idx, 1);
          store.lists.set(key, list);
          return idx !== -1 ? 1 : 0;
        });
      },
      hdel: (key: string, field: string) => {
        ops.push(() => {
          const hash = store.hashes.get(key);
          if (!hash) return 0;
          return hash.delete(field) ? 1 : 0;
        });
      },
      hset: (key: string, field: string, val: string) => {
        ops.push(() => {
          const hash = store.hashes.get(key) ?? new Map();
          hash.set(field, val);
          store.hashes.set(key, hash);
          return 1;
        });
      },
      set: (key: string, val: string, ..._args: unknown[]) => {
        ops.push(() => {
          store.keys.set(key, val);
          return "OK";
        });
      },
      exec: async () => ops.map((op) => [null, op()]),
    };
  };

  return {
    _store: store,
    set: vi.fn(async (key: string, val: string, ...args: unknown[]) => {
      const hasNX = args.includes("NX");
      if (hasNX && store.keys.has(key)) return null;
      store.keys.set(key, val);
      return "OK";
    }),
    get: vi.fn(async (key: string) => store.keys.get(key) ?? null),
    llen: vi.fn(async (key: string) => (store.lists.get(key) ?? []).length),
    lrange: vi.fn(async (key: string, start: number, stop: number) => {
      const list = store.lists.get(key) ?? [];
      if (stop === -1) stop = list.length - 1;
      return list.slice(start, stop + 1);
    }),
    lrem: vi.fn(async (key: string, count: number, val: string) => {
      const list = store.lists.get(key) ?? [];
      const idx = list.indexOf(val);
      if (idx !== -1) list.splice(idx, 1);
      store.lists.set(key, list);
      return idx !== -1 ? 1 : 0;
    }),
    lpush: vi.fn(async (key: string, val: string) => {
      const list = store.lists.get(key) ?? [];
      list.unshift(val);
      store.lists.set(key, list);
      return list.length;
    }),
    rpush: vi.fn(async (key: string, val: string) => {
      const list = store.lists.get(key) ?? [];
      list.push(val);
      store.lists.set(key, list);
      return list.length;
    }),
    brpoplpush: vi.fn(async (src: string, dst: string, _timeout: number) => {
      const srcList = store.lists.get(src) ?? [];
      if (srcList.length === 0) return null;
      const val = srcList.pop()!;
      store.lists.set(src, srcList);
      const dstList = store.lists.get(dst) ?? [];
      dstList.unshift(val);
      store.lists.set(dst, dstList);
      return val;
    }),
    hset: vi.fn(async (key: string, field: string, val: string) => {
      const hash = store.hashes.get(key) ?? new Map();
      hash.set(field, val);
      store.hashes.set(key, hash);
      return 1;
    }),
    hget: vi.fn(async (key: string, field: string) => {
      return store.hashes.get(key)?.get(field) ?? null;
    }),
    hdel: vi.fn(async (key: string, field: string) => {
      const hash = store.hashes.get(key);
      if (!hash) return 0;
      return hash.delete(field) ? 1 : 0;
    }),
    zcard: vi.fn(async (key: string) => (store.sets.get(key)?.size ?? 0)),
    zcount: vi.fn(async (key: string, min: number, max: number) => {
      const set = store.sets.get(key);
      if (!set) return 0;
      let count = 0;
      for (const [, score] of set) {
        if (score >= min && score <= max) count++;
      }
      return count;
    }),
    zscore: vi.fn(async (key: string, member: string) => {
      const set = store.sets.get(key);
      if (!set) return null;
      const score = set.get(member);
      return score !== undefined ? String(score) : null;
    }),
    zadd: vi.fn(async (key: string, score: number, member: string) => {
      const set = store.sets.get(key) ?? new Map();
      set.set(member, score);
      store.sets.set(key, set);
      return 1;
    }),
    zrangebyscore: vi.fn(async (key: string, min: number, max: number) => {
      const set = store.sets.get(key);
      if (!set) return [];
      const results: string[] = [];
      for (const [member, score] of set) {
        if (score >= min && score <= max) results.push(member);
      }
      return results;
    }),
    pipeline,
  } as any;
}

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
      expect(stats.pending).toBe(3); // 0-9, 10-19, 20-29
    });

    it("only seeds once (distributed lock)", async () => {
      await coordinator.initQueue(0, 19, 10);
      const first = await coordinator.getStats();
      expect(first.pending).toBe(2);

      // Second call gets blocked by the lock
      await coordinator.initQueue(0, 19, 10);
      const second = await coordinator.getStats();
      expect(second.pending).toBe(2); // unchanged
    });

    it("skips already-completed ranges", async () => {
      // Mark blocks 0-9 as completed
      for (let b = 0; b <= 9; b++) {
        await redis.zadd("indexer:1:completed_blocks", b, b.toString());
      }
      await coordinator.initQueue(0, 19, 10);
      const stats = await coordinator.getStats();
      expect(stats.pending).toBe(1); // only 10-19
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
      expect(stats.completed).toBe(5); // blocks 0,1,2,3,4

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
      const task = await coordinator.claimTask("w1", 0);

      // Simulate drain: mark blocks 0-4 as completed
      await coordinator.markBlocksCompleted(0, 4);

      // Processing queue still has the task
      const stats = await coordinator.getStats();
      expect(stats.processing).toBe(1);

      // But individual blocks are marked
      expect(await coordinator.isBlockCompleted(0)).toBe(true);
      expect(await coordinator.isBlockCompleted(4)).toBe(true);
      expect(await coordinator.isBlockCompleted(5)).toBe(false);
    });
  });

  describe("reclaimStaleTasks", () => {
    it("reclaims tasks with expired heartbeat", async () => {
      await coordinator.initQueue(0, 9, 10);
      const task = await coordinator.claimTask("dead-worker", 0);

      // Simulate stale metadata (assigned 5 minutes ago)
      const taskKey = `${task!.startBlock}:${task!.endBlock}`;
      await redis.hset(
        "indexer:1:task_meta",
        taskKey,
        JSON.stringify({ assignedTo: "dead-worker", assignedAt: Date.now() - 300_000 })
      );
      // Remove heartbeat — simulating worker crash (TTL expired)
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

      // Stale timestamp but worker has heartbeat
      const taskKey = `${task!.startBlock}:${task!.endBlock}`;
      await redis.hset(
        "indexer:1:task_meta",
        taskKey,
        JSON.stringify({ assignedTo: "slow-worker", assignedAt: Date.now() - 300_000 })
      );
      // Set heartbeat — worker is alive
      await coordinator.updateHeartbeat("slow-worker");

      const reclaimed = await coordinator.reclaimStaleTasks(120_000);
      expect(reclaimed).toBe(0);

      const stats = await coordinator.getStats();
      expect(stats.processing).toBe(1);
    });

    it("reclaims orphaned tasks with no metadata", async () => {
      await coordinator.initQueue(0, 9, 10);
      await coordinator.claimTask("crashed-worker", 0);

      // Delete metadata to simulate crash between BRPOPLPUSH and HSET
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
});
