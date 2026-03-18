// High-load / stress tests — verify the system handles scale-like workloads.
// All in-memory (no Redis/PG), testing logic under volume.

import { describe, it, expect, vi } from "vitest";
import { Coordinator, type BlockTask } from "./coordinator.js";
import { Worker } from "./worker.js";
import type { RpcEndpoint } from "./rpc.js";
import type { Storage } from "./storage.js";
import { createMockRedis } from "./test-utils.js";
describe("High-load: Coordinator", () => {
  it("seeds and drains 1000 tasks without data loss", async () => {
    const redis = createMockRedis();
    const coordinator = new Coordinator(redis);

    // 10,000 blocks in batches of 10 → 1000 tasks
    await coordinator.initQueue(0, 9999, 10);
    const stats = await coordinator.getStats();
    expect(stats.pending).toBe(1000);

    // Claim and complete all 1000 tasks
    let completed = 0;
    while (true) {
      const task = await coordinator.claimTask("stress-worker", 0);
      if (!task) break;
      await coordinator.completeTask(task);
      completed++;
    }

    expect(completed).toBe(1000);

    const finalStats = await coordinator.getStats();
    expect(finalStats.pending).toBe(0);
    expect(finalStats.processing).toBe(0);
    expect(finalStats.completed).toBe(10000); // all blocks marked
  });

  it("handles concurrent claim/complete from multiple simulated workers", async () => {
    const redis = createMockRedis();
    const coordinator = new Coordinator(redis);

    await coordinator.initQueue(0, 499, 5); // 100 tasks of 5 blocks each
    const stats = await coordinator.getStats();
    expect(stats.pending).toBe(100);

    // Simulate 5 workers racing to claim tasks
    const workerResults: Array<{ id: string; tasks: number; blocks: number }> = [];

    const workerLoop = async (workerId: string) => {
      let tasks = 0;
      let blocks = 0;
      while (true) {
        const task = await coordinator.claimTask(workerId, 0);
        if (!task) break;
        tasks++;
        blocks += task.endBlock - task.startBlock + 1;
        await coordinator.completeTask(task);
      }
      workerResults.push({ id: workerId, tasks, blocks });
    };

    // Run 5 workers concurrently
    await Promise.all([
      workerLoop("w1"),
      workerLoop("w2"),
      workerLoop("w3"),
      workerLoop("w4"),
      workerLoop("w5"),
    ]);

    // All tasks should be completed, no duplicates
    const totalTasks = workerResults.reduce((s, w) => s + w.tasks, 0);
    const totalBlocks = workerResults.reduce((s, w) => s + w.blocks, 0);
    expect(totalTasks).toBe(100);
    expect(totalBlocks).toBe(500);

    // Every block should be marked completed exactly once
    const finalStats = await coordinator.getStats();
    expect(finalStats.completed).toBe(500);
    expect(finalStats.pending).toBe(0);
    expect(finalStats.processing).toBe(0);
  });

  it("markBlocksCompleted handles large ranges efficiently", async () => {
    const redis = createMockRedis();
    const coordinator = new Coordinator(redis);

    // Mark 10,000 blocks completed in one call
    await coordinator.markBlocksCompleted(0, 9999);

    // Verify random samples are marked
    expect(await coordinator.isBlockCompleted(0)).toBe(true);
    expect(await coordinator.isBlockCompleted(5000)).toBe(true);
    expect(await coordinator.isBlockCompleted(9999)).toBe(true);
    expect(await coordinator.isBlockCompleted(10000)).toBe(false);

    const stats = await coordinator.getStats();
    expect(stats.completed).toBe(10000);
  });

  it("reclaim handles many stale tasks (XAUTOCLAIM)", async () => {
    const redis = createMockRedis();
    const coordinator = new Coordinator(redis);

    await coordinator.initQueue(0, 99, 10); // 10 tasks

    // Claim all tasks by a "dead" worker
    const tasks: BlockTask[] = [];
    while (true) {
      const task = await coordinator.claimTask("dead-worker", 0);
      if (!task) break;
      tasks.push(task);
    }
    expect(tasks.length).toBe(10);

    // Simulate stale by backdating pending entries in the stream
    const stream = redis._store.streams.get(`indexer:{1}:tasks`);
    const group = stream?.groups.get("workers");
    if (group) {
      for (const [, info] of group.pending) {
        info.deliveredAt = Date.now() - 300_000;
      }
    }

    // Reclaim all 10 via XAUTOCLAIM
    const reclaimed = await coordinator.reclaimStaleTasks(120_000);
    expect(reclaimed).toBe(10);

    const stats = await coordinator.getStats();
    expect(stats.pending).toBe(10);
    expect(stats.processing).toBe(0);
  });
});

// ──────────────────────────────────────────────────────────────
// High-load worker tests
// ──────────────────────────────────────────────────────────────
describe("High-load: Worker", () => {
  function createMockCoordinator() {
    const completedBlocks = new Set<number>();
    const blockHashes = new Map<number, string>();
    let taskQueue: BlockTask[] = [];

    return {
      completedBlocks,
      blockHashes,
      setTasks: (tasks: BlockTask[]) => { taskQueue = [...tasks]; },

      reclaimStaleTasks: vi.fn(async () => 0),
      claimTask: vi.fn(async () => taskQueue.shift() ?? null),
      completeTask: vi.fn(async (task: BlockTask) => {
        for (let b = task.startBlock; b <= task.endBlock; b++) {
          completedBlocks.add(b);
        }
      }),
      requeueTask: vi.fn(async () => {}),
      markBlocksCompleted: vi.fn(async (from: number, to: number) => {
        for (let b = from; b <= to; b++) completedBlocks.add(b);
      }),
      isBlockCompleted: vi.fn(async (b: number) => completedBlocks.has(b)),
      getCompletedBlocksInRange: vi.fn(async (from: number, to: number) => {
        const result = new Set<number>();
        for (let b = from; b <= to; b++) {
          if (completedBlocks.has(b)) result.add(b);
        }
        return result;
      }),
      getStats: vi.fn(async () => ({
        pending: taskQueue.length,
        processing: 0,
        completed: completedBlocks.size,
        dlqSize: 0,
      })),
      evictCompletedBelow: vi.fn(async () => 0),
      getLastBlockHash: vi.fn(async (blockNumber: number) => {
        return blockHashes.get(blockNumber) ?? null;
      }),
      setLastBlockHash: vi.fn(async (blockNumber: number, hash: string) => {
        blockHashes.set(blockNumber, hash);
      }),
      getContiguousWatermark: vi.fn(async (currentWatermark: number) => {
        let contiguous = currentWatermark - 1;
        for (let b = currentWatermark; ; b++) {
          if (completedBlocks.has(b)) {
            contiguous = b;
          } else {
            break;
          }
        }
        return contiguous;
      }),
    } as any;
  }

  it("processes 500 blocks across 50 tasks without loss", async () => {
    const coord = createMockCoordinator();
    const storage = {
      insertBlock: vi.fn(async () => {}),
      updateWatermark: vi.fn(async () => {}),
      getWatermark: vi.fn(async () => 0),
    } as unknown as Storage;

    let rpcCalls = 0;
    const rpc = {
      getBlockWithReceipts: vi.fn(async (blockNum: number) => {
        rpcCalls++;
        return {
          block: {
            number: "0x" + blockNum.toString(16),
            hash: `0xhash_${blockNum}`,
            parentHash: blockNum > 0 ? `0xhash_${blockNum - 1}` : "0xgenesis",
            timestamp: "0x60",
            gasUsed: "0x100",
            gasLimit: "0x200",
            transactions: [],
          },
          receipts: [],
        };
      }),
    } as unknown as RpcEndpoint;

    // Create 50 tasks of 10 blocks each
    const tasks: BlockTask[] = [];
    for (let i = 0; i < 50; i++) {
      tasks.push({ startBlock: i * 10, endBlock: i * 10 + 9 });
    }
    coord.setTasks(tasks);

    const worker = new Worker("highload-w", coord, rpc, storage);
    await worker.start();

    expect(rpcCalls).toBe(500);
    expect(storage.insertBlock).toHaveBeenCalledTimes(500);
    expect(coord.completeTask).toHaveBeenCalledTimes(50);
    expect(coord.completedBlocks.size).toBe(500);
  });

  it("handles blocks with many transactions (batch insert pressure)", async () => {
    const coord = createMockCoordinator();
    const storage = {
      insertBlock: vi.fn(async () => {}),
      updateWatermark: vi.fn(async () => {}),
      getWatermark: vi.fn(async () => 0),
    } as unknown as Storage;

    // Simulate a block with 200 transactions and 500 logs
    const makeTx = (i: number) => ({
      hash: `0xtx_${i}`,
      blockNumber: "0xa",
      transactionIndex: "0x" + i.toString(16),
      from: `0xfrom_${i}`,
      to: `0xto_${i}`,
      value: "0x0",
      gasPrice: "0x1",
      gas: "0x5208",
      input: "0x",
      nonce: "0x" + i.toString(16),
    });

    const makeLog = (txIdx: number, logIdx: number) => ({
      address: `0xcontract_${txIdx}`,
      topics: ["0xddf252ad"],
      data: "0x",
      logIndex: "0x" + logIdx.toString(16),
      transactionHash: `0xtx_${txIdx}`,
      blockNumber: "0xa",
    });

    const transactions = Array.from({ length: 200 }, (_, i) => makeTx(i));
    const receipts = transactions.map((tx, i) => ({
      transactionHash: tx.hash,
      status: "0x1",
      gasUsed: "0x5208",
      cumulativeGasUsed: "0x" + ((i + 1) * 0x5208).toString(16),
      contractAddress: null,
      // 2-3 logs per receipt → ~500 logs total
      logs: Array.from({ length: i % 3 + 1 }, (_, j) => makeLog(i, j)),
    }));

    const rpc = {
      getBlockWithReceipts: vi.fn(async () => ({
        block: {
          number: "0xa",
          hash: "0xhash_10",
          parentHash: "0xhash_9",
          timestamp: "0x60",
          gasUsed: "0xfffff",
          gasLimit: "0x1000000",
          transactions,
        },
        receipts,
      })),
    } as unknown as RpcEndpoint;

    coord.setTasks([{ startBlock: 10, endBlock: 10 }]);

    const worker = new Worker("big-block-w", coord, rpc, storage);
    await worker.start();

    // Block with 200 txs should be passed to insertBlock once
    expect(storage.insertBlock).toHaveBeenCalledOnce();

    // Verify the block data has 200 transactions
    const insertCall = (storage.insertBlock as any).mock.calls[0];
    expect(insertCall[0].transactions.length).toBe(200);
    expect(insertCall[1].length).toBe(200); // 200 receipts
  });

  it("intermittent RPC failures don't cause data loss", async () => {
    const coord = createMockCoordinator();
    const storage = {
      insertBlock: vi.fn(async () => {}),
      updateWatermark: vi.fn(async () => {}),
      getWatermark: vi.fn(async () => 0),
    } as unknown as Storage;

    let callCount = 0;
    const rpc = {
      getBlockWithReceipts: vi.fn(async (blockNum: number) => {
        callCount++;
        // Fail on every 3rd call
        if (callCount % 3 === 0) {
          throw new Error("Simulated RPC failure");
        }
        return {
          block: {
            number: "0x" + blockNum.toString(16),
            hash: `0xhash_${blockNum}`,
            parentHash: `0xhash_${blockNum - 1}`,
            timestamp: "0x60",
            gasUsed: "0x100",
            gasLimit: "0x200",
            transactions: [],
          },
          receipts: [],
        };
      }),
    } as unknown as RpcEndpoint;

    // 5 tasks of 10 blocks
    const tasks: BlockTask[] = [];
    for (let i = 0; i < 5; i++) {
      tasks.push({ startBlock: i * 10, endBlock: i * 10 + 9 });
    }
    coord.setTasks(tasks);

    const worker = new Worker("flaky-w", coord, rpc, storage);
    await worker.start();

    // Some tasks completed, some requeued — no data lost
    const completed = (coord.completeTask as any).mock.calls.length;
    const requeued = (coord.requeueTask as any).mock.calls.length;
    expect(completed + requeued).toBe(5); // all 5 tasks accounted for
  });
});

// ──────────────────────────────────────────────────────────────
// High-load batch insert SQL construction
// ──────────────────────────────────────────────────────────────
describe("High-load: Storage batch insert", () => {
  it("constructs correct placeholder count for 200-tx block", async () => {
    const queries: Array<{ text?: string; values?: unknown[] }> = [];
    const mockClient = {
      query: vi.fn(async (q: any, values?: unknown[]) => {
        if (typeof q === "string") queries.push({ text: q, values });
        else queries.push(q);
        return { rows: [] };
      }),
      release: vi.fn(),
    };
    const mockPool = {
      connect: vi.fn(async () => mockClient),
      query: vi.fn(async (q: any, values?: unknown[]) => {
        if (typeof q === "string") queries.push({ text: q, values });
        else queries.push(q);
        return { rows: [] };
      }),
      end: vi.fn(async () => {}),
    };

    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = mockPool;
    (storage as any).ensurePartition = vi.fn(async () => {});

    // Build a block with 200 transactions — use valid hex for BYTEA conversion
    const pad32 = (i: number) => "0x" + i.toString(16).padStart(64, "0");
    const pad20 = (i: number) => "0x" + i.toString(16).padStart(40, "0");

    const transactions = Array.from({ length: 200 }, (_, i) => ({
      hash: pad32(0xaa0000 + i),
      blockNumber: "0xa",
      transactionIndex: "0x" + i.toString(16),
      from: pad20(0xf00000 + i),
      to: pad20(0xe00000 + i),
      value: "0x0",
      gasPrice: "0x1",
      gas: "0x5208",
      input: "0x",
      nonce: "0x" + i.toString(16),
    }));

    const receipts = transactions.map((tx, i) => ({
      transactionHash: tx.hash,
      status: "0x1",
      gasUsed: "0x5208",
      cumulativeGasUsed: "0x" + ((i + 1) * 0x5208).toString(16),
      contractAddress: null,
      logs: [
        {
          address: pad20(0xcc0000),
          topics: [pad32(0xddf252ad)],
          data: "0x",
          logIndex: "0x0",
          transactionHash: tx.hash,
          blockNumber: "0xa",
        },
      ],
    }));

    const block = {
      number: "0xa",
      hash: pad32(0xb10c),
      parentHash: pad32(0xb10b),
      timestamp: "0x60",
      gasUsed: "0xfffff",
      gasLimit: "0x1000000",
      transactions,
    };

    await storage.insertBlock(block as any, receipts as any, "stress-worker");

    // Verify transaction INSERT has 200 * 15 = 3000 parameters (10 original + 5 EIP-1559/4844)
    const txInsert = queries.find(
      (q) => q.text?.includes("raw.transactions") && q.text?.includes("INSERT")
    );
    expect(txInsert).toBeDefined();
    expect(txInsert!.values!.length).toBe(3000); // 200 tx * 15 columns

    // Verify receipt INSERT has 200 * 7 = 1400 parameters
    const rcptInsert = queries.find(
      (q) => q.text?.includes("raw.receipts") && q.text?.includes("INSERT")
    );
    expect(rcptInsert).toBeDefined();
    expect(rcptInsert!.values!.length).toBe(1400); // 200 receipts * 7 columns (including block_number)

    const logInsert = queries.find(
      (q) => q.text?.includes("raw.logs") && q.text?.includes("INSERT")
    );
    expect(logInsert).toBeDefined();
    expect(logInsert!.values!.length).toBe(1400); // 200 logs * 7 columns
  });

  it("handles block with zero transactions (empty block)", async () => {
    const queries: Array<{ text?: string; values?: unknown[] }> = [];
    const mockClient = {
      query: vi.fn(async (q: any) => {
        if (typeof q === "string") queries.push({ text: q });
        else queries.push(q);
        return { rows: [] };
      }),
      release: vi.fn(),
    };
    const mockPool = {
      connect: vi.fn(async () => mockClient),
      query: vi.fn(async () => ({ rows: [] })),
      end: vi.fn(async () => {}),
    };

    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = mockPool;
    (storage as any).ensurePartition = vi.fn(async () => {});

    const block = {
      number: "0xa",
      hash: "0x" + "ab".repeat(32),
      parentHash: "0x" + "cd".repeat(32),
      timestamp: "0x60",
      gasUsed: "0x0",
      gasLimit: "0x200",
      transactions: [],
    };

    await storage.insertBlock(block as any, [], "worker");

    // Should have block insert but no tx/receipt/log inserts
    const insertQueries = queries.filter((q) => q.text?.includes("INSERT"));
    expect(insertQueries.length).toBe(1); // only block insert
  });
});
