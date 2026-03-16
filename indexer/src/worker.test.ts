import { describe, it, expect, vi, beforeEach } from "vitest";
import { Worker } from "./worker.js";
import type { Coordinator, BlockTask } from "./coordinator.js";
import type { RpcEndpoint } from "./rpc.js";
import type { Storage } from "./storage.js";

function createMockCoordinator() {
  const completedBlocks = new Set<number>();
  const completedTasks: BlockTask[] = [];
  const requeuedTasks: BlockTask[] = [];
  const blockHashes = new Map<number, string>();
  let taskQueue: BlockTask[] = [];

  return {
    completedBlocks,
    completedTasks,
    requeuedTasks,
    blockHashes,
    setTasks: (tasks: BlockTask[]) => { taskQueue = [...tasks]; },
    updateHeartbeat: vi.fn(async () => {}),
    reclaimStaleTasks: vi.fn(async () => 0),
    claimTask: vi.fn(async () => taskQueue.shift() ?? null),
    completeTask: vi.fn(async (task: BlockTask) => {
      completedTasks.push(task);
      for (let b = task.startBlock; b <= task.endBlock; b++) {
        completedBlocks.add(b);
      }
    }),
    requeueTask: vi.fn(async (task: BlockTask) => {
      requeuedTasks.push(task);
    }),
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
    })),
    evictCompletedBelow: vi.fn(async () => 0),
    getLastBlockHash: vi.fn(async (blockNumber: number) => {
      return blockHashes.get(blockNumber) ?? null;
    }),
    setLastBlockHash: vi.fn(async (blockNumber: number, hash: string) => {
      blockHashes.set(blockNumber, hash);
    }),
    getContiguousWatermark: vi.fn(async (currentWatermark: number) => {
      // Walk from currentWatermark upward, stop at first gap
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

function createMockRpc() {
  return {
    getBlockWithReceipts: vi.fn(async (blockNum: number) => ({
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
    })),
  } as unknown as RpcEndpoint;
}

function createMockStorage() {
  return {
    insertBlock: vi.fn(async () => {}),
    updateWatermark: vi.fn(async () => {}),
    getWatermark: vi.fn(async () => 0),
  } as unknown as Storage;
}

describe("Worker", () => {
  it("processes a single task and completes it", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    coord.setTasks([{ startBlock: 0, endBlock: 2 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // All 3 blocks should have been indexed
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(3);
    expect(storage.insertBlock).toHaveBeenCalledTimes(3);
    expect(coord.completeTask).toHaveBeenCalledOnce();
    // Contiguous watermark: blocks 0,1,2 all completed → watermark advances to 2
    expect(storage.updateWatermark).toHaveBeenCalledWith(1, 2, "test-w");
  });

  it("skips already-completed blocks", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // Block 0 already done
    coord.completedBlocks.add(0);
    coord.setTasks([{ startBlock: 0, endBlock: 2 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // Only 2 blocks indexed (1, 2)
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(2);
  });

  it("requeues task on RPC failure", async () => {
    const coord = createMockCoordinator();
    const rpc = {
      getBlockWithReceipts: vi.fn(async () => {
        throw new Error("RPC down");
      }),
    } as unknown as RpcEndpoint;
    const storage = createMockStorage();

    coord.setTasks([{ startBlock: 0, endBlock: 0 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    expect(coord.requeueTask).toHaveBeenCalledOnce();
    expect(storage.insertBlock).not.toHaveBeenCalled();
    // No blocks processed → no partial progress to mark
    expect(coord.markBlocksCompleted).not.toHaveBeenCalled();
  });

  it("marks partial progress before requeue on mid-task failure", async () => {
    const coord = createMockCoordinator();
    const storage = createMockStorage();

    const rpc = {
      getBlockWithReceipts: vi.fn(async (blockNum: number) => {
        if (blockNum === 3) throw new Error("RPC down");
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

    coord.setTasks([{ startBlock: 0, endBlock: 4 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // Blocks 0, 1, 2 indexed; block 3 fails — blocks 0-2 marked as partial progress
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(4); // 0, 1, 2, 3 (3 throws)
    expect(storage.insertBlock).toHaveBeenCalledTimes(3);
    expect(coord.markBlocksCompleted).toHaveBeenCalledWith(0, 2);
    expect(coord.requeueTask).toHaveBeenCalledOnce();
    // completeTask NOT called — task failed
    expect(coord.completeTask).not.toHaveBeenCalled();
  });

  it("graceful drain finishes current block then stops", async () => {
    const coord = createMockCoordinator();
    const storage = createMockStorage();

    let blockCount = 0;
    const rpc = {
      getBlockWithReceipts: vi.fn(async (blockNum: number) => {
        blockCount++;
        // After first block is done, trigger drain
        if (blockCount === 1) {
          // Drain will be checked at next loop iteration
          worker.stop();
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

    coord.setTasks([{ startBlock: 0, endBlock: 9 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // Worker should have indexed 1 block, then drained
    expect(blockCount).toBe(1);
    // Should have requeued the original task
    expect(coord.requeueTask).toHaveBeenCalled();
    // Should have marked block 0 as completed
    expect(coord.markBlocksCompleted).toHaveBeenCalledWith(0, 0);
  });

  it("exits when all tasks are done (empty poll threshold)", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // No tasks — worker should exit after empty polls
    coord.setTasks([]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // Should have polled at least 3 times before deciding to stop
    expect(coord.claimTask).toHaveBeenCalledTimes(3);
  });

  it("uses contiguous watermark — does not advance past gaps", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // Simulate out-of-order completion: blocks 0-9 not completed yet,
    // but this worker processes task [30-39].
    // Blocks 30-39 will be added by completeTask, but blocks 0-29 are missing.
    coord.setTasks([{ startBlock: 30, endBlock: 39 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // completeTask ran (blocks 30-39 marked completed)
    expect(coord.completeTask).toHaveBeenCalledOnce();
    // getContiguousWatermark(0) should return -1 since block 0 is not completed
    expect(coord.getContiguousWatermark).toHaveBeenCalledWith(0);
    // Watermark should NOT advance — contiguousWatermark (-1) < currentWatermark (0)
    expect(storage.updateWatermark).not.toHaveBeenCalled();
    // Eviction should still run with the current watermark (0)
    expect(coord.evictCompletedBelow).toHaveBeenCalledWith(0);
  });

  it("stores block hashes in Redis for cross-task chain integrity", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    coord.setTasks([{ startBlock: 0, endBlock: 2 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // setLastBlockHash should have been called for each block
    expect(coord.setLastBlockHash).toHaveBeenCalledTimes(3);
    expect(coord.setLastBlockHash).toHaveBeenCalledWith(0, "0xhash_0");
    expect(coord.setLastBlockHash).toHaveBeenCalledWith(1, "0xhash_1");
    expect(coord.setLastBlockHash).toHaveBeenCalledWith(2, "0xhash_2");

    // Block hashes should be persisted in the mock store
    expect(coord.blockHashes.get(0)).toBe("0xhash_0");
    expect(coord.blockHashes.get(1)).toBe("0xhash_1");
    expect(coord.blockHashes.get(2)).toBe("0xhash_2");
  });

  it("verifies cross-task parentHash from Redis on task start", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // Simulate: previous task indexed blocks 0-4, stored hash for block 4
    coord.blockHashes.set(4, "0xhash_4");

    // Next task starts at block 5 — should load stored hash for block 4
    // and verify block 5's parentHash matches
    coord.setTasks([{ startBlock: 5, endBlock: 7 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // getLastBlockHash should have been called for block 4 (startBlock - 1)
    expect(coord.getLastBlockHash).toHaveBeenCalledWith(4);
    // All 3 blocks processed successfully (parentHash chain is valid)
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(3);
    expect(coord.completeTask).toHaveBeenCalledOnce();
  });

  it("detects cross-task reorg via parentHash mismatch from Redis", async () => {
    const coord = createMockCoordinator();
    const storage = createMockStorage();

    // Simulate: previous task stored hash for block 9 — but a reorg happened
    // and the RPC now returns a different parentHash for block 10
    coord.blockHashes.set(9, "0xhash_9_original");

    const rpc = {
      getBlockWithReceipts: vi.fn(async (blockNum: number) => ({
        block: {
          number: "0x" + blockNum.toString(16),
          hash: `0xhash_${blockNum}_reorged`,
          // Block 10's parentHash does NOT match the stored hash for block 9
          parentHash: `0xhash_${blockNum - 1}_reorged`,
          timestamp: "0x60",
          gasUsed: "0x100",
          gasLimit: "0x200",
          transactions: [],
        },
        receipts: [],
      })),
    } as unknown as RpcEndpoint;

    coord.setTasks([{ startBlock: 10, endBlock: 12 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // Should have fetched block 10, detected parentHash mismatch, and requeued
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(1);
    expect(coord.requeueTask).toHaveBeenCalledOnce();
    expect(coord.completeTask).not.toHaveBeenCalled();
    // No blocks were stored → no partial progress
    expect(storage.insertBlock).not.toHaveBeenCalled();
  });

  it("skips cross-task verification for genesis block (startBlock === 0)", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // Task starts at block 0 — no previous block to verify against
    coord.setTasks([{ startBlock: 0, endBlock: 2 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // getLastBlockHash should NOT have been called (startBlock === 0)
    expect(coord.getLastBlockHash).not.toHaveBeenCalled();
    // All blocks processed normally
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(3);
    expect(coord.completeTask).toHaveBeenCalledOnce();
  });

  it("skips cross-task verification when no stored hash exists", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // No stored hash for block 9 — first time indexing this range
    // getLastBlockHash will return null
    coord.setTasks([{ startBlock: 10, endBlock: 12 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // getLastBlockHash called for block 9, returns null
    expect(coord.getLastBlockHash).toHaveBeenCalledWith(9);
    // All blocks processed (null lastBlockHash means no verification for first block)
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(3);
    expect(coord.completeTask).toHaveBeenCalledOnce();
  });

  it("advances watermark only to contiguous point", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // Blocks 0-4 already completed (from a previous task)
    for (let b = 0; b <= 4; b++) coord.completedBlocks.add(b);
    // Current watermark in PG is 0
    storage.getWatermark = vi.fn(async () => 0) as any;

    // This worker processes task [5-9]
    coord.setTasks([{ startBlock: 5, endBlock: 9 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // After completing [5-9], blocks 0-9 are all completed.
    // getContiguousWatermark(0) should return 9.
    expect(storage.updateWatermark).toHaveBeenCalledWith(1, 9, "test-w");
  });
});
