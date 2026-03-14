import { describe, it, expect, vi, beforeEach } from "vitest";
import { Worker } from "./worker.js";
import type { Coordinator, BlockTask } from "./coordinator.js";
import type { RpcEndpoint } from "./rpc.js";
import type { Storage } from "./storage.js";

function createMockCoordinator() {
  const completedBlocks = new Set<number>();
  const completedTasks: BlockTask[] = [];
  const requeuedTasks: BlockTask[] = [];
  let taskQueue: BlockTask[] = [];

  return {
    completedBlocks,
    completedTasks,
    requeuedTasks,
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

    coord.setTasks([{ startBlock: 10, endBlock: 12 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // All 3 blocks should have been indexed
    expect(rpc.getBlockWithReceipts).toHaveBeenCalledTimes(3);
    expect(storage.insertBlock).toHaveBeenCalledTimes(3);
    expect(coord.completeTask).toHaveBeenCalledOnce();
    expect(storage.updateWatermark).toHaveBeenCalledWith(1, 12, "test-w");
  });

  it("skips already-completed blocks", async () => {
    const coord = createMockCoordinator();
    const rpc = createMockRpc();
    const storage = createMockStorage();

    // Block 10 already done
    coord.completedBlocks.add(10);
    coord.setTasks([{ startBlock: 10, endBlock: 12 }]);

    const worker = new Worker("test-w", coord as any, rpc, storage);
    await worker.start();

    // Only 2 blocks indexed (11, 12)
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
});
