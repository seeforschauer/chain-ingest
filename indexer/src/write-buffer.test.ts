import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { WriteBuffer } from "./write-buffer.js";
import type { Storage } from "./storage.js";
import type { BlockData, ReceiptData } from "./rpc.js";

function makeBlock(blockNum: number): BlockData {
  return {
    number: "0x" + blockNum.toString(16),
    hash: `0xhash_${blockNum}`,
    parentHash: `0xhash_${blockNum - 1}`,
    timestamp: "0x60",
    gasUsed: "0x100",
    gasLimit: "0x200",
    transactions: [],
  } as BlockData;
}

function createMockStorage() {
  return {
    insertBlock: vi.fn(async () => {}),
    insertBlocks: vi.fn(async () => {}),
    updateWatermark: vi.fn(async () => {}),
  } as unknown as Storage;
}

describe("WriteBuffer", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("flushes when buffer reaches flushSize", async () => {
    const storage = createMockStorage();
    const buf = new WriteBuffer(storage, 3, 0);

    await buf.add(makeBlock(1), [], "w1");
    await buf.add(makeBlock(2), [], "w1");
    expect(storage.insertBlocks).not.toHaveBeenCalled();

    await buf.add(makeBlock(3), [], "w1");
    expect(storage.insertBlocks).toHaveBeenCalledOnce();
    expect(storage.insertBlocks).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({ workerId: "w1" }),
      ])
    );
    expect((storage.insertBlocks as any).mock.calls[0][0]).toHaveLength(3);
    expect(buf.pending).toBe(0);

    buf.dispose();
  });

  it("flushes on timer interval", async () => {
    const storage = createMockStorage();
    const buf = new WriteBuffer(storage, 100, 500);

    await buf.add(makeBlock(1), [], "w1");
    await buf.add(makeBlock(2), [], "w1");
    expect(storage.insertBlocks).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(500);
    expect(storage.insertBlocks).toHaveBeenCalledOnce();
    expect((storage.insertBlocks as any).mock.calls[0][0]).toHaveLength(2);

    buf.dispose();
  });

  it("flushes remaining blocks on explicit flush()", async () => {
    const storage = createMockStorage();
    const buf = new WriteBuffer(storage, 100, 0);

    await buf.add(makeBlock(1), [], "w1");
    await buf.add(makeBlock(2), [], "w1");

    await buf.flush();
    expect(storage.insertBlocks).toHaveBeenCalledOnce();
    expect((storage.insertBlocks as any).mock.calls[0][0]).toHaveLength(2);
    expect(buf.pending).toBe(0);

    buf.dispose();
  });

  it("dispose clears interval timer but does not flush", async () => {
    const storage = createMockStorage();
    const buf = new WriteBuffer(storage, 100, 500);

    await buf.add(makeBlock(1), [], "w1");
    buf.dispose();

    // Timer no longer fires after dispose
    await vi.advanceTimersByTimeAsync(1000);
    expect(storage.insertBlocks).not.toHaveBeenCalled();

    // Caller must flush before dispose to avoid data loss
    // (main.ts handles this: await writeBuffer.flush(); writeBuffer.dispose();)
    expect(buf.pending).toBe(1);
  });

  it("flush then dispose is the safe teardown sequence", async () => {
    const storage = createMockStorage();
    const buf = new WriteBuffer(storage, 100, 500);

    await buf.add(makeBlock(1), [], "w1");
    await buf.flush();
    buf.dispose();

    expect(storage.insertBlocks).toHaveBeenCalledOnce();
    expect(buf.pending).toBe(0);
  });

  it("empty buffer flush is a no-op", async () => {
    const storage = createMockStorage();
    const buf = new WriteBuffer(storage, 10, 0);

    await buf.flush();
    expect(storage.insertBlocks).not.toHaveBeenCalled();

    buf.dispose();
  });

  it("flushSize 1 flushes every block immediately", async () => {
    const storage = createMockStorage();
    const buf = new WriteBuffer(storage, 1, 0);

    await buf.add(makeBlock(1), [], "w1");
    expect(storage.insertBlocks).toHaveBeenCalledOnce();

    await buf.add(makeBlock(2), [], "w1");
    expect(storage.insertBlocks).toHaveBeenCalledTimes(2);

    buf.dispose();
  });
});
