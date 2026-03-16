import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { RpcClient, JsonRpcError, isMethodNotSupported } from "./rpc.js";
import type { RateLimiter } from "./rate-limiter.js";

// Stub rate limiter — always allows
const stubLimiter: RateLimiter = {
  acquire: async () => {},
  recordThrottle: vi.fn(),
};

// Mock global fetch
const originalFetch = globalThis.fetch;

function mockFetch(responses: Array<{ status: number; body: unknown }>) {
  let callIdx = 0;
  globalThis.fetch = vi.fn(async () => {
    const resp = responses[callIdx] ?? responses[responses.length - 1]!;
    callIdx++;
    return {
      status: resp.status,
      ok: resp.status >= 200 && resp.status < 300,
      statusText: resp.status === 200 ? "OK" : "Error",
      json: async () => resp.body,
    } as Response;
  });
}

describe("RpcClient", () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it("getBlockNumber parses hex response", async () => {
    mockFetch([
      { status: 200, body: { jsonrpc: "2.0", id: 1, result: "0x1a4" } },
    ]);
    const rpc = new RpcClient("http://test", stubLimiter, 0);
    const num = await rpc.getBlockNumber();
    expect(num).toBe(420);
  });

  it("getFinalizedBlockNumber throws on null block", async () => {
    mockFetch([
      { status: 200, body: { jsonrpc: "2.0", id: 1, result: null } },
    ]);
    const rpc = new RpcClient("http://test", stubLimiter, 0);
    await expect(rpc.getFinalizedBlockNumber()).rejects.toThrow(
      "Finalized block not available"
    );
  });

  it("getBlockWithReceipts returns block and receipts via batch", async () => {
    const blockData = {
      number: "0xa",
      hash: "0xabc",
      parentHash: "0xdef",
      timestamp: "0x60",
      gasUsed: "0x100",
      gasLimit: "0x200",
      transactions: [],
    };
    const receiptsData: unknown[] = [];

    mockFetch([
      {
        status: 200,
        body: [
          { jsonrpc: "2.0", id: 1, result: blockData },
          { jsonrpc: "2.0", id: 2, result: receiptsData },
        ],
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    const { block, receipts } = await rpc.getBlockWithReceipts(10);

    expect(block.hash).toBe("0xabc");
    expect(receipts).toEqual([]);
  });

  it("throws on null block in getBlockWithReceipts", async () => {
    mockFetch([
      {
        status: 200,
        body: [
          { jsonrpc: "2.0", id: 1, result: null },
          { jsonrpc: "2.0", id: 2, result: [] },
        ],
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    await expect(rpc.getBlockWithReceipts(999)).rejects.toThrow(
      "Block 999 not found"
    );
  });

  it("retries on transient failure then succeeds", async () => {
    mockFetch([
      { status: 500, body: {} },
      { status: 200, body: { jsonrpc: "2.0", id: 2, result: "0xff" } },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 2);
    const num = await rpc.getBlockNumber();
    expect(num).toBe(255);
    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
  });

  it("records throttle on 429", async () => {
    const limiter: RateLimiter = {
      acquire: async () => {},
      recordThrottle: vi.fn(),
    };

    mockFetch([
      { status: 429, body: {} },
      { status: 200, body: { jsonrpc: "2.0", id: 2, result: "0x1" } },
    ]);

    const rpc = new RpcClient("http://test", limiter, 2);
    await rpc.getBlockNumber();
    expect(limiter.recordThrottle).toHaveBeenCalledOnce();
  });

  it("circuit breaker opens after windowed failure rate exceeded", async () => {
    vi.useFakeTimers();

    mockFetch([
      { status: 500, body: {} },
      { status: 500, body: {} },
      { status: 500, body: {} },
      { status: 500, body: {} },
    ]);

    // failureRateThreshold=0.6, windowSize=3, maxRetries=0
    // After 3 failures in a window of 3: 100% failure rate → opens
    const rpc = new RpcClient("http://test", stubLimiter, 0, 0.6, 60_000, 3);

    await expect(rpc.getBlockNumber()).rejects.toThrow(); // 1/1 = 100% but window not full
    await expect(rpc.getBlockNumber()).rejects.toThrow(); // 2/2 = 100% but window not full
    await expect(rpc.getBlockNumber()).rejects.toThrow(); // 3/3 = 100% ≥ 60% → opens

    // Next call rejected by circuit breaker
    await expect(rpc.getBlockNumber()).rejects.toThrow("Circuit breaker OPEN");

    vi.useRealTimers();
  });

  it("throws on JSON-RPC error", async () => {
    mockFetch([
      {
        status: 200,
        body: {
          jsonrpc: "2.0",
          id: 1,
          error: { code: -32600, message: "Invalid Request" },
        },
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    await expect(rpc.getBlockNumber()).rejects.toThrow("Invalid Request");
  });

  it("throws JsonRpcError with code on JSON-RPC error", async () => {
    mockFetch([
      {
        status: 200,
        body: {
          jsonrpc: "2.0",
          id: 1,
          error: { code: -32601, message: "Method not found" },
        },
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    await expect(rpc.getBlockNumber()).rejects.toSatisfy((err: unknown) => {
      return err instanceof JsonRpcError && err.code === -32601;
    });
  });

  it("getFinalizedBlockNumber throws JsonRpcError -32601 when method not supported", async () => {
    mockFetch([
      {
        status: 200,
        body: {
          jsonrpc: "2.0",
          id: 1,
          error: { code: -32601, message: "The method eth_getBlockByNumber does not exist" },
        },
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    try {
      await rpc.getFinalizedBlockNumber();
      expect.unreachable("should have thrown");
    } catch (err) {
      expect(isMethodNotSupported(err)).toBe(true);
    }
  });

  it("getFinalizedBlockNumber does NOT match isMethodNotSupported for other RPC errors", async () => {
    mockFetch([
      {
        status: 200,
        body: {
          jsonrpc: "2.0",
          id: 1,
          error: { code: -32000, message: "Server error" },
        },
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    try {
      await rpc.getFinalizedBlockNumber();
      expect.unreachable("should have thrown");
    } catch (err) {
      expect(isMethodNotSupported(err)).toBe(false);
      expect(err).toBeInstanceOf(JsonRpcError);
      expect((err as JsonRpcError).code).toBe(-32000);
    }
  });

  it("throws JsonRpcError with code from batch calls (non-receipt errors)", async () => {
    mockFetch([
      {
        status: 200,
        body: [
          { jsonrpc: "2.0", id: 1, error: { code: -32000, message: "Server error" } },
          { jsonrpc: "2.0", id: 2, result: [] },
        ],
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    await expect(rpc.getBlockWithReceipts(10)).rejects.toSatisfy((err: unknown) => {
      return err instanceof JsonRpcError && err.code === -32000;
    });
  });

  it("falls back to per-tx receipts when eth_getBlockReceipts returns -32601", async () => {
    const blockData = {
      number: "0xa",
      hash: "0xabc",
      parentHash: "0xdef",
      timestamp: "0x60",
      gasUsed: "0x100",
      gasLimit: "0x200",
      transactions: [
        { hash: "0xtx1", blockNumber: "0xa", transactionIndex: "0x0", from: "0x1", to: "0x2", value: "0x0", gas: "0x0", input: "0x", nonce: "0x0" },
      ],
    };
    const receiptData = {
      transactionHash: "0xtx1",
      status: "0x1",
      cumulativeGasUsed: "0x100",
      gasUsed: "0x50",
      contractAddress: null,
      logs: [],
    };

    mockFetch([
      // First call: batch [eth_getBlockByNumber, eth_getBlockReceipts] — receipts returns -32601
      {
        status: 200,
        body: [
          { jsonrpc: "2.0", id: 1, result: blockData },
          { jsonrpc: "2.0", id: 2, error: { code: -32601, message: "Method not found" } },
        ],
      },
      // Second call: fallback eth_getBlockByNumber (single)
      { status: 200, body: { jsonrpc: "2.0", id: 3, result: blockData } },
      // Third call: batch [eth_getTransactionReceipt] per tx
      {
        status: 200,
        body: [
          { jsonrpc: "2.0", id: 4, result: receiptData },
        ],
      },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);
    const { block, receipts } = await rpc.getBlockWithReceipts(10);

    expect(block.hash).toBe("0xabc");
    expect(receipts).toHaveLength(1);
    expect(receipts[0]!.transactionHash).toBe("0xtx1");
  });

  it("caches the fallback flag — second call skips eth_getBlockReceipts", async () => {
    const blockData = {
      number: "0xa",
      hash: "0xabc",
      parentHash: "0xdef",
      timestamp: "0x60",
      gasUsed: "0x100",
      gasLimit: "0x200",
      transactions: [],
    };

    mockFetch([
      // First getBlockWithReceipts: batch fails with -32601
      {
        status: 200,
        body: [
          { jsonrpc: "2.0", id: 1, result: blockData },
          { jsonrpc: "2.0", id: 2, error: { code: -32601, message: "Method not found" } },
        ],
      },
      // Fallback: individual eth_getBlockByNumber
      { status: 200, body: { jsonrpc: "2.0", id: 3, result: blockData } },
      // Second getBlockWithReceipts: goes straight to fallback (no batch attempt)
      { status: 200, body: { jsonrpc: "2.0", id: 4, result: blockData } },
    ]);

    const rpc = new RpcClient("http://test", stubLimiter, 0);

    // First call triggers fallback
    await rpc.getBlockWithReceipts(10);
    // Second call should use cached flag — no batch call, just individual fetch
    const { block } = await rpc.getBlockWithReceipts(11);
    expect(block.hash).toBe("0xabc");

    // 3 fetch calls total: batch (failed), fallback block, second fallback block
    expect(globalThis.fetch).toHaveBeenCalledTimes(3);
  });
});

describe("isMethodNotSupported", () => {
  it("returns true for JsonRpcError with code -32601", () => {
    expect(isMethodNotSupported(new JsonRpcError(-32601, "Method not found"))).toBe(true);
  });

  it("returns false for JsonRpcError with different code", () => {
    expect(isMethodNotSupported(new JsonRpcError(-32600, "Invalid Request"))).toBe(false);
    expect(isMethodNotSupported(new JsonRpcError(-32000, "Server error"))).toBe(false);
  });

  it("returns false for plain Error", () => {
    expect(isMethodNotSupported(new Error("network timeout"))).toBe(false);
  });

  it("returns false for non-error values", () => {
    expect(isMethodNotSupported(null)).toBe(false);
    expect(isMethodNotSupported(undefined)).toBe(false);
    expect(isMethodNotSupported("string error")).toBe(false);
  });
});
