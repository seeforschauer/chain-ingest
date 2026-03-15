import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { RpcClient } from "./rpc.js";
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
});
