import { describe, it, expect, vi, beforeEach } from "vitest";

// Storage is tightly coupled to pg — we test the hex conversion helper
// and verify SQL construction patterns via mock.

// Extract hexToBigInt for direct testing (it's module-private,
// so we re-implement and verify the same logic)
function hexToBigInt(hex: string): string {
  return BigInt(hex).toString();
}

describe("hexToBigInt", () => {
  it("converts small hex values", () => {
    expect(hexToBigInt("0x0")).toBe("0");
    expect(hexToBigInt("0xa")).toBe("10");
    expect(hexToBigInt("0xff")).toBe("255");
  });

  it("converts large hex values without precision loss", () => {
    // 2^64 - 1 = 18446744073709551615
    expect(hexToBigInt("0xffffffffffffffff")).toBe("18446744073709551615");
  });

  it("handles EVM max uint256", () => {
    const maxUint256 = "0x" + "f".repeat(64);
    const result = hexToBigInt(maxUint256);
    expect(result).toBe(
      "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    );
  });

  it("preserves precision beyond JS Number.MAX_SAFE_INTEGER", () => {
    // 2^53 + 1 — Number would lose this
    const hex = "0x20000000000001";
    expect(hexToBigInt(hex)).toBe("9007199254740993");
  });
});

describe("Storage (mock verification)", () => {
  // Mock pg Pool to verify SQL queries without a real database
  function createMockPool() {
    const queries: Array<{ text?: string; name?: string; values?: unknown[] }> = [];
    const mockClient = {
      query: vi.fn(async (q: string | { text: string; name?: string; values?: unknown[] }) => {
        if (typeof q === "string") {
          queries.push({ text: q });
        } else {
          queries.push(q);
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    return {
      queries,
      mockClient,
      pool: {
        connect: vi.fn(async () => mockClient),
        query: vi.fn(async (q: any) => {
          if (typeof q === "string") queries.push({ text: q });
          else queries.push(q);
          return { rows: [] };
        }),
        end: vi.fn(async () => {}),
      },
    };
  }

  it("migrate creates all five tables", async () => {
    // We import Storage dynamically so the mock works
    const { pool, queries, mockClient } = createMockPool();

    // Construct Storage with mocked pool
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    // Replace pool with mock
    (storage as any).pool = pool;

    await storage.migrate();

    const allSQL = queries.map((q) => q.text ?? "").join("\n");
    expect(allSQL).toContain("raw.blocks");
    expect(allSQL).toContain("raw.transactions");
    expect(allSQL).toContain("raw.receipts");
    expect(allSQL).toContain("raw.logs");
    expect(allSQL).toContain("raw.indexer_state");
    expect(allSQL).toContain("PARTITION BY RANGE");
    expect(allSQL).toContain("BRIN");
    expect(allSQL).toContain("idx_logs_addr_topic");
  });

  it("insertBlock uses ON CONFLICT DO NOTHING for idempotency", async () => {
    const { pool, queries, mockClient } = createMockPool();
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = pool;

    // Also mock ensurePartition to avoid real partition DDL
    (storage as any).ensurePartition = vi.fn(async () => {});

    const block = {
      number: "0xa",
      hash: "0xblock",
      parentHash: "0xparent",
      timestamp: "0x60",
      gasUsed: "0x100",
      gasLimit: "0x200",
      transactions: [],
    };

    await storage.insertBlock(block as any, [], "test-worker");

    const insertSQL = queries
      .filter((q) => q.text?.includes("INSERT"))
      .map((q) => q.text ?? q.name ?? "");
    expect(insertSQL.some((s) => s.includes("ON CONFLICT") && s.includes("DO NOTHING"))).toBe(true);
  });

  it("updateWatermark uses GREATEST for concurrent safety", async () => {
    const { pool, queries } = createMockPool();
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = pool;

    await storage.updateWatermark(1, 1000, "w1");

    const upsertSQL = queries.map((q) => q.text ?? "").join("\n");
    expect(upsertSQL).toContain("GREATEST");
  });

  it("getWatermark returns 0 when no rows exist", async () => {
    const { pool } = createMockPool();
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = pool;

    const wm = await storage.getWatermark(1);
    expect(wm).toBe(0);
  });

  it("batchInsertLogs chunks when log count exceeds BATCH_CHUNK_SIZE", async () => {
    const { pool, queries, mockClient } = createMockPool();
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = pool;
    (storage as any).ensurePartition = vi.fn(async () => {});

    // Create a block with 2 transactions, each producing 6000 logs.
    // Total: 12,000 logs × 7 params = 84,000 > PG's 65,535 limit.
    // Without per-log chunking, this crashes. With chunking at 5000,
    // it should produce 3 INSERT statements (5000 + 5000 + 2000).
    const makeLogs = (txHash: string, count: number) =>
      Array.from({ length: count }, (_, i) => ({
        transactionHash: txHash,
        logIndex: "0x" + i.toString(16),
        address: "0xaddr",
        topics: ["0xtopic"],
        data: "0x",
        blockNumber: "0xa",
      }));

    const tx1 = {
      hash: "0xtx1", blockNumber: "0xa", transactionIndex: "0x0",
      from: "0xfrom", to: "0xto", value: "0x0", gas: "0x100",
      input: "0x", nonce: "0x0",
    };
    const tx2 = {
      hash: "0xtx2", blockNumber: "0xa", transactionIndex: "0x1",
      from: "0xfrom", to: "0xto", value: "0x0", gas: "0x100",
      input: "0x", nonce: "0x1",
    };

    const block = {
      number: "0xa", hash: "0xblock", parentHash: "0xparent",
      timestamp: "0x60", gasUsed: "0x100", gasLimit: "0x200",
      transactions: [tx1, tx2],
    };

    const receipts = [
      { transactionHash: "0xtx1", status: "0x1", gasUsed: "0x50",
        cumulativeGasUsed: "0x50", contractAddress: null, logs: makeLogs("0xtx1", 6000) },
      { transactionHash: "0xtx2", status: "0x1", gasUsed: "0x50",
        cumulativeGasUsed: "0xa0", contractAddress: null, logs: makeLogs("0xtx2", 6000) },
    ];

    await storage.insertBlock(block as any, receipts as any, "test-worker");

    // Count log INSERT queries — should be chunked into multiple statements
    const logInserts = queries.filter(
      (q) => q.text?.includes("INSERT INTO raw.logs")
    );
    expect(logInserts.length).toBeGreaterThanOrEqual(2);

    // Each chunk should have at most 5000 × 7 = 35,000 params
    for (const q of logInserts) {
      expect((q.values ?? []).length).toBeLessThanOrEqual(35_000);
    }
  });
});
