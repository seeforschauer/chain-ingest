import { describe, it, expect, vi, beforeEach } from "vitest";

// Storage is tightly coupled to pg — we test the hex conversion helper
// and verify SQL construction patterns via mock.

// Extract hexToBigInt for direct testing (it's module-private,
// so we re-implement and verify the same logic)
function hexToBigInt(hex: string): string {
  return BigInt(hex).toString();
}

function hexToBytes(hex: string): Buffer {
  return Buffer.from(hex.startsWith("0x") ? hex.slice(2) : hex, "hex");
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

describe("hexToBytes", () => {
  it("converts hex string to Buffer", () => {
    const buf = hexToBytes("0xabcdef");
    expect(buf).toBeInstanceOf(Buffer);
    expect(buf.toString("hex")).toBe("abcdef");
  });

  it("handles 32-byte hash", () => {
    const hash = "0x" + "ab".repeat(32);
    const buf = hexToBytes(hash);
    expect(buf.length).toBe(32);
  });

  it("handles 20-byte address", () => {
    const addr = "0x" + "cd".repeat(20);
    const buf = hexToBytes(addr);
    expect(buf.length).toBe(20);
  });

  it("saves 50%+ vs TEXT storage", () => {
    // TEXT: "0x" + 64 hex chars + varlena = ~67 bytes
    // BYTEA: 32 bytes + varlena = ~33 bytes
    const hash = "0x" + "ab".repeat(32);
    const textSize = hash.length; // 66 chars
    const byteaSize = hexToBytes(hash).length; // 32 bytes
    expect(byteaSize).toBeLessThan(textSize / 2);
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

  it("migrate creates all five tables with BYTEA columns", async () => {
    const { pool, queries, mockClient } = createMockPool();

    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
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
    // BYTEA columns
    expect(allSQL).toContain("block_hash      BYTEA");
    expect(allSQL).toContain("tx_hash                  BYTEA");
    expect(allSQL).toContain("from_address             BYTEA");
    expect(allSQL).toContain("address          BYTEA");
    // EIP-1559/4844 columns
    expect(allSQL).toContain("blob_gas_used");
    expect(allSQL).toContain("excess_blob_gas");
    expect(allSQL).toContain("tx_type");
    expect(allSQL).toContain("max_fee_per_gas");
    expect(allSQL).toContain("max_priority_fee_per_gas");
    expect(allSQL).toContain("max_fee_per_blob_gas");
    expect(allSQL).toContain("blob_versioned_hashes");
    // tx_type partial index
    expect(allSQL).toContain("idx_tx_type");
  });

  it("insertBlock uses ON CONFLICT DO NOTHING for idempotency", async () => {
    const { pool, queries, mockClient } = createMockPool();
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = pool;

    (storage as any).ensurePartition = vi.fn(async () => {});

    const block = {
      number: "0xa",
      hash: "0xabababababababababababababababababababababababababababababababab01",
      parentHash: "0xabababababababababababababababababababababababababababababababab02",
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

  it("insertBlock passes Buffer values for BYTEA columns", async () => {
    const { pool, queries, mockClient } = createMockPool();
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = pool;
    (storage as any).ensurePartition = vi.fn(async () => {});

    const block = {
      number: "0xa",
      hash: "0x" + "ab".repeat(32),
      parentHash: "0x" + "cd".repeat(32),
      timestamp: "0x60",
      gasUsed: "0x100",
      gasLimit: "0x200",
      transactions: [{
        hash: "0x" + "ef".repeat(32),
        blockNumber: "0xa",
        transactionIndex: "0x0",
        from: "0x" + "11".repeat(20),
        to: "0x" + "22".repeat(20),
        value: "0x0",
        gas: "0x100",
        input: "0x",
        nonce: "0x0",
      }],
    };

    const receipts = [{
      transactionHash: "0x" + "ef".repeat(32),
      status: "0x1",
      gasUsed: "0x50",
      cumulativeGasUsed: "0x50",
      contractAddress: null,
      logs: [{
        transactionHash: "0x" + "ef".repeat(32),
        logIndex: "0x0",
        address: "0x" + "33".repeat(20),
        topics: ["0x" + "44".repeat(32)],
        data: "0x",
        blockNumber: "0xa",
      }],
    }];

    await storage.insertBlock(block as any, receipts as any, "test-worker");

    // Block insert should have Buffer values for hash columns
    const blockInsert = mockClient.query.mock.calls.find(
      (call: any) => typeof call[0] === 'string' && call[0].includes("INSERT INTO raw.blocks")
    );
    expect(blockInsert).toBeDefined();
    const blockValues = (blockInsert as unknown as any[])[1] as any[];
    expect(Buffer.isBuffer(blockValues[1])).toBe(true); // block_hash
    expect(Buffer.isBuffer(blockValues[2])).toBe(true); // parent_hash
  });

  it("insertBlock stores EIP-1559 fields", async () => {
    const { pool, queries, mockClient } = createMockPool();
    const { Storage } = await import("./storage.js");
    const storage = new Storage("postgresql://test");
    (storage as any).pool = pool;
    (storage as any).ensurePartition = vi.fn(async () => {});

    const block = {
      number: "0xa",
      hash: "0x" + "ab".repeat(32),
      parentHash: "0x" + "cd".repeat(32),
      timestamp: "0x60",
      gasUsed: "0x100",
      gasLimit: "0x200",
      baseFeePerGas: "0x3b9aca00",
      blobGasUsed: "0x20000",
      excessBlobGas: "0x10000",
      transactions: [{
        hash: "0x" + "ef".repeat(32),
        blockNumber: "0xa",
        transactionIndex: "0x0",
        from: "0x" + "11".repeat(20),
        to: "0x" + "22".repeat(20),
        value: "0x0",
        gas: "0x100",
        gasPrice: "0x3b9aca00",
        input: "0x",
        nonce: "0x0",
        type: "0x2",
        maxFeePerGas: "0x77359400",
        maxPriorityFeePerGas: "0x3b9aca00",
      }],
    };

    await storage.insertBlock(block as any, [], "test-worker");

    // Verify EIP-1559/4844 columns are in the INSERT
    const txInsert = queries.find((q) => q.text?.includes("INSERT INTO raw.transactions"));
    expect(txInsert?.text).toContain("tx_type");
    expect(txInsert?.text).toContain("max_fee_per_gas");
    expect(txInsert?.text).toContain("max_priority_fee_per_gas");

    const blockInsert = queries.find((q) => q.text?.includes("INSERT INTO raw.blocks"));
    expect(blockInsert?.text).toContain("blob_gas_used");
    expect(blockInsert?.text).toContain("excess_blob_gas");
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
        address: "0x" + "aa".repeat(20),
        topics: ["0x" + "bb".repeat(32)],
        data: "0x",
        blockNumber: "0xa",
      }));

    const tx1 = {
      hash: "0x" + "c1".repeat(32), blockNumber: "0xa", transactionIndex: "0x0",
      from: "0x" + "d1".repeat(20), to: "0x" + "d2".repeat(20), value: "0x0", gas: "0x100",
      input: "0x", nonce: "0x0",
    };
    const tx2 = {
      hash: "0x" + "c2".repeat(32), blockNumber: "0xa", transactionIndex: "0x1",
      from: "0x" + "d1".repeat(20), to: "0x" + "d2".repeat(20), value: "0x0", gas: "0x100",
      input: "0x", nonce: "0x1",
    };

    const block = {
      number: "0xa", hash: "0x" + "e1".repeat(32), parentHash: "0x" + "e2".repeat(32),
      timestamp: "0x60", gasUsed: "0x100", gasLimit: "0x200",
      transactions: [tx1, tx2],
    };

    const receipts = [
      { transactionHash: "0x" + "c1".repeat(32), status: "0x1", gasUsed: "0x50",
        cumulativeGasUsed: "0x50", contractAddress: null, logs: makeLogs("0x" + "c1".repeat(32), 6000) },
      { transactionHash: "0x" + "c2".repeat(32), status: "0x1", gasUsed: "0x50",
        cumulativeGasUsed: "0xa0", contractAddress: null, logs: makeLogs("0x" + "c2".repeat(32), 6000) },
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
