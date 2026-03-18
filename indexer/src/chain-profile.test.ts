import { describe, it, expect, vi } from "vitest";
import { getChainProfile, resolveEndBlock } from "./chain-profile.js";
import type { RpcEndpoint } from "./rpc.js";
import { JsonRpcError } from "./rpc.js";

describe("getChainProfile", () => {
  it("returns Ethereum profile for chainId 1", () => {
    const profile = getChainProfile(1);
    expect(profile.name).toBe("Ethereum");
    expect(profile.finalityMode).toBe("tag");
    expect(profile.blockTimeSec).toBe(12);
  });

  it("returns Polygon profile for chainId 137", () => {
    const profile = getChainProfile(137);
    expect(profile.name).toBe("Polygon");
    expect(profile.finalityMode).toBe("depth");
    expect(profile.finalityDepth).toBe(128);
  });

  it("returns Arbitrum profile for chainId 42161", () => {
    const profile = getChainProfile(42161);
    expect(profile.name).toBe("Arbitrum One");
    expect(profile.finalityMode).toBe("instant");
    expect(profile.maxTxPerBlock).toBe(10000);
  });

  it("returns BSC profile for chainId 56", () => {
    const profile = getChainProfile(56);
    expect(profile.name).toBe("BNB Chain");
    expect(profile.finalityMode).toBe("depth");
    expect(profile.finalityDepth).toBe(15);
  });

  it("returns default profile for unknown chain", () => {
    const profile = getChainProfile(99999);
    expect(profile.name).toBe("Chain 99999");
    expect(profile.finalityMode).toBe("tag");
    expect(profile.defaultRateLimit).toBe(50);
  });
});

describe("resolveEndBlock", () => {
  function createMockRpc(overrides: Partial<RpcEndpoint> = {}): RpcEndpoint {
    return {
      getBlockNumber: vi.fn(async () => 20_000_000),
      getFinalizedBlockNumber: vi.fn(async () => 19_999_900),
      getBlockWithReceipts: vi.fn(async () => ({
        block: {} as any,
        receipts: [],
      })),
      ...overrides,
    };
  }

  it("returns explicit block number unchanged", async () => {
    const rpc = createMockRpc();
    const profile = getChainProfile(1);
    const result = await resolveEndBlock(rpc, profile, 1000);
    expect(result).toEqual({ endBlock: 1000, resolvedFinality: "explicit" });
    expect(rpc.getBlockNumber).not.toHaveBeenCalled();
  });

  it("returns latest block for 'latest' config", async () => {
    const rpc = createMockRpc();
    const profile = getChainProfile(1);
    const result = await resolveEndBlock(rpc, profile, "latest");
    expect(result).toEqual({ endBlock: 20_000_000, resolvedFinality: "latest" });
  });

  it("uses finalized tag for Ethereum", async () => {
    const rpc = createMockRpc();
    const profile = getChainProfile(1);
    const result = await resolveEndBlock(rpc, profile, "finalized");
    expect(result).toEqual({ endBlock: 19_999_900, resolvedFinality: "finalized-tag" });
    expect(rpc.getFinalizedBlockNumber).toHaveBeenCalled();
  });

  it("falls back to latest when finalized tag not supported", async () => {
    const rpc = createMockRpc({
      getFinalizedBlockNumber: vi.fn(async () => {
        throw new JsonRpcError(-32601, "Method not found");
      }),
    });
    const profile = getChainProfile(1);
    const result = await resolveEndBlock(rpc, profile, "finalized");
    expect(result.resolvedFinality).toBe("latest-fallback");
    expect(result.endBlock).toBe(20_000_000);
  });

  it("propagates non-method-not-found errors", async () => {
    const rpc = createMockRpc({
      getFinalizedBlockNumber: vi.fn(async () => {
        throw new Error("connection timeout");
      }),
    });
    const profile = getChainProfile(1);
    await expect(resolveEndBlock(rpc, profile, "finalized")).rejects.toThrow("connection timeout");
  });

  it("uses depth-based finality for Polygon", async () => {
    const rpc = createMockRpc();
    const profile = getChainProfile(137);
    const result = await resolveEndBlock(rpc, profile, "finalized");
    expect(result.resolvedFinality).toBe("depth-128");
    expect(result.endBlock).toBe(20_000_000 - 128);
    expect(rpc.getFinalizedBlockNumber).not.toHaveBeenCalled();
  });

  it("uses instant finality for Arbitrum", async () => {
    const rpc = createMockRpc();
    const profile = getChainProfile(42161);
    const result = await resolveEndBlock(rpc, profile, "finalized");
    expect(result.resolvedFinality).toBe("instant");
    expect(result.endBlock).toBe(20_000_000);
  });

  it("depth-based finality never goes below 0", async () => {
    const rpc = createMockRpc({
      getBlockNumber: vi.fn(async () => 50),
    });
    const profile = getChainProfile(137); // depth 128
    const result = await resolveEndBlock(rpc, profile, "finalized");
    expect(result.endBlock).toBe(0);
  });
});
