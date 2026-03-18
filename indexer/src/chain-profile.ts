// Per-chain configuration profile.
//
// Different EVM chains have fundamentally different finality semantics,
// block times, RPC quirks, and rate limit budgets. A single-chain heuristic
// ("finalized with latest fallback") breaks silently on heterogeneous chains:
//   - Polygon: 128-block finality via Heimdall checkpoints, not "finalized" tag
//   - Arbitrum: L2 with instant soft-finality, batch posts to L1 for hard finality
//   - BSC: 15-block finality, 3-second block time, 45M+ blocks
//   - Ethereum: 2-epoch (~12 min) finality via Casper FFG
//
// ChainProfile encapsulates these differences so the indexer adapts per-chain
// without if/else chains in the hot path.

import type { RpcEndpoint } from "./rpc.js";
import { isMethodNotSupported } from "./rpc.js";
import { log } from "./logger.js";

export interface ChainProfile {
  readonly chainId: number;
  readonly name: string;
  /** Average block time in seconds — used for progress estimation. */
  readonly blockTimeSec: number;
  /** Finality semantics: how this chain determines a block is final. */
  readonly finalityMode: "tag" | "depth" | "instant";
  /** For depth-based finality: number of confirmations required. */
  readonly finalityDepth: number;
  /** Default rate limit ceiling for this chain's RPC providers. */
  readonly defaultRateLimit: number;
  /** Whether eth_getBlockReceipts is expected to be available. */
  readonly supportsBlockReceipts: boolean;
  /** Maximum transactions per block (affects chunking). L2s can have 10K+. */
  readonly maxTxPerBlock: number;
}

/** Known chain profiles — covers the most common EVM chains at TT's scale. */
const PROFILES: Record<number, ChainProfile> = {
  1: {
    chainId: 1,
    name: "Ethereum",
    blockTimeSec: 12,
    finalityMode: "tag",
    finalityDepth: 0,
    defaultRateLimit: 50,
    supportsBlockReceipts: true,
    maxTxPerBlock: 500,
  },
  137: {
    chainId: 137,
    name: "Polygon",
    blockTimeSec: 2,
    finalityMode: "depth",
    finalityDepth: 128,
    defaultRateLimit: 100,
    supportsBlockReceipts: true,
    maxTxPerBlock: 1000,
  },
  42161: {
    chainId: 42161,
    name: "Arbitrum One",
    blockTimeSec: 0.25,
    finalityMode: "instant",
    finalityDepth: 0,
    defaultRateLimit: 100,
    supportsBlockReceipts: false,
    maxTxPerBlock: 10000,
  },
  10: {
    chainId: 10,
    name: "Optimism",
    blockTimeSec: 2,
    finalityMode: "instant",
    finalityDepth: 0,
    defaultRateLimit: 100,
    supportsBlockReceipts: false,
    maxTxPerBlock: 5000,
  },
  56: {
    chainId: 56,
    name: "BNB Chain",
    blockTimeSec: 3,
    finalityMode: "depth",
    finalityDepth: 15,
    defaultRateLimit: 100,
    supportsBlockReceipts: true,
    maxTxPerBlock: 2000,
  },
  8453: {
    chainId: 8453,
    name: "Base",
    blockTimeSec: 2,
    finalityMode: "instant",
    finalityDepth: 0,
    defaultRateLimit: 100,
    supportsBlockReceipts: false,
    maxTxPerBlock: 5000,
  },
  43114: {
    chainId: 43114,
    name: "Avalanche C-Chain",
    blockTimeSec: 2,
    finalityMode: "tag",
    finalityDepth: 0,
    defaultRateLimit: 50,
    supportsBlockReceipts: true,
    maxTxPerBlock: 1000,
  },
};

/** Default profile for unknown chains — conservative settings. */
function defaultProfile(chainId: number): ChainProfile {
  return {
    chainId,
    name: `Chain ${chainId}`,
    blockTimeSec: 12,
    finalityMode: "tag",
    finalityDepth: 0,
    defaultRateLimit: 50,
    supportsBlockReceipts: true,
    maxTxPerBlock: 500,
  };
}

/** Get the chain profile for a given chain ID. Falls back to conservative defaults. */
export function getChainProfile(chainId: number): ChainProfile {
  return PROFILES[chainId] ?? defaultProfile(chainId);
}

/**
 * Resolve the end block number using chain-appropriate finality semantics.
 *
 * - tag: uses `eth_getBlockByNumber("finalized")` with fallback to `latest`
 * - depth: uses `eth_blockNumber() - finalityDepth`
 * - instant: uses `eth_blockNumber()` directly (L2 soft-finality)
 */
export async function resolveEndBlock(
  rpc: RpcEndpoint,
  profile: ChainProfile,
  configEndBlock: number | "latest" | "finalized"
): Promise<{ endBlock: number; resolvedFinality: string }> {
  if (typeof configEndBlock === "number") {
    return { endBlock: configEndBlock, resolvedFinality: "explicit" };
  }

  if (configEndBlock === "latest") {
    const endBlock = await rpc.getBlockNumber();
    return { endBlock, resolvedFinality: "latest" };
  }

  // "finalized" — resolve per chain profile
  switch (profile.finalityMode) {
    case "tag": {
      try {
        const endBlock = await rpc.getFinalizedBlockNumber();
        return { endBlock, resolvedFinality: "finalized-tag" };
      } catch (err) {
        if (!isMethodNotSupported(err)) throw err;
        log("warn", `${profile.name}: RPC does not support 'finalized' tag (-32601), falling back to latest`);
        const endBlock = await rpc.getBlockNumber();
        return { endBlock, resolvedFinality: "latest-fallback" };
      }
    }
    case "depth": {
      const latest = await rpc.getBlockNumber();
      const endBlock = Math.max(0, latest - profile.finalityDepth);
      log("info", `${profile.name}: using depth-based finality`, {
        latest,
        depth: profile.finalityDepth,
        finalized: endBlock,
      });
      return { endBlock, resolvedFinality: `depth-${profile.finalityDepth}` };
    }
    case "instant": {
      const endBlock = await rpc.getBlockNumber();
      log("info", `${profile.name}: L2 instant finality — using latest block`);
      return { endBlock, resolvedFinality: "instant" };
    }
  }
}
