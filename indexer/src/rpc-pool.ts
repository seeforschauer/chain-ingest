// Multi-endpoint RPC pool with round-robin selection and failover.

import { RpcClient } from "./rpc.js";
import type { BlockData, ReceiptData, RpcEndpoint } from "./rpc.js";
import type { RateLimiter } from "./rate-limiter.js";
import { log } from "./logger.js";

export class RpcPool implements RpcEndpoint {
  private readonly clients: RpcClient[];
  private index = 0;

  constructor(
    urls: string[],
    rateLimiter: RateLimiter,
    maxRetries: number,
    failureThreshold?: number,
    cooldownMs?: number
  ) {
    if (urls.length === 0) {
      throw new Error("RpcPool requires at least one URL");
    }
    this.clients = urls.map(
      (url) => new RpcClient(url, rateLimiter, maxRetries, failureThreshold, cooldownMs)
    );
    log("info", "RPC pool initialized", { endpoints: urls.length });
  }

  private next(): number {
    const idx = this.index;
    this.index = (this.index + 1) % this.clients.length;
    return idx;
  }

  private async withFailover<T>(fn: (client: RpcClient) => Promise<T>): Promise<T> {
    const startIdx = this.next();
    let lastError: Error | undefined;

    for (let i = 0; i < this.clients.length; i++) {
      const idx = (startIdx + i) % this.clients.length;
      try {
        return await fn(this.clients[idx]!);
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        log("warn", "RPC endpoint failed, trying next", {
          failedIndex: idx,
          error: lastError.message,
          remaining: this.clients.length - i - 1,
        });
      }
    }

    throw lastError ?? new Error("All RPC endpoints exhausted");
  }

  async getBlockNumber(): Promise<number> {
    return this.withFailover((c) => c.getBlockNumber());
  }

  async getFinalizedBlockNumber(): Promise<number> {
    return this.withFailover((c) => c.getFinalizedBlockNumber());
  }

  async getBlockWithReceipts(
    blockNumber: number
  ): Promise<{ block: BlockData; receipts: ReceiptData[] }> {
    return this.withFailover((c) => c.getBlockWithReceipts(blockNumber));
  }
}
