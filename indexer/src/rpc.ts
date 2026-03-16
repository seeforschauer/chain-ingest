// JSON-RPC client with retry, circuit breaker, and request batching.

import { log } from "./logger.js";
import type { RateLimiter } from "./rate-limiter.js";

export interface BlockData {
  number: string; // hex
  hash: string;
  parentHash: string;
  timestamp: string; // hex
  gasUsed: string;
  gasLimit: string;
  baseFeePerGas?: string;
  transactions: TransactionData[];
}

export interface TransactionData {
  hash: string;
  blockNumber: string;
  transactionIndex: string;
  from: string;
  to: string | null;
  value: string;
  gasPrice?: string;
  gas: string;
  input: string;
  nonce: string;
}

export interface ReceiptData {
  transactionHash: string;
  status: string;
  cumulativeGasUsed: string;
  gasUsed: string;
  contractAddress: string | null;
  logs: LogData[];
}

export interface LogData {
  address: string;
  topics: string[];
  data: string;
  logIndex: string;
  transactionHash: string;
  blockNumber: string;
}

export interface RpcEndpoint {
  getBlockNumber(): Promise<number>;
  getFinalizedBlockNumber(): Promise<number>;
  getBlockWithReceipts(
    blockNumber: number
  ): Promise<{ block: BlockData; receipts: ReceiptData[] }>;
}

/** JSON-RPC error with the original numeric error code preserved. */
export class JsonRpcError extends Error {
  constructor(
    public readonly code: number,
    message: string
  ) {
    super(`RPC error ${code}: ${message}`);
    this.name = "JsonRpcError";
  }
}

/** Returns true if the error is a JSON-RPC "method not found" (-32601). */
export function isMethodNotSupported(err: unknown): boolean {
  return err instanceof JsonRpcError && err.code === -32601;
}

interface JsonRpcResponse<T> {
  jsonrpc: string;
  id: number;
  result?: T;
  error?: { code: number; message: string };
}

const METHOD_TIMEOUTS: Record<string, number> = {
  eth_blockNumber: 10_000,
  eth_getBlockByNumber: 15_000,
  eth_getBlockReceipts: 30_000,
  eth_getTransactionReceipt: 15_000,
};
const DEFAULT_TIMEOUT = 15_000;

enum CircuitState {
  CLOSED,
  OPEN,
  HALF_OPEN,
}

export class RpcClient implements RpcEndpoint {
  private id = 0;
  private circuitState = CircuitState.CLOSED;
  private circuitOpenedAt = 0;
  private readonly failureRateThreshold: number;
  private readonly windowSize: number;
  private readonly cooldownMs: number;

  // Ring buffer for O(1) outcome tracking (replaces O(n) array.shift + filter)
  private readonly outcomeRing: boolean[];
  private ringHead = 0;
  private ringCount = 0;
  private failureCount = 0;
  private supportsBlockReceipts = true;

  constructor(
    private readonly url: string,
    private readonly rateLimiter: RateLimiter,
    private readonly maxRetries: number,
    failureRateThreshold = 0.6,
    cooldownMs = 30_000,
    windowSize = 20
  ) {
    this.failureRateThreshold = failureRateThreshold;
    this.cooldownMs = cooldownMs;
    this.windowSize = windowSize;
    this.outcomeRing = new Array<boolean>(windowSize);
  }

  private checkCircuit(): void {
    if (this.circuitState === CircuitState.CLOSED) return;

    if (this.circuitState === CircuitState.OPEN) {
      const elapsed = Date.now() - this.circuitOpenedAt;
      if (elapsed >= this.cooldownMs) {
        this.circuitState = CircuitState.HALF_OPEN;
        log("info", "Circuit breaker half-open — allowing probe call", {
          url: this.url,
        });
        return;
      }
      throw new Error(
        `Circuit breaker OPEN — rejecting call (${Math.round((this.cooldownMs - elapsed) / 1000)}s remaining)`
      );
    }
  }

  private recordOutcome(success: boolean): void {
    if (this.ringCount === this.windowSize) {
      // Evict the oldest entry from the ring
      if (!this.outcomeRing[this.ringHead]) this.failureCount--;
    } else {
      this.ringCount++;
    }
    this.outcomeRing[this.ringHead] = success;
    if (!success) this.failureCount++;
    this.ringHead = (this.ringHead + 1) % this.windowSize;
  }

  private onSuccess(): void {
    this.recordOutcome(true);
    if (this.circuitState === CircuitState.HALF_OPEN) {
      log("info", "Circuit breaker closed — probe succeeded", {
        url: this.url,
      });
    }
    this.circuitState = CircuitState.CLOSED;
  }

  private onFailure(): void {
    this.recordOutcome(false);

    if (this.circuitState === CircuitState.HALF_OPEN) {
      this.circuitState = CircuitState.OPEN;
      this.circuitOpenedAt = Date.now();
      log("warn", "Circuit breaker OPEN — probe failed", { url: this.url });
      return;
    }

    if (this.ringCount >= this.windowSize) {
      if (this.failureCount / this.windowSize >= this.failureRateThreshold) {
        this.circuitState = CircuitState.OPEN;
        this.circuitOpenedAt = Date.now();
        log("warn", "Circuit breaker OPEN", {
          url: this.url,
          failureRate: (this.failureCount / this.windowSize * 100).toFixed(0) + "%",
          cooldownMs: this.cooldownMs,
        });
      }
    }
  }

  private async withRetry<T>(
    execute: () => Promise<Response>,
    parseResponse: (res: Response) => Promise<T>,
    label: string
  ): Promise<T> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      this.checkCircuit();
      await this.rateLimiter.acquire();

      // Flag-based 429 detection — string matching for control flow is fragile
      let rateLimited = false;

      try {
        const res = await execute();

        if (res.status === 429) {
          // 429 = healthy but overloaded. Rate limiter handles this, NOT circuit breaker.
          rateLimited = true;
          this.rateLimiter.recordThrottle?.();
          throw new Error("rate limited (429)");
        }

        if (!res.ok) {
          throw new Error(`RPC HTTP ${res.status}: ${res.statusText}`);
        }

        const result = await parseResponse(res);
        this.onSuccess();
        return result;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        if (!rateLimited) {
          this.onFailure();
        }

        if (attempt < this.maxRetries) {
          const delay = Math.min(1000 * Math.pow(2, attempt), 30_000);
          const jitter = Math.random() * 1000;
          log("warn", `RPC retry ${attempt + 1}/${this.maxRetries}`, {
            call: label,
            error: lastError.message,
            delayMs: Math.round(delay + jitter),
          });
          await sleep(delay + jitter);
        }
      }
    }

    throw lastError ?? new Error("RPC call failed");
  }

  private async call<T>(method: string, params: unknown[]): Promise<T> {
    const timeout = METHOD_TIMEOUTS[method] ?? DEFAULT_TIMEOUT;

    return this.withRetry<T>(
      () => {
        const body = JSON.stringify({
          jsonrpc: "2.0",
          method,
          params,
          id: ++this.id,
        });
        return fetch(this.url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
          signal: AbortSignal.timeout(timeout),
        });
      },
      async (res) => {
        const json = (await res.json()) as JsonRpcResponse<T>;
        if (json.error) {
          throw new JsonRpcError(json.error.code, json.error.message);
        }
        return json.result as T;
      },
      method
    );
  }

  private async batchCall<T extends unknown[]>(
    calls: Array<{ method: string; params: unknown[] }>
  ): Promise<T> {
    const timeout = Math.max(
      ...calls.map((c) => METHOD_TIMEOUTS[c.method] ?? DEFAULT_TIMEOUT)
    );
    const label = calls.map((c) => c.method).join("+");

    // Track IDs outside lambdas so parseResponse can match by ID, not sort order
    let batchIds: number[] = [];

    return this.withRetry<T>(
      () => {
        batchIds = calls.map(() => ++this.id);
        const batch = calls.map((c, i) => ({
          jsonrpc: "2.0" as const,
          method: c.method,
          params: c.params,
          id: batchIds[i]!,
        }));
        return fetch(this.url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(batch),
          signal: AbortSignal.timeout(timeout),
        });
      },
      async (res) => {
        const raw = (await res.json()) as JsonRpcResponse<unknown>[];

        // Match responses by ID map — sort-by-id is fragile if providers normalize IDs
        const byId = new Map<number, JsonRpcResponse<unknown>>();
        for (const r of raw) {
          if (r != null && typeof r.id === "number") byId.set(r.id, r);
        }
        if (byId.size !== calls.length) {
          throw new Error(
            `RPC batch: expected ${calls.length} responses, got ${byId.size} valid entries`
          );
        }

        const results: unknown[] = [];
        for (const id of batchIds) {
          const resp = byId.get(id);
          if (!resp) throw new Error(`RPC batch: missing response for id ${id}`);
          if (resp.error) {
            throw new JsonRpcError(resp.error.code, resp.error.message);
          }
          results.push(resp.result);
        }
        return results as T;
      },
      label
    );
  }

  async getBlockNumber(): Promise<number> {
    const hex = await this.call<string>("eth_blockNumber", []);
    return parseInt(hex, 16);
  }

  async getFinalizedBlockNumber(): Promise<number> {
    const block = await this.call<{ number: string } | null>(
      "eth_getBlockByNumber",
      ["finalized", false]
    );
    if (!block) throw new Error("Finalized block not available (null from RPC)");
    return parseInt(block.number, 16);
  }

  async getBlockWithReceipts(
    blockNumber: number
  ): Promise<{ block: BlockData; receipts: ReceiptData[] }> {
    const hex = "0x" + blockNumber.toString(16);

    if (this.supportsBlockReceipts) {
      try {
        const [block, receipts] = await this.batchCall<[BlockData, ReceiptData[]]>([
          { method: "eth_getBlockByNumber", params: [hex, true] },
          { method: "eth_getBlockReceipts", params: [hex] },
        ]);
        if (!block) throw new Error(`Block ${blockNumber} not found (null from RPC)`);
        return { block, receipts: receipts ?? [] };
      } catch (err) {
        if (!isMethodNotSupported(err)) throw err;
        this.supportsBlockReceipts = false;
        log("warn", "eth_getBlockReceipts not supported (-32601), falling back to per-tx receipts");
      }
    }

    // Fallback: fetch block then individual receipts per transaction
    const block = await this.call<BlockData>(
      "eth_getBlockByNumber",
      [hex, true]
    );
    if (!block) throw new Error(`Block ${blockNumber} not found (null from RPC)`);

    const receipts: ReceiptData[] = [];
    if (block.transactions.length > 0) {
      // Chunk per-tx receipt calls to stay within provider batch limits (L2 blocks can have 1000+ txs)
      const RECEIPT_BATCH_SIZE = 100;
      for (let i = 0; i < block.transactions.length; i += RECEIPT_BATCH_SIZE) {
        const chunk = block.transactions.slice(i, i + RECEIPT_BATCH_SIZE);
        const receiptCalls = chunk.map((tx) => ({
          method: "eth_getTransactionReceipt",
          params: [tx.hash],
        }));
        const receiptResults = await this.batchCall<ReceiptData[]>(receiptCalls);
        for (const receipt of receiptResults) {
          if (receipt) receipts.push(receipt);
        }
      }
    }

    return { block, receipts };
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
