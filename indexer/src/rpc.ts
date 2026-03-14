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
};
const DEFAULT_TIMEOUT = 15_000;

enum CircuitState {
  CLOSED,
  OPEN,
  HALF_OPEN,
}

export class RpcClient {
  private id = 0;
  private circuitState = CircuitState.CLOSED;
  private consecutiveFailures = 0;
  private circuitOpenedAt = 0;
  private readonly failureThreshold: number;
  private readonly cooldownMs: number;

  constructor(
    private readonly url: string,
    private readonly rateLimiter: RateLimiter,
    private readonly maxRetries: number,
    failureThreshold = 5,
    cooldownMs = 30_000
  ) {
    this.failureThreshold = failureThreshold;
    this.cooldownMs = cooldownMs;
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

  private onSuccess(): void {
    if (this.circuitState === CircuitState.HALF_OPEN) {
      log("info", "Circuit breaker closed — probe succeeded", {
        url: this.url,
      });
    }
    this.consecutiveFailures = 0;
    this.circuitState = CircuitState.CLOSED;
  }

  private onFailure(): void {
    this.consecutiveFailures++;

    if (
      this.circuitState === CircuitState.HALF_OPEN ||
      this.consecutiveFailures >= this.failureThreshold
    ) {
      this.circuitState = CircuitState.OPEN;
      this.circuitOpenedAt = Date.now();
      log("warn", "Circuit breaker OPEN", {
        url: this.url,
        consecutiveFailures: this.consecutiveFailures,
        cooldownMs: this.cooldownMs,
      });
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
          throw new Error(`RPC error ${json.error.code}: ${json.error.message}`);
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

    return this.withRetry<T>(
      () => {
        const batch = calls.map((c) => ({
          jsonrpc: "2.0" as const,
          method: c.method,
          params: c.params,
          id: ++this.id,
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

        // Filter nulls — some providers return malformed batch responses
        const responses = raw.filter(
          (r): r is JsonRpcResponse<unknown> => r != null && typeof r.id === "number"
        );
        if (responses.length !== calls.length) {
          throw new Error(
            `RPC batch: expected ${calls.length} responses, got ${responses.length} valid entries`
          );
        }

        responses.sort((a, b) => a.id - b.id);
        const results: unknown[] = [];
        for (const resp of responses) {
          if (resp.error) {
            throw new Error(`RPC batch error ${resp.error.code}: ${resp.error.message}`);
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
    const [block, receipts] = await this.batchCall<[BlockData, ReceiptData[]]>([
      { method: "eth_getBlockByNumber", params: [hex, true] },
      { method: "eth_getBlockReceipts", params: [hex] },
    ]);
    if (!block) throw new Error(`Block ${blockNumber} not found (null from RPC)`);
    return { block, receipts: receipts ?? [] };
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
