// Write buffer — accumulates blocks and flushes to storage in batches.

import type { Storage } from "./storage.js";
import type { BlockData, ReceiptData } from "./rpc.js";
import { log } from "./logger.js";

interface BufferedBlock {
  block: BlockData;
  receipts: ReceiptData[];
  workerId: string;
}

export class WriteBuffer {
  private buffer: BufferedBlock[] = [];
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private flushQueue: Promise<void> = Promise.resolve();

  constructor(
    private readonly storage: Storage,
    private readonly flushSize: number,
    private readonly flushIntervalMs: number
  ) {
    if (flushIntervalMs > 0) {
      this.flushTimer = setInterval(() => {
        this.flush().catch((err) => {
          log("error", "Write buffer periodic flush failed", {
            error: err instanceof Error ? err.message : String(err),
          });
        });
      }, flushIntervalMs);
    }
  }

  async add(block: BlockData, receipts: ReceiptData[], workerId: string): Promise<void> {
    this.buffer.push({ block, receipts, workerId });

    if (this.buffer.length >= this.flushSize) {
      await this.flush();
    }
  }

  flush(): Promise<void> {
    this.flushQueue = this.flushQueue.then(() => this.drainBuffer());
    return this.flushQueue;
  }

  private async drainBuffer(): Promise<void> {
    if (this.buffer.length === 0) return;
    const batch = this.buffer.splice(0);
    await this.doFlush(batch);
  }

  private async doFlush(batch: BufferedBlock[]): Promise<void> {
    log("debug", "Write buffer flushing", { blocks: batch.length });
    await this.storage.insertBlocks(batch);
    log("debug", "Write buffer flushed", { blocks: batch.length });
  }

  dispose(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  get pending(): number {
    return this.buffer.length;
  }
}
