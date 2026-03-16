// Entry point — wires components and starts the worker loop.
//
// Usage:
//   npm start                          # Worker (chain 1)
//   CHAIN_ID=137 npm start             # Polygon
//   SEED_ONLY=true npm start           # Seed queue, then exit
//   WORKER_ID=w2 npm start             # Second worker
//   LOG_LEVEL=debug npm start          # Verbose

import Redis from "ioredis";
import { loadConfig } from "./config.js";
import { Coordinator } from "./coordinator.js";
import { DistributedRateLimiter } from "./rate-limiter.js";
import { RpcClient, isMethodNotSupported } from "./rpc.js";
import { RpcPool } from "./rpc-pool.js";
import { Storage } from "./storage.js";
import { WriteBuffer } from "./write-buffer.js";
import { Worker } from "./worker.js";
import { Metrics } from "./metrics.js";
import { startMetricsServer } from "./metrics-server.js";
import { log, setLogLevel } from "./logger.js";

let storage: Storage | undefined;
let redis: Redis | undefined;
let metricsServer: ReturnType<typeof startMetricsServer> | undefined;

async function main() {
  const config = loadConfig();
  setLogLevel(config.logLevel);

  log("info", "Starting chain-ingest", {
    chainId: config.chainId,
    workerId: config.workerId,
    rpcUrl: (() => { try { return new URL(config.rpcUrls[0]!).host; } catch { return config.rpcUrls[0]; } })(),
    rpcEndpoints: config.rpcUrls.length,
    batchSize: config.batchSize,
    rateLimit: config.rateLimit,
    seedOnly: config.seedOnly,
  });

  redis = new Redis(config.redisUrl);
  redis.on("error", (err) => log("error", "Redis error", { error: err.message }));

  storage = new Storage(config.postgresUrl, config.pgPoolMax);

  const coordinator = new Coordinator(redis, config.chainId);
  const rateLimiter = new DistributedRateLimiter(redis, config.rateLimit, config.chainId);
  const rpc = config.rpcUrls.length > 1
    ? new RpcPool(config.rpcUrls, rateLimiter, config.maxRetries)
    : new RpcClient(config.rpcUrls[0]!, rateLimiter, config.maxRetries);

  let endBlock: number;
  let finalityMode: "finalized" | "latest";
  if (config.endBlock === "finalized") {
    try {
      endBlock = await rpc.getFinalizedBlockNumber();
      finalityMode = "finalized";
    } catch (err) {
      if (!isMethodNotSupported(err)) {
        // Network timeout, 500, circuit breaker, etc. — don't silently degrade.
        // The RPC client already retried with backoff; if we're here, it's persistent.
        throw err;
      }
      // JSON-RPC -32601: node doesn't support "finalized" tag — fall back to latest.
      log("warn", "RPC does not support 'finalized' block tag (-32601), falling back to 'latest'");
      endBlock = await rpc.getBlockNumber();
      finalityMode = "latest";
    }
  } else if (config.endBlock === "latest") {
    endBlock = await rpc.getBlockNumber();
    finalityMode = "latest";
  } else {
    endBlock = config.endBlock;
    finalityMode = "finalized"; // explicit block number — finality is irrelevant
  }

  log("info", "Finality mode resolved", { finalityMode, endBlock });

  await storage.migrate(endBlock);

  const startBlock = config.startBlock;

  if (startBlock > endBlock) {
    throw new Error(
      `START_BLOCK (${startBlock}) exceeds END_BLOCK (${endBlock}) — nothing to index`
    );
  }

  log("info", "Block range", {
    chainId: config.chainId,
    startBlock,
    endBlock,
    totalBlocks: endBlock - startBlock + 1,
  });

  await coordinator.initQueue(startBlock, endBlock, config.batchSize);

  if (config.seedOnly) {
    log("info", "Seed complete — exiting (SEED_ONLY=true)");
    await storage.close();
    await redis.quit();
    return;
  }

  let metrics: Metrics | undefined;
  if (config.metricsPort) {
    metrics = new Metrics(config.chainId, config.workerId);
    metricsServer = startMetricsServer(config.metricsPort, metrics.registry);
  }

  const writeBuffer = config.flushSize > 1
    ? new WriteBuffer(storage, config.flushSize, config.flushIntervalMs, metrics)
    : undefined;

  const worker = new Worker(config.workerId, coordinator, rpc, storage, config.chainId, writeBuffer, metrics, rateLimiter);

  const shutdown = () => {
    log("info", "Shutting down — draining current block...");
    worker.stop();
    setTimeout(() => {
      log("error", "Shutdown deadline exceeded — forcing exit");
      process.exit(1);
    }, 25_000).unref();
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  await worker.start();

  await writeBuffer?.flush();
  writeBuffer?.dispose();
  await new Promise<void>((resolve) => {
    if (!metricsServer) return resolve();
    metricsServer.closeAllConnections?.();
    metricsServer.close(() => resolve());
  });
  await storage.close();
  await redis.quit();
}

main().catch(async (err) => {
  log("error", "Fatal error", { error: err.message, stack: err.stack });
  try { await storage?.close(); } catch {}
  try { await redis?.quit(); } catch {}
  process.exit(1);
});
