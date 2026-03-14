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
import { RpcClient } from "./rpc.js";
import { Storage } from "./storage.js";
import { Worker } from "./worker.js";
import { log, setLogLevel } from "./logger.js";

let storage: Storage | undefined;
let redis: Redis | undefined;

async function main() {
  const config = loadConfig();
  setLogLevel(config.logLevel);

  log("info", "Starting chain-ingest", {
    chainId: config.chainId,
    workerId: config.workerId,
    rpcUrl: config.rpcUrl,
    batchSize: config.batchSize,
    rateLimit: config.rateLimit,
    seedOnly: config.seedOnly,
  });

  redis = new Redis(config.redisUrl);
  redis.on("error", (err) => log("error", "Redis error", { error: err.message }));

  storage = new Storage(config.postgresUrl);
  await storage.migrate();

  const coordinator = new Coordinator(redis, config.chainId);
  const rateLimiter = new DistributedRateLimiter(redis, config.rateLimit, config.chainId);
  const rpc = new RpcClient(config.rpcUrl, rateLimiter, config.maxRetries);

  let endBlock: number;
  if (config.endBlock === "finalized") {
    try {
      endBlock = await rpc.getFinalizedBlockNumber();
    } catch {
      endBlock = await rpc.getBlockNumber();
    }
  } else if (config.endBlock === "latest") {
    endBlock = await rpc.getBlockNumber();
  } else {
    endBlock = config.endBlock;
  }

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

  const worker = new Worker(config.workerId, coordinator, rpc, storage, config.chainId);

  const shutdown = () => {
    log("info", "Shutting down — draining current block...");
    worker.stop();
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  await worker.start();

  // quit() sends QUIT for graceful disconnect; disconnect() drops TCP
  await storage.close();
  await redis.quit();
}

main().catch(async (err) => {
  log("error", "Fatal error", { error: err.message, stack: err.stack });
  try { await storage?.close(); } catch {}
  try { await redis?.quit(); } catch {}
  process.exit(1);
});
