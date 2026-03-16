export interface Config {
  chainId: number;
  redisUrl: string;
  rpcUrls: string[];
  postgresUrl: string;
  batchSize: number;
  flushSize: number;
  flushIntervalMs: number;
  rateLimit: number;
  workerId: string;
  maxRetries: number;
  startBlock: number;
  endBlock: number | "latest" | "finalized";
  logLevel: "debug" | "info" | "warn" | "error";
  seedOnly: boolean;
  pgPoolMax: number;
  metricsPort: number | null;
}

function requirePositiveInt(
  val: string | undefined,
  name: string,
  defaultVal: number
): number {
  const raw = val ?? String(defaultVal);
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid config: ${name}="${val}" must be a positive integer (> 0)`);
  }
  return parsed;
}

function requireNonNegativeInt(
  val: string | undefined,
  name: string,
  defaultVal: number
): number {
  const raw = val ?? String(defaultVal);
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid config: ${name}="${val}" must be a non-negative integer`);
  }
  return parsed;
}

function parseEndBlock(
  val: string | undefined
): number | "latest" | "finalized" {
  if (!val || val === "finalized") return "finalized";
  if (val === "latest") return "latest";
  const n = Number(val);
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(`Invalid END_BLOCK: "${val}" — must be a non-negative integer, "latest", or "finalized"`);
  }
  return n;
}

function parseLogLevel(
  val: string | undefined
): "debug" | "info" | "warn" | "error" {
  const valid = ["debug", "info", "warn", "error"] as const;
  const level = (val ?? "info").toLowerCase();
  if (!valid.includes(level as any)) {
    throw new Error(`Invalid LOG_LEVEL: "${val}" — must be debug|info|warn|error`);
  }
  return level as (typeof valid)[number];
}

function parseOptionalPort(val: string | undefined): number | null {
  if (!val) return null;
  const parsed = Number(val);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65535) {
    throw new Error(`Invalid config: METRICS_PORT="${val}" must be a valid port (1-65535)`);
  }
  return parsed;
}

function parseRpcUrls(rpcUrlsRaw: string | undefined, rpcUrl: string): string[] {
  if (!rpcUrlsRaw) return [rpcUrl];
  const urls = rpcUrlsRaw.split(",").map((u) => u.trim()).filter(Boolean);
  if (urls.length === 0) return [rpcUrl];
  return urls;
}

export function loadConfig(): Config {
  const rpcUrl = process.env["RPC_URL"] ?? "http://localhost:8545";

  return {
    chainId: requirePositiveInt(process.env["CHAIN_ID"], "CHAIN_ID", 1),
    redisUrl: process.env["REDIS_URL"] ?? "redis://localhost:6379",
    rpcUrls: parseRpcUrls(process.env["RPC_URLS"], rpcUrl),
    postgresUrl:
      process.env["POSTGRES_URL"] ??
      "postgresql://indexer:indexer_password@localhost:5432/indexer",
    batchSize: requirePositiveInt(process.env["BATCH_SIZE"], "BATCH_SIZE", 10),
    flushSize: requirePositiveInt(process.env["FLUSH_SIZE"], "FLUSH_SIZE", 1),
    flushIntervalMs: requireNonNegativeInt(process.env["FLUSH_INTERVAL_MS"], "FLUSH_INTERVAL_MS", 0),
    rateLimit: requirePositiveInt(process.env["RATE_LIMIT"], "RATE_LIMIT", 50),
    workerId:
      process.env["WORKER_ID"] ?? `worker-${process.pid}-${Date.now()}`,
    maxRetries: requirePositiveInt(process.env["MAX_RETRIES"], "MAX_RETRIES", 5),
    startBlock: requireNonNegativeInt(process.env["START_BLOCK"], "START_BLOCK", 0),
    endBlock: parseEndBlock(process.env["END_BLOCK"]),
    logLevel: parseLogLevel(process.env["LOG_LEVEL"]),
    seedOnly: process.env["SEED_ONLY"] === "true",
    pgPoolMax: requirePositiveInt(process.env["PG_POOL_MAX"], "PG_POOL_MAX", 20),
    metricsPort: parseOptionalPort(process.env["METRICS_PORT"]),
  };
}
