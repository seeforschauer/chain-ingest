// Configuration loaded from environment variables

export interface Config {
  chainId: number;
  redisUrl: string;
  rpcUrl: string;
  postgresUrl: string;
  batchSize: number;
  rateLimit: number;
  workerId: string;
  maxRetries: number;
  startBlock: number;
  endBlock: number | "latest" | "finalized";
  logLevel: "debug" | "info" | "warn" | "error";
  seedOnly: boolean;
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

export function loadConfig(): Config {
  return {
    chainId: requirePositiveInt(process.env["CHAIN_ID"], "CHAIN_ID", 1),
    redisUrl: process.env["REDIS_URL"] ?? "redis://localhost:6379",
    rpcUrl: process.env["RPC_URL"] ?? "http://localhost:8545",
    postgresUrl:
      process.env["POSTGRES_URL"] ??
      "postgresql://indexer:indexer_password@localhost:5432/indexer",
    batchSize: requirePositiveInt(process.env["BATCH_SIZE"], "BATCH_SIZE", 10),
    rateLimit: requirePositiveInt(process.env["RATE_LIMIT"], "RATE_LIMIT", 50),
    workerId:
      process.env["WORKER_ID"] ?? `worker-${process.pid}-${Date.now()}`,
    maxRetries: requirePositiveInt(process.env["MAX_RETRIES"], "MAX_RETRIES", 5),
    startBlock: requireNonNegativeInt(process.env["START_BLOCK"], "START_BLOCK", 0),
    endBlock: parseEndBlock(process.env["END_BLOCK"]),
    logLevel: parseLogLevel(process.env["LOG_LEVEL"]),
    seedOnly: process.env["SEED_ONLY"] === "true",
  };
}
