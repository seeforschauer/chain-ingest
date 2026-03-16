import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { loadConfig } from "./config.js";

describe("loadConfig", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it("returns defaults when no env vars set", () => {
    const config = loadConfig();
    expect(config.redisUrl).toBe("redis://localhost:6379");
    expect(config.rpcUrls).toEqual(["http://localhost:8545"]);
    expect(config.batchSize).toBe(10);
    expect(config.flushSize).toBe(1);
    expect(config.flushIntervalMs).toBe(0);
    expect(config.rateLimit).toBe(50);
    expect(config.maxRetries).toBe(5);
    expect(config.startBlock).toBe(0);
    expect(config.endBlock).toBe("finalized");
    expect(config.pgPoolMax).toBe(20);
    expect(config.metricsPort).toBeNull();
  });

  it("reads PG_POOL_MAX env var", () => {
    process.env["PG_POOL_MAX"] = "50";
    const config = loadConfig();
    expect(config.pgPoolMax).toBe(50);
  });

  it("reads env vars when set", () => {
    process.env["BATCH_SIZE"] = "25";
    process.env["RATE_LIMIT"] = "100";
    process.env["START_BLOCK"] = "1000";
    process.env["END_BLOCK"] = "2000";
    process.env["WORKER_ID"] = "test-worker";

    const config = loadConfig();
    expect(config.batchSize).toBe(25);
    expect(config.rateLimit).toBe(100);
    expect(config.startBlock).toBe(1000);
    expect(config.endBlock).toBe(2000);
    expect(config.workerId).toBe("test-worker");
  });

  it("rejects batchSize=0 (would cause infinite loop)", () => {
    process.env["BATCH_SIZE"] = "0";
    expect(() => loadConfig()).toThrow("positive integer");
  });

  it("rejects negative batchSize", () => {
    process.env["BATCH_SIZE"] = "-5";
    expect(() => loadConfig()).toThrow("positive integer");
  });

  it("rejects NaN batchSize", () => {
    process.env["BATCH_SIZE"] = "abc";
    expect(() => loadConfig()).toThrow("positive integer");
  });

  it("rejects float batchSize (no silent truncation)", () => {
    process.env["BATCH_SIZE"] = "10.5";
    expect(() => loadConfig()).toThrow("positive integer");
  });

  it("allows startBlock=0", () => {
    process.env["START_BLOCK"] = "0";
    const config = loadConfig();
    expect(config.startBlock).toBe(0);
  });

  it("rejects negative startBlock", () => {
    process.env["START_BLOCK"] = "-1";
    expect(() => loadConfig()).toThrow("non-negative integer");
  });

  it("parses endBlock='latest'", () => {
    process.env["END_BLOCK"] = "latest";
    const config = loadConfig();
    expect(config.endBlock).toBe("latest");
  });

  it("parses endBlock='finalized'", () => {
    process.env["END_BLOCK"] = "finalized";
    const config = loadConfig();
    expect(config.endBlock).toBe("finalized");
  });

  it("rejects non-numeric endBlock", () => {
    process.env["END_BLOCK"] = "garbage";
    expect(() => loadConfig()).toThrow("Invalid END_BLOCK");
  });

  it("defaults chainId to 1", () => {
    const config = loadConfig();
    expect(config.chainId).toBe(1);
  });

  it("reads CHAIN_ID env var", () => {
    process.env["CHAIN_ID"] = "137";
    const config = loadConfig();
    expect(config.chainId).toBe(137);
  });

  it("defaults logLevel to info", () => {
    const config = loadConfig();
    expect(config.logLevel).toBe("info");
  });

  it("reads LOG_LEVEL env var", () => {
    process.env["LOG_LEVEL"] = "debug";
    const config = loadConfig();
    expect(config.logLevel).toBe("debug");
  });

  it("rejects invalid LOG_LEVEL", () => {
    process.env["LOG_LEVEL"] = "verbose";
    expect(() => loadConfig()).toThrow("Invalid LOG_LEVEL");
  });

  it("parses SEED_ONLY flag", () => {
    process.env["SEED_ONLY"] = "true";
    const config = loadConfig();
    expect(config.seedOnly).toBe(true);
  });

  it("parses RPC_URLS as comma-separated list", () => {
    process.env["RPC_URLS"] = "http://rpc1:8545,http://rpc2:8545,http://rpc3:8545";
    const config = loadConfig();
    expect(config.rpcUrls).toEqual([
      "http://rpc1:8545",
      "http://rpc2:8545",
      "http://rpc3:8545",
    ]);
  });

  it("falls back to [rpcUrl] when RPC_URLS is unset", () => {
    process.env["RPC_URL"] = "http://custom:8545";
    const config = loadConfig();
    expect(config.rpcUrls).toEqual(["http://custom:8545"]);
  });

  it("trims whitespace in RPC_URLS entries", () => {
    process.env["RPC_URLS"] = " http://a:8545 , http://b:8545 ";
    const config = loadConfig();
    expect(config.rpcUrls).toEqual(["http://a:8545", "http://b:8545"]);
  });

  it("falls back to [rpcUrl] when RPC_URLS is empty", () => {
    process.env["RPC_URLS"] = "";
    const config = loadConfig();
    expect(config.rpcUrls).toEqual(["http://localhost:8545"]);
  });

  it("reads FLUSH_SIZE env var", () => {
    process.env["FLUSH_SIZE"] = "100";
    const config = loadConfig();
    expect(config.flushSize).toBe(100);
  });

  it("rejects FLUSH_SIZE=0", () => {
    process.env["FLUSH_SIZE"] = "0";
    expect(() => loadConfig()).toThrow("positive integer");
  });

  it("reads FLUSH_INTERVAL_MS env var", () => {
    process.env["FLUSH_INTERVAL_MS"] = "5000";
    const config = loadConfig();
    expect(config.flushIntervalMs).toBe(5000);
  });

  it("allows FLUSH_INTERVAL_MS=0", () => {
    process.env["FLUSH_INTERVAL_MS"] = "0";
    const config = loadConfig();
    expect(config.flushIntervalMs).toBe(0);
  });

  it("rejects negative FLUSH_INTERVAL_MS", () => {
    process.env["FLUSH_INTERVAL_MS"] = "-1";
    expect(() => loadConfig()).toThrow("non-negative integer");
  });

  it("reads METRICS_PORT env var", () => {
    process.env["METRICS_PORT"] = "9090";
    const config = loadConfig();
    expect(config.metricsPort).toBe(9090);
  });

  it("defaults METRICS_PORT to null", () => {
    const config = loadConfig();
    expect(config.metricsPort).toBeNull();
  });

  it("rejects invalid METRICS_PORT", () => {
    process.env["METRICS_PORT"] = "abc";
    expect(() => loadConfig()).toThrow("valid port");
  });

  it("rejects METRICS_PORT out of range", () => {
    process.env["METRICS_PORT"] = "70000";
    expect(() => loadConfig()).toThrow("valid port");
  });

});
