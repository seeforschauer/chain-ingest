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
    expect(config.rpcUrl).toBe("http://localhost:8545");
    expect(config.batchSize).toBe(10);
    expect(config.rateLimit).toBe(50);
    expect(config.maxRetries).toBe(5);
    expect(config.startBlock).toBe(0);
    expect(config.endBlock).toBe("finalized");
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
});
