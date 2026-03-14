import { describe, it, expect, vi } from "vitest";
import { DistributedRateLimiter } from "./rate-limiter.js";

// Mock Redis that always allows (or blocks N times then allows)
function createMockRedis(blockCount = 0) {
  let calls = 0;
  return {
    eval: vi.fn(async () => {
      calls++;
      return calls > blockCount ? 1 : 0;
    }),
  } as any;
}

describe("DistributedRateLimiter", () => {
  it("acquires immediately when under limit", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 50);

    await limiter.acquire();
    expect(redis.eval).toHaveBeenCalledOnce();
  });

  it("retries when rate limit is full", async () => {
    // Block first 2 calls, allow on 3rd
    const redis = createMockRedis(2);
    const limiter = new DistributedRateLimiter(redis, 50);

    await limiter.acquire();
    expect(redis.eval).toHaveBeenCalledTimes(3);
  });

  it("reduces effective rate on throttle", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 100);

    limiter.recordThrottle();

    // After throttle, Lua script should be called with reduced limit
    // The effective rate should be 75 (100 * 0.75)
    await limiter.acquire();
    const lastCall = redis.eval.mock.lastCall;
    // Args: script, numkeys, key, now, window, limit
    expect(Number(lastCall![5])).toBe(75);
  });

  it("floors effective rate at 1 req/s", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 2);

    // Throttle multiple times
    limiter.recordThrottle(); // 2 * 0.75 = 1.5
    limiter.recordThrottle(); // 1.5 * 0.75 = 1.125
    limiter.recordThrottle(); // 1.125 * 0.75 = 0.84 → clamped to 1

    await limiter.acquire();
    const lastCall = redis.eval.mock.lastCall;
    expect(Number(lastCall![5])).toBeGreaterThanOrEqual(1);
  });

  it("recovers rate after healthy streak", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 100);

    // Throttle first
    limiter.recordThrottle(); // → 75

    // Do 10 healthy acquires (streak threshold)
    for (let i = 0; i < 10; i++) {
      await limiter.acquire();
    }

    // After recovery: 75 * 1.10 = 82.5 → 82
    await limiter.acquire();
    const lastCall = redis.eval.mock.lastCall;
    expect(Number(lastCall![5])).toBe(82); // floor(82.5)
  });
});
