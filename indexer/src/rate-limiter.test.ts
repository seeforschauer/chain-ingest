import { describe, it, expect, vi } from "vitest";
import { DistributedRateLimiter } from "./rate-limiter.js";

// Mock Redis that handles both the sliding-window script and throttle/recovery scripts.
function createMockRedis(blockCount = 0) {
  let slidingWindowCalls = 0;
  let sharedRate: number | null = null;

  return {
    eval: vi.fn(async (...args: unknown[]) => {
      const script = args[0] as string;

      // Sliding window rate limit script (checks ZREMRANGEBYSCORE)
      if (script.includes("ZREMRANGEBYSCORE")) {
        slidingWindowCalls++;
        return slidingWindowCalls > blockCount ? 1 : 0;
      }

      // Throttle script (reduces rate by 25%)
      if (script.includes("math.max")) {
        const reduction = parseFloat(args[3] as string);
        const floor = parseFloat(args[4] as string);
        const current = sharedRate ?? parseFloat(args[5] as string);
        sharedRate = Math.max(floor, Math.floor(current * reduction));
        return sharedRate;
      }

      // Recovery script (increases rate by 10%)
      if (script.includes("math.min")) {
        const increase = parseFloat(args[3] as string);
        const max = parseFloat(args[4] as string);
        const current = sharedRate ?? parseFloat(args[5] as string);
        const next = Math.min(max, Math.floor(current * increase));
        if (next > current) sharedRate = next;
        return sharedRate ?? current;
      }

      return 0;
    }),
    get: vi.fn(async () => sharedRate?.toString() ?? null),
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
    const redis = createMockRedis(2);
    const limiter = new DistributedRateLimiter(redis, 50);

    await limiter.acquire();
    // 2 blocked sliding-window calls + 1 get (refresh cache) + 1 successful = 4 eval calls
    // Plus 2 redis.get calls during waits
    const slidingCalls = redis.eval.mock.calls.filter(
      (c: unknown[]) => (c[0] as string).includes("ZREMRANGEBYSCORE")
    );
    expect(slidingCalls.length).toBe(3);
  });

  it("reduces effective rate on throttle", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 100);

    limiter.recordThrottle();
    // Wait for the fire-and-forget eval to complete
    await new Promise((r) => setTimeout(r, 10));

    await limiter.acquire();
    // The sliding-window call should use the throttled rate (75)
    const lastSlidingCall = redis.eval.mock.calls.filter(
      (c: unknown[]) => (c[0] as string).includes("ZREMRANGEBYSCORE")
    ).pop();
    expect(Number(lastSlidingCall![5])).toBe(75);
  });

  it("floors effective rate at 1 req/s", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 2);

    limiter.recordThrottle();
    limiter.recordThrottle();
    limiter.recordThrottle();
    await new Promise((r) => setTimeout(r, 10));

    await limiter.acquire();
    const lastSlidingCall = redis.eval.mock.calls.filter(
      (c: unknown[]) => (c[0] as string).includes("ZREMRANGEBYSCORE")
    ).pop();
    expect(Number(lastSlidingCall![5])).toBeGreaterThanOrEqual(1);
  });

  it("recovers rate after healthy streak", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 100);

    limiter.recordThrottle(); // → 75 (shared in Redis)
    await new Promise((r) => setTimeout(r, 10));

    // 10 healthy acquires trigger recovery
    for (let i = 0; i < 10; i++) {
      await limiter.acquire();
    }

    // After recovery: 75 * 1.10 = 82.5 → 82
    await limiter.acquire();
    const lastSlidingCall = redis.eval.mock.calls.filter(
      (c: unknown[]) => (c[0] as string).includes("ZREMRANGEBYSCORE")
    ).pop();
    expect(Number(lastSlidingCall![5])).toBe(82);
  });
});
