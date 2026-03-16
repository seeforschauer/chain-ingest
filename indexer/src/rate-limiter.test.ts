import { describe, it, expect, vi } from "vitest";
import { DistributedRateLimiter } from "./rate-limiter.js";

// Mock Redis that handles the unified sliding-window+recovery script and the throttle script.
// Simulates the Lua scripts reading effective rate from Redis.
function createMockRedis(blockCount = 0) {
  let slidingWindowCalls = 0;
  let sharedRate: number | null = null;
  let healthyStreak = 0;

  return {
    eval: vi.fn(async (...args: unknown[]) => {
      const script = args[0] as string;

      // Unified sliding window + recovery script (checks ZREMRANGEBYSCORE)
      // eval(script, 4, key, rateKey, seqKey, streakKey,
      //   ARGV[1]=now, ARGV[2]=window, ARGV[3]=defaultLimit,
      //   ARGV[4]=streakThreshold, ARGV[5]=increase, ARGV[6]=maxRate)
      // args:  [0]     [1] [2]  [3]     [4]      [5]
      //        [6]=now  [7]=window  [8]=defaultLimit
      //        [9]=streakThreshold  [10]=increase  [11]=maxRate
      if (script.includes("ZREMRANGEBYSCORE")) {
        slidingWindowCalls++;
        const defaultLimit = parseFloat(args[8] as string);
        const streakThreshold = parseFloat(args[9] as string);
        const increase = parseFloat(args[10] as string);
        const maxRate = parseFloat(args[11] as string);
        const limit = sharedRate ?? defaultLimit;
        const allowed = slidingWindowCalls > blockCount ? 1 : 0;

        if (allowed) {
          healthyStreak++;
          let recoveredRate = limit;
          if (healthyStreak >= streakThreshold) {
            const current = sharedRate ?? defaultLimit;
            const next = Math.min(maxRate, Math.max(current + 1, Math.ceil(current * increase)));
            if (next > current) {
              sharedRate = next;
              recoveredRate = next;
            }
            healthyStreak = 0;
          }
          return [1, recoveredRate];
        } else {
          healthyStreak = 0;
          return [0, limit];
        }
      }

      // Throttle script (reduces rate by 25%, resets streak)
      // eval(script, 2, effectiveRateKey, streakKey, ARGV[1]=reduction, ARGV[2]=floor, ARGV[3]=maxPerSecond)
      // args:  [0]     [1] [2]             [3]        [4]=reduction  [5]=floor  [6]=fallback
      if (script.includes("math.max")) {
        const reduction = parseFloat(args[4] as string);
        const floor = parseFloat(args[5] as string);
        const fallback = parseFloat(args[6] as string);
        const current = sharedRate ?? fallback;
        sharedRate = Math.max(floor, Math.floor(current * reduction));
        healthyStreak = 0;
        return sharedRate;
      }

      return 0;
    }),
    get: vi.fn(async () => sharedRate?.toString() ?? null),
    // Expose sharedRate for test assertions
    getSharedRate: () => sharedRate,
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
    // 2 blocked sliding-window calls + 1 successful = 3 sliding-window eval calls
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

    // After throttle: sharedRate in Redis should be 75 (100 * 0.75)
    expect(redis.getSharedRate()).toBe(75);

    await limiter.acquire();
    // The Lua script reads rate from Redis (75), not from ARGV
    // effectiveRate getter should reflect the shared state
    expect(limiter.effectiveRate).toBe(75);
  });

  it("floors effective rate at 1 req/s", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 2);

    limiter.recordThrottle();
    limiter.recordThrottle();
    limiter.recordThrottle();
    await new Promise((r) => setTimeout(r, 10));

    // 2 * 0.75 = 1.5 → 1, then 1 * 0.75 = 0.75 → floor at 1, stays 1
    expect(redis.getSharedRate()).toBeGreaterThanOrEqual(1);

    await limiter.acquire();
    expect(limiter.effectiveRate).toBeGreaterThanOrEqual(1);
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

    // After recovery: max(75 + 1, ceil(75 * 1.10)) = max(76, 83) = 83
    expect(redis.getSharedRate()).toBe(83);

    await limiter.acquire();
    expect(limiter.effectiveRate).toBe(83);
  });

  it("all workers see throttled rate from Redis (cache coherence fix)", async () => {
    const redis = createMockRedis(0);
    const workerA = new DistributedRateLimiter(redis, 50);
    const workerB = new DistributedRateLimiter(redis, 50);

    // Worker A records a 429 throttle — rate drops to 37 in Redis
    workerA.recordThrottle();
    await new Promise((r) => setTimeout(r, 10));
    expect(redis.getSharedRate()).toBe(37);

    // Worker B acquires — Lua script reads 37 from Redis, NOT its local cache of 50
    await workerB.acquire();
    expect(workerB.effectiveRate).toBe(37);

    // Worker A also sees the same rate
    await workerA.acquire();
    expect(workerA.effectiveRate).toBe(37);
  });

  it("passes maxPerSecond as default limit to Lua script, not cachedEffectiveRate", async () => {
    const redis = createMockRedis(0);
    const limiter = new DistributedRateLimiter(redis, 50);

    await limiter.acquire();

    // The sliding-window eval call should pass maxPerSecond (50) as ARGV[3] (defaultLimit)
    // Args: script, numKeys(4), KEYS[1..4], ARGV[1]=now, ARGV[2]=window, ARGV[3]=defaultLimit,
    //       ARGV[4]=streakThreshold, ARGV[5]=increase, ARGV[6]=maxRate
    const slidingCall = redis.eval.mock.calls.find(
      (c: unknown[]) => (c[0] as string).includes("ZREMRANGEBYSCORE")
    );
    expect(slidingCall).toBeDefined();
    // numKeys = 4
    expect(slidingCall![1]).toBe(4);
    // ARGV[3] = maxPerSecond as string (index 8 in the args array: script + numKeys + 4 keys + now + window + defaultLimit)
    expect(slidingCall![8]).toBe("50");
  });
});
