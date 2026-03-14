// Distributed rate limiter — Redis sliding window with adaptive backpressure.
// On 429: cut rate 25%. After 10 healthy acquires: recover 10%, capped at ceiling.

import type Redis from "ioredis";
import { log } from "./logger.js";

const WINDOW_MS = 1000;

const RATE_LIMIT_SCRIPT = `
  local key = KEYS[1]
  local now = tonumber(ARGV[1])
  local window = tonumber(ARGV[2])
  local limit = tonumber(ARGV[3])

  redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
  local count = redis.call('ZCARD', key)

  if count < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
    redis.call('PEXPIRE', key, window * 2)
    return 1
  else
    return 0
  end
`;

export interface RateLimiter {
  acquire(): Promise<void>;
  recordThrottle?(): void;
}

const THROTTLE_REDUCTION = 0.75;
const RECOVERY_INCREASE = 1.10;
const HEALTHY_STREAK_THRESHOLD = 10;

// Lua script for atomic throttle: reduce effective rate by 25%, floor at 1
const THROTTLE_SCRIPT = `
  local key = KEYS[1]
  local reduction = tonumber(ARGV[1])
  local floor = tonumber(ARGV[2])
  local current = tonumber(redis.call('GET', key) or ARGV[3])
  local next = math.max(floor, math.floor(current * reduction))
  redis.call('SET', key, next)
  redis.call('PEXPIRE', key, 60000)
  return next
`;

// Lua script for atomic recovery: increase effective rate by 10%, cap at max
const RECOVERY_SCRIPT = `
  local key = KEYS[1]
  local increase = tonumber(ARGV[1])
  local max = tonumber(ARGV[2])
  local current = tonumber(redis.call('GET', key) or ARGV[3])
  local next = math.min(max, math.floor(current * increase))
  if next > current then
    redis.call('SET', key, next)
    redis.call('PEXPIRE', key, 60000)
    return next
  end
  return current
`;

export class DistributedRateLimiter implements RateLimiter {
  private healthyStreak = 0;
  private readonly key: string;
  private readonly effectiveRateKey: string;
  private cachedEffectiveRate: number;

  constructor(
    private readonly redis: Redis,
    private readonly maxPerSecond: number,
    chainId: number = 1
  ) {
    this.cachedEffectiveRate = maxPerSecond;
    this.key = `indexer:${chainId}:rate_limit`;
    this.effectiveRateKey = `indexer:${chainId}:effective_rate`;
  }

  async acquire(): Promise<void> {
    while (true) {
      const now = Date.now();

      const allowed = await this.redis.eval(
        RATE_LIMIT_SCRIPT,
        1,
        this.key,
        now.toString(),
        WINDOW_MS.toString(),
        Math.floor(this.cachedEffectiveRate).toString()
      );

      if (allowed === 1) {
        this.healthyStreak++;

        if (this.healthyStreak >= HEALTHY_STREAK_THRESHOLD) {
          // Atomic recovery in Redis — all workers see the same effective rate
          const newRate = await this.redis.eval(
            RECOVERY_SCRIPT,
            1,
            this.effectiveRateKey,
            RECOVERY_INCREASE.toString(),
            this.maxPerSecond.toString(),
            this.cachedEffectiveRate.toString()
          ) as number;

          if (newRate > this.cachedEffectiveRate) {
            log("debug", "Rate limiter recovered", {
              effectiveRate: newRate,
              maxPerSecond: this.maxPerSecond,
            });
          }
          this.cachedEffectiveRate = newRate;
          this.healthyStreak = 0;
        }

        return;
      }

      // Refresh local cache from shared state on wait
      const shared = await this.redis.get(this.effectiveRateKey);
      if (shared) this.cachedEffectiveRate = parseInt(shared, 10);

      const waitMs = 20 + Math.random() * 30;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }

  recordThrottle(): void {
    // Fire-and-forget atomic throttle in Redis — all workers reduce together
    this.redis.eval(
      THROTTLE_SCRIPT,
      1,
      this.effectiveRateKey,
      THROTTLE_REDUCTION.toString(),
      "1",
      this.cachedEffectiveRate.toString()
    ).then((newRate) => {
      const prev = this.cachedEffectiveRate;
      this.cachedEffectiveRate = newRate as number;
      this.healthyStreak = 0;

      log("warn", "Rate limiter throttled", {
        previousRate: prev,
        effectiveRate: this.cachedEffectiveRate,
        maxPerSecond: this.maxPerSecond,
      });
    }).catch((err) => {
      log("error", "Failed to update shared throttle state", {
        error: err instanceof Error ? err.message : String(err),
      });
    });
  }
}
