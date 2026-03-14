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

export class DistributedRateLimiter implements RateLimiter {
  private effectiveRate: number;
  private healthyStreak = 0;
  private readonly key: string;

  constructor(
    private readonly redis: Redis,
    private readonly maxPerSecond: number,
    chainId: number = 1
  ) {
    this.effectiveRate = maxPerSecond;
    this.key = `indexer:${chainId}:rate_limit`;
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
        Math.floor(this.effectiveRate).toString()
      );

      if (allowed === 1) {
        this.healthyStreak++;

        if (this.healthyStreak >= HEALTHY_STREAK_THRESHOLD) {
          const prev = this.effectiveRate;
          this.effectiveRate = Math.min(
            this.effectiveRate * RECOVERY_INCREASE,
            this.maxPerSecond
          );
          if (this.effectiveRate > prev) {
            log("debug", "Rate limiter recovered", {
              effectiveRate: Math.floor(this.effectiveRate),
              maxPerSecond: this.maxPerSecond,
            });
          }
          this.healthyStreak = 0;
        }

        return;
      }

      const waitMs = 20 + Math.random() * 30;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }

  recordThrottle(): void {
    const prev = this.effectiveRate;
    this.effectiveRate = Math.max(1, this.effectiveRate * THROTTLE_REDUCTION);
    this.healthyStreak = 0;

    log("warn", "Rate limiter throttled", {
      previousRate: Math.floor(prev),
      effectiveRate: Math.floor(this.effectiveRate),
      maxPerSecond: this.maxPerSecond,
    });
  }
}
