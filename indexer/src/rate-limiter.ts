// Distributed rate limiter — Redis sliding window with adaptive backpressure.
// On 429: cut rate 25%. After 10 healthy acquires: recover 10%, capped at ceiling.

import type Redis from "ioredis";
import { log } from "./logger.js";

const WINDOW_MS = 1000;

const RATE_LIMIT_SCRIPT = `
  local key = KEYS[1]
  local rateKey = KEYS[2]
  local seqKey = KEYS[3]
  local streakKey = KEYS[4]
  local now = tonumber(ARGV[1])
  local window = tonumber(ARGV[2])
  local defaultLimit = tonumber(ARGV[3])
  local streakThreshold = tonumber(ARGV[4])
  local increase = tonumber(ARGV[5])
  local maxRate = tonumber(ARGV[6])

  local limit = tonumber(redis.call('GET', rateKey)) or defaultLimit

  redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
  local count = redis.call('ZCARD', key)

  if count < limit then
    local seq = redis.call('INCR', seqKey)
    redis.call('ZADD', key, now, now .. '-' .. seq)
    redis.call('PEXPIRE', key, window * 2)

    local streak = redis.call('INCR', streakKey)
    redis.call('PEXPIRE', streakKey, 60000)

    local recoveredRate = limit
    if streak >= streakThreshold then
      local current = tonumber(redis.call('GET', rateKey)) or defaultLimit
      local next = math.min(maxRate, math.max(current + 1, math.ceil(current * increase)))
      if next > current then
        redis.call('SET', rateKey, next)
        redis.call('PEXPIRE', rateKey, 60000)
        recoveredRate = next
      end
      redis.call('SET', streakKey, 0)
    end

    return {1, recoveredRate}
  else
    redis.call('SET', streakKey, 0)
    return {0, limit}
  end
`;

export interface RateLimiter {
  acquire(): Promise<void>;
  recordThrottle?(): void;
  readonly effectiveRate?: number;
}

const THROTTLE_REDUCTION = 0.75;
const RECOVERY_INCREASE = 1.10;
const HEALTHY_STREAK_THRESHOLD = 10;

// Lua script for atomic throttle: reduce effective rate by 25%, floor at 1.
// Also resets the shared healthy streak counter.
const THROTTLE_SCRIPT = `
  local rateKey = KEYS[1]
  local streakKey = KEYS[2]
  local reduction = tonumber(ARGV[1])
  local floor = tonumber(ARGV[2])
  local current = tonumber(redis.call('GET', rateKey) or ARGV[3])
  local next = math.max(floor, math.floor(current * reduction))
  redis.call('SET', rateKey, next)
  redis.call('PEXPIRE', rateKey, 60000)
  redis.call('SET', streakKey, 0)
  return next
`;

export class DistributedRateLimiter implements RateLimiter {
  private readonly key: string;
  private readonly effectiveRateKey: string;
  private readonly seqKey: string;
  private readonly streakKey: string;
  private cachedEffectiveRate: number;

  constructor(
    private readonly redis: Redis,
    private readonly maxPerSecond: number,
    chainId: number = 1
  ) {
    this.cachedEffectiveRate = maxPerSecond;
    // Hash tags ensure all keys map to the same Redis Cluster slot
    this.key = `indexer:{${chainId}}:rate_limit`;
    this.effectiveRateKey = `indexer:{${chainId}}:effective_rate`;
    this.seqKey = `indexer:{${chainId}}:seq`;
    this.streakKey = `indexer:{${chainId}}:healthy_streak`;
  }

  async acquire(): Promise<void> {
    while (true) {
      const now = Date.now();

      // Single Lua script handles: sliding window check, INCR-based unique member,
      // shared healthy streak tracking, and inline recovery when threshold is reached.
      const result = await this.redis.eval(
        RATE_LIMIT_SCRIPT,
        4,
        this.key,
        this.effectiveRateKey,
        this.seqKey,
        this.streakKey,
        now.toString(),
        WINDOW_MS.toString(),
        this.maxPerSecond.toString(),
        HEALTHY_STREAK_THRESHOLD.toString(),
        RECOVERY_INCREASE.toString(),
        this.maxPerSecond.toString()
      ) as [number, number];

      const [allowed, currentRate] = result;
      this.cachedEffectiveRate = currentRate;

      if (allowed === 1) {
        return;
      }

      const waitMs = 20 + Math.random() * 30;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }

  get effectiveRate(): number {
    return this.cachedEffectiveRate;
  }

  recordThrottle(): void {
    // Fire-and-forget atomic throttle in Redis — all workers reduce together.
    // Also resets the shared healthy streak counter.
    this.redis.eval(
      THROTTLE_SCRIPT,
      2,
      this.effectiveRateKey,
      this.streakKey,
      THROTTLE_REDUCTION.toString(),
      "1",
      this.maxPerSecond.toString()
    ).then((newRate) => {
      const prev = this.cachedEffectiveRate;
      this.cachedEffectiveRate = newRate as number;

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
