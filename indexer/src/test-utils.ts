// Shared test utilities — mock Redis implementation used across test files.

import { vi } from "vitest";

export function createMockRedis() {
  const store = {
    lists: new Map<string, string[]>(),
    hashes: new Map<string, Map<string, string>>(),
    sets: new Map<string, Map<string, number>>(),
    keys: new Map<string, string>(),
  };

  const pipeline = () => {
    const ops: Array<() => unknown> = [];
    return {
      lpush: (key: string, val: string) => {
        ops.push(() => {
          const list = store.lists.get(key) ?? [];
          list.unshift(val);
          store.lists.set(key, list);
          return list.length;
        });
      },
      zadd: (key: string, score: number, member: string) => {
        ops.push(() => {
          const set = store.sets.get(key) ?? new Map();
          set.set(member, score);
          store.sets.set(key, set);
          return 1;
        });
      },
      zcount: (key: string, min: number, max: number) => {
        ops.push(() => {
          const set = store.sets.get(key);
          if (!set) return 0;
          let count = 0;
          for (const [, score] of set) {
            if (score >= min && score <= max) count++;
          }
          return count;
        });
      },
      lrem: (key: string, _count: number, val: string) => {
        ops.push(() => {
          const list = store.lists.get(key) ?? [];
          const idx = list.indexOf(val);
          if (idx !== -1) list.splice(idx, 1);
          store.lists.set(key, list);
          return idx !== -1 ? 1 : 0;
        });
      },
      hdel: (key: string, field: string) => {
        ops.push(() => {
          const hash = store.hashes.get(key);
          if (!hash) return 0;
          return hash.delete(field) ? 1 : 0;
        });
      },
      hset: (key: string, field: string, val: string) => {
        ops.push(() => {
          const hash = store.hashes.get(key) ?? new Map();
          hash.set(field, val);
          store.hashes.set(key, hash);
          return 1;
        });
      },
      set: (key: string, val: string, ..._args: unknown[]) => {
        ops.push(() => {
          store.keys.set(key, val);
          return "OK";
        });
      },
      hget: (key: string, field: string) => {
        ops.push(() => store.hashes.get(key)?.get(field) ?? null);
      },
      get: (key: string) => {
        ops.push(() => store.keys.get(key) ?? null);
      },
      exec: async () => ops.map((op) => [null, op()]),
    };
  };

  return {
    _store: store,
    set: vi.fn(async (key: string, val: string, ...args: unknown[]) => {
      const hasNX = args.includes("NX");
      if (hasNX && store.keys.has(key)) return null;
      store.keys.set(key, val);
      return "OK";
    }),
    get: vi.fn(async (key: string) => store.keys.get(key) ?? null),
    llen: vi.fn(async (key: string) => (store.lists.get(key) ?? []).length),
    lrange: vi.fn(async (key: string, start: number, stop: number) => {
      const list = store.lists.get(key) ?? [];
      if (stop === -1) stop = list.length - 1;
      return list.slice(start, stop + 1);
    }),
    lrem: vi.fn(async (key: string, _count: number, val: string) => {
      const list = store.lists.get(key) ?? [];
      const idx = list.indexOf(val);
      if (idx !== -1) list.splice(idx, 1);
      store.lists.set(key, list);
      return idx !== -1 ? 1 : 0;
    }),
    lpush: vi.fn(async (key: string, val: string) => {
      const list = store.lists.get(key) ?? [];
      list.unshift(val);
      store.lists.set(key, list);
      return list.length;
    }),
    rpush: vi.fn(async (key: string, val: string) => {
      const list = store.lists.get(key) ?? [];
      list.push(val);
      store.lists.set(key, list);
      return list.length;
    }),
    brpoplpush: vi.fn(async (src: string, dst: string, _timeout?: number) => {
      const srcList = store.lists.get(src) ?? [];
      if (srcList.length === 0) return null;
      const val = srcList.pop()!;
      store.lists.set(src, srcList);
      const dstList = store.lists.get(dst) ?? [];
      dstList.unshift(val);
      store.lists.set(dst, dstList);
      return val;
    }),
    hset: vi.fn(async (key: string, field: string, val: string) => {
      const hash = store.hashes.get(key) ?? new Map();
      hash.set(field, val);
      store.hashes.set(key, hash);
      return 1;
    }),
    hget: vi.fn(async (key: string, field: string) =>
      store.hashes.get(key)?.get(field) ?? null
    ),
    hdel: vi.fn(async (key: string, field: string) => {
      const hash = store.hashes.get(key);
      if (!hash) return 0;
      return hash.delete(field) ? 1 : 0;
    }),
    zcard: vi.fn(async (key: string) => store.sets.get(key)?.size ?? 0),
    zcount: vi.fn(async (key: string, min: number, max: number) => {
      const set = store.sets.get(key);
      if (!set) return 0;
      let count = 0;
      for (const [, score] of set) {
        if (score >= min && score <= max) count++;
      }
      return count;
    }),
    zscore: vi.fn(async (key: string, member: string) => {
      const set = store.sets.get(key);
      if (!set) return null;
      const score = set.get(member);
      return score !== undefined ? String(score) : null;
    }),
    zadd: vi.fn(async (key: string, score: number, member: string) => {
      const set = store.sets.get(key) ?? new Map();
      set.set(member, score);
      store.sets.set(key, set);
      return 1;
    }),
    zrangebyscore: vi.fn(async (key: string, min: number, max: number) => {
      const set = store.sets.get(key);
      if (!set) return [];
      const results: string[] = [];
      for (const [member, score] of set) {
        if (score >= min && score <= max) results.push(member);
      }
      return results;
    }),
    zremrangebyscore: vi.fn(async (key: string, min: number, max: number) => {
      const set = store.sets.get(key);
      if (!set) return 0;
      let removed = 0;
      for (const [member, score] of set) {
        if (score >= min && score <= max) { set.delete(member); removed++; }
      }
      return removed;
    }),
    pipeline,
  } as any;
}
