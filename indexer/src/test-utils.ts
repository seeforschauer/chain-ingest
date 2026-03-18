// Shared test utilities — mock Redis implementation with Streams + Bitmaps.

import { vi } from "vitest";
import {
  BATCH_SETBIT_SCRIPT,
  GET_COMPLETED_RANGE_SCRIPT,
  CONTIGUOUS_WATERMARK_SCRIPT,
} from "./coordinator.js";

interface StreamEntry {
  id: string;
  fields: string[]; // [field1, value1, field2, value2, ...]
}

interface PendingEntry {
  deliveredAt: number;
  deliveryCount: number;
  consumer: string;
}

interface ConsumerGroup {
  lastDeliveredId: string; // last delivered entry ID (not array index)
  pending: Map<string, PendingEntry>; // entryId -> pending info
}

interface StreamState {
  entries: StreamEntry[];
  seq: number;
  groups: Map<string, ConsumerGroup>;
}

export function createMockRedis() {
  const store = {
    hashes: new Map<string, Map<string, string>>(),
    keys: new Map<string, string>(),
    streams: new Map<string, StreamState>(),
    bitmaps: new Map<string, Map<number, number>>(),
  };

  function getOrCreateStream(key: string): StreamState {
    let s = store.streams.get(key);
    if (!s) {
      s = { entries: [], seq: 0, groups: new Map() };
      store.streams.set(key, s);
    }
    return s;
  }

  const pipeline = () => {
    const ops: Array<() => unknown> = [];
    return {
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
      setbit: (key: string, offset: number, value: number) => {
        ops.push(() => {
          const bm = store.bitmaps.get(key) ?? new Map();
          const old = bm.get(offset) ?? 0;
          if (value) bm.set(offset, 1); else bm.delete(offset);
          store.bitmaps.set(key, bm);
          return old;
        });
      },
      xadd: (key: string, id: string, ...fieldValues: string[]) => {
        ops.push(() => {
          const s = getOrCreateStream(key);
          const entryId = id === "*" ? `${Date.now()}-${s.seq++}` : id;
          s.entries.push({ id: entryId, fields: fieldValues });
          return entryId;
        });
      },
      xack: (key: string, group: string, ...ids: string[]) => {
        ops.push(() => {
          const s = store.streams.get(key);
          if (!s) return 0;
          const g = s.groups.get(group);
          if (!g) return 0;
          let acked = 0;
          for (const id of ids) {
            if (g.pending.delete(id)) acked++;
          }
          return acked;
        });
      },
      xdel: (key: string, ...ids: string[]) => {
        ops.push(() => {
          const s = store.streams.get(key);
          if (!s) return 0;
          let deleted = 0;
          for (const id of ids) {
            const idx = s.entries.findIndex((e) => e.id === id);
            if (idx !== -1) { s.entries.splice(idx, 1); deleted++; }
          }
          return deleted;
        });
      },
      exec: async () => ops.map((op) => [null, op()]),
    };
  };

  const mock = {
    _store: store,
    set: vi.fn(async (key: string, val: string, ...args: unknown[]) => {
      const hasNX = args.includes("NX");
      if (hasNX && store.keys.has(key)) return null;
      store.keys.set(key, val);
      return "OK";
    }),
    get: vi.fn(async (key: string) => store.keys.get(key) ?? null),
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
    hscan: vi.fn(async (key: string, _cursor: string, ..._args: unknown[]) => {
      const hash = store.hashes.get(key);
      if (!hash) return ["0", []];
      const fields: string[] = [];
      for (const [field, val] of hash) {
        fields.push(field, val);
      }
      return ["0", fields];
    }),
    time: vi.fn(async () => {
      const now = Date.now();
      const seconds = Math.floor(now / 1000).toString();
      const microseconds = ((now % 1000) * 1000).toString();
      return [seconds, microseconds];
    }),

    // ── Stream operations ──

    xadd: vi.fn(async (key: string, id: string, ...fieldValues: string[]) => {
      const s = getOrCreateStream(key);
      const entryId = id === "*" ? `${Date.now()}-${s.seq++}` : id;
      s.entries.push({ id: entryId, fields: fieldValues });
      return entryId;
    }),

    xgroup: vi.fn(async (...args: string[]) => {
      const subcommand = args[0]?.toUpperCase();
      if (subcommand === "CREATE") {
        const [, key, group, startId] = args;
        const s = getOrCreateStream(key!);
        if (!s.groups.has(group!)) {
          s.groups.set(group!, {
            lastDeliveredId: startId === "0" || startId === "0-0" ? "" : (s.entries[s.entries.length - 1]?.id ?? ""),
            pending: new Map(),
          });
        }
        return "OK";
      }
      throw new Error(`xgroup ${subcommand} not mocked`);
    }),

    xreadgroup: vi.fn(async (...args: unknown[]) => {
      // Parse: 'GROUP', groupName, consumerName, 'COUNT', N, 'BLOCK', ms, 'STREAMS', key, '>'
      let groupName = "", consumerName = "", count = 1, streamKey = "";
      for (let i = 0; i < args.length; i++) {
        const a = String(args[i]).toUpperCase();
        if (a === "GROUP") { groupName = String(args[i + 1]); consumerName = String(args[i + 2]); i += 2; }
        else if (a === "COUNT") { count = Number(args[i + 1]); i++; }
        else if (a === "BLOCK") { i++; } // skip timeout
        else if (a === "STREAMS") { streamKey = String(args[i + 1]); i += 2; break; }
      }

      const s = store.streams.get(streamKey);
      if (!s) return null;
      const g = s.groups.get(groupName);
      if (!g) return null;

      const results: [string, string[]][] = [];
      let delivered = 0;

      // Find entries after lastDeliveredId
      let startIdx = 0;
      if (g.lastDeliveredId) {
        const idx = s.entries.findIndex((e) => e.id === g.lastDeliveredId);
        startIdx = idx === -1 ? 0 : idx + 1;
      }

      for (let i = startIdx; i < s.entries.length && delivered < count; i++) {
        const entry = s.entries[i]!;
        g.pending.set(entry.id, {
          deliveredAt: Date.now(),
          deliveryCount: (g.pending.get(entry.id)?.deliveryCount ?? 0) + 1,
          consumer: consumerName,
        });
        g.lastDeliveredId = entry.id;
        results.push([entry.id, entry.fields]);
        delivered++;
      }

      if (results.length === 0) return null;
      return [[streamKey, results]];
    }),

    xack: vi.fn(async (key: string, group: string, ...ids: string[]) => {
      const s = store.streams.get(key);
      if (!s) return 0;
      const g = s.groups.get(group);
      if (!g) return 0;
      let acked = 0;
      for (const id of ids) {
        if (g.pending.delete(id)) acked++;
      }
      return acked;
    }),

    xdel: vi.fn(async (key: string, ...ids: string[]) => {
      const s = store.streams.get(key);
      if (!s) return 0;
      let deleted = 0;
      for (const id of ids) {
        const idx = s.entries.findIndex((e) => e.id === id);
        if (idx !== -1) {
          s.entries.splice(idx, 1);
          deleted++;
        }
      }
      return deleted;
    }),

    xlen: vi.fn(async (key: string) => {
      return store.streams.get(key)?.entries.length ?? 0;
    }),

    xpending: vi.fn(async (key: string, group: string) => {
      const s = store.streams.get(key);
      if (!s) return [0, null, null, null];
      const g = s.groups.get(group);
      if (!g) return [0, null, null, null];
      return [g.pending.size, null, null, null];
    }),

    xautoclaim: vi.fn(async (key: string, group: string, consumer: string, minIdleTime: number, _start: string, ..._rest: unknown[]) => {
      const s = store.streams.get(key);
      if (!s) return ["0-0", [], []];
      const g = s.groups.get(group);
      if (!g) return ["0-0", [], []];

      const now = Date.now();
      const claimed: [string, string[]][] = [];
      const deletedIds: string[] = [];

      for (const [entryId, info] of g.pending) {
        if (now - info.deliveredAt >= minIdleTime && info.consumer !== consumer) {
          // Find the entry in the stream
          const entry = s.entries.find((e) => e.id === entryId);
          if (entry) {
            info.consumer = consumer;
            info.deliveredAt = now;
            info.deliveryCount++;
            claimed.push([entryId, entry.fields]);
          } else {
            // Entry was deleted from stream but still in PEL
            g.pending.delete(entryId);
            deletedIds.push(entryId);
          }
        }
      }

      return ["0-0", claimed, deletedIds];
    }),

    // ── Bitmap operations ──

    setbit: vi.fn(async (key: string, offset: number, value: number) => {
      const bm = store.bitmaps.get(key) ?? new Map<number, number>();
      const old = bm.get(offset) ?? 0;
      if (value) bm.set(offset, 1); else bm.delete(offset);
      store.bitmaps.set(key, bm);
      return old;
    }),

    getbit: vi.fn(async (key: string, offset: number) => {
      return store.bitmaps.get(key)?.get(offset) ?? 0;
    }),

    bitcount: vi.fn(async (key: string) => {
      return store.bitmaps.get(key)?.size ?? 0;
    }),

    // Lua script dispatch — identity-based matching (not fragile string includes)
    eval: vi.fn(async (script: string, numKeys: number, ...args: unknown[]) => {
      if (script === BATCH_SETBIT_SCRIPT) {
        const key = String(args[0]);
        const from = Number(args[1]);
        const to = Number(args[2]);
        const bm = store.bitmaps.get(key) ?? new Map<number, number>();
        for (let i = from; i <= to; i++) bm.set(i, 1);
        store.bitmaps.set(key, bm);
        return to - from + 1;
      }
      if (script === GET_COMPLETED_RANGE_SCRIPT) {
        const key = String(args[0]);
        const from = Number(args[1]);
        const to = Number(args[2]);
        const bm = store.bitmaps.get(key);
        const completed: number[] = [];
        if (bm) {
          for (let i = from; i <= to; i++) {
            if (bm.get(i) === 1) completed.push(i);
          }
        }
        return completed;
      }
      if (script === CONTIGUOUS_WATERMARK_SCRIPT) {
        const key = String(args[0]);
        const from = Number(args[1]);
        const limit = Number(args[2]);
        const bm = store.bitmaps.get(key);
        if (!bm || bm.size === 0) return from - 1;
        for (let i = from; i < from + limit; i++) {
          if ((bm.get(i) ?? 0) === 0) return i - 1;
        }
        return from + limit - 1;
      }
      // Rate limiter scripts
      if (script.includes("ZREMRANGEBYSCORE")) return [1, 50];
      if (script.includes("reduction")) return 37;
      return null;
    }),

    pipeline,
  } as any;

  return mock;
}
