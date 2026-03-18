// Prometheus metrics for the indexer pipeline.

import {
  Registry,
  Counter,
  Gauge,
  Histogram,
  type CounterConfiguration,
  type GaugeConfiguration,
  type HistogramConfiguration,
} from "prom-client";

const PREFIX = "indexer_";

export class Metrics {
  readonly registry: Registry;

  readonly blocksIndexedTotal: Counter;
  readonly transactionsIndexedTotal: Counter;
  readonly rpcCallsTotal: Counter;
  readonly rpcErrorsTotal: Counter;
  readonly storageFlushesTotal: Counter;

  readonly currentBlock: Gauge;
  readonly bufferSize: Gauge;
  readonly queuePending: Gauge;
  readonly queueProcessing: Gauge;
  readonly queueCompleted: Gauge;
  readonly rateLimiterEffectiveRate: Gauge;
  readonly dlqSize: Gauge;

  readonly rpcDurationSeconds: Histogram;
  readonly storageFlushDurationSeconds: Histogram;
  readonly blockProcessingDurationSeconds: Histogram;

  constructor(chainId: number, workerId: string) {
    this.registry = new Registry();
    this.registry.setDefaultLabels({ chain_id: String(chainId), worker_id: workerId });

    const counter = <T extends string>(cfg: CounterConfiguration<T>) => {
      const c = new Counter({ ...cfg, registers: [this.registry] });
      return c;
    };
    const gauge = <T extends string>(cfg: GaugeConfiguration<T>) => {
      const g = new Gauge({ ...cfg, registers: [this.registry] });
      return g;
    };
    const histogram = <T extends string>(cfg: HistogramConfiguration<T>) => {
      const h = new Histogram({ ...cfg, registers: [this.registry] });
      return h;
    };

    this.blocksIndexedTotal = counter({
      name: `${PREFIX}blocks_indexed_total`,
      help: "Total blocks indexed",
    });
    this.transactionsIndexedTotal = counter({
      name: `${PREFIX}transactions_indexed_total`,
      help: "Total transactions indexed",
    });
    this.rpcCallsTotal = counter({
      name: `${PREFIX}rpc_calls_total`,
      help: "Total RPC calls made",
      labelNames: ["method"],
    });
    this.rpcErrorsTotal = counter({
      name: `${PREFIX}rpc_errors_total`,
      help: "Total RPC errors",
      labelNames: ["error_type"],
    });
    this.storageFlushesTotal = counter({
      name: `${PREFIX}storage_flushes_total`,
      help: "Total storage flush operations",
    });

    this.currentBlock = gauge({
      name: `${PREFIX}current_block`,
      help: "Current block being processed",
    });
    this.bufferSize = gauge({
      name: `${PREFIX}buffer_size`,
      help: "Current buffer size",
    });
    this.queuePending = gauge({
      name: `${PREFIX}queue_pending`,
      help: "Tasks pending in queue",
    });
    this.queueProcessing = gauge({
      name: `${PREFIX}queue_processing`,
      help: "Tasks currently processing",
    });
    this.queueCompleted = gauge({
      name: `${PREFIX}queue_completed`,
      help: "Tasks completed",
    });
    this.rateLimiterEffectiveRate = gauge({
      name: `${PREFIX}rate_limiter_effective_rate`,
      help: "Effective rate limiter rate",
    });
    this.dlqSize = gauge({
      name: `${PREFIX}dlq_size`,
      help: "Dead-letter queue size (tasks that exceeded max retries)",
    });

    this.rpcDurationSeconds = histogram({
      name: `${PREFIX}rpc_duration_seconds`,
      help: "RPC call duration in seconds",
      buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    });
    this.storageFlushDurationSeconds = histogram({
      name: `${PREFIX}storage_flush_duration_seconds`,
      help: "Storage flush duration in seconds",
      buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    });
    this.blockProcessingDurationSeconds = histogram({
      name: `${PREFIX}block_processing_duration_seconds`,
      help: "Block processing duration in seconds",
      buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
    });
  }
}
