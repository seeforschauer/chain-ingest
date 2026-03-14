// PostgreSQL storage with batch inserts, range partitioning, and BRIN indexes.

import { Pool, type PoolClient } from "pg";
import type { BlockData, TransactionData, ReceiptData } from "./rpc.js";
import { log } from "./logger.js";

const PARTITION_RANGE = 1_000_000; // ~2 weeks on Ethereum mainnet
const BATCH_CHUNK_SIZE = 5_000;    // PG max 65,535 params; 5K × 10 cols = 50K

export class Storage {
  private pool: Pool;
  private readonly knownPartitions = new Set<string>();

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      max: 20,
      idleTimeoutMillis: 30_000,
      statement_timeout: 30_000,
      idle_in_transaction_session_timeout: 60_000,
    });
  }

  async migrate(): Promise<void> {
    // Phase 1: transactional schema creation (tables + state)
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`CREATE SCHEMA IF NOT EXISTS raw`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS raw.blocks (
          block_number    BIGINT NOT NULL,
          block_hash      TEXT NOT NULL,
          parent_hash     TEXT NOT NULL,
          block_timestamp TIMESTAMPTZ NOT NULL,
          gas_used        NUMERIC NOT NULL,
          gas_limit       NUMERIC NOT NULL,
          base_fee_per_gas NUMERIC,
          tx_count        INT NOT NULL DEFAULT 0,
          indexed_by      TEXT,
          indexed_at      TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (block_number)
        ) PARTITION BY RANGE (block_number)
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS raw.transactions (
          tx_hash         TEXT NOT NULL,
          block_number    BIGINT NOT NULL,
          tx_index        INT NOT NULL,
          from_address    TEXT NOT NULL,
          to_address      TEXT,
          value           NUMERIC NOT NULL,
          gas_price       NUMERIC,
          gas_limit       NUMERIC NOT NULL,
          input_data      TEXT,
          nonce           BIGINT NOT NULL,
          PRIMARY KEY (block_number, tx_hash)
        ) PARTITION BY RANGE (block_number)
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS raw.receipts (
          tx_hash             TEXT NOT NULL,
          block_number        BIGINT NOT NULL,
          status              SMALLINT,
          gas_used            NUMERIC NOT NULL,
          cumulative_gas_used NUMERIC NOT NULL,
          contract_address    TEXT,
          log_count           INT NOT NULL DEFAULT 0,
          PRIMARY KEY (block_number, tx_hash)
        ) PARTITION BY RANGE (block_number)
      `);

      // Highest-volume table — no FK to transactions (FKs on partitioned tables
      // add write overhead; data is always inserted atomically in one transaction)
      await client.query(`
        CREATE TABLE IF NOT EXISTS raw.logs (
          id               BIGSERIAL,
          tx_hash          TEXT NOT NULL,
          log_index        INT NOT NULL,
          address          TEXT NOT NULL,
          topics           JSONB NOT NULL,
          data             TEXT NOT NULL,
          block_number     BIGINT NOT NULL,
          PRIMARY KEY (block_number, id),
          UNIQUE (block_number, tx_hash, log_index)
        ) PARTITION BY RANGE (block_number)
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS raw.indexer_state (
          chain_id         INT NOT NULL PRIMARY KEY,
          high_water_mark  BIGINT NOT NULL DEFAULT 0,
          last_indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          worker_id        TEXT
        )
      `);

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    // Phase 2: partition + index DDL — runs outside transaction to avoid
    // holding ACCESS EXCLUSIVE locks on parent tables during the full migration.
    // All statements are idempotent (IF NOT EXISTS / error code 42P07).
    const ddlClient = await this.pool.connect();
    try {
      for (let start = 0; start < 10_000_000; start += PARTITION_RANGE) {
        await this.createPartitionIfNotExists(ddlClient, "raw.blocks", start);
        await this.createPartitionIfNotExists(ddlClient, "raw.transactions", start);
        await this.createPartitionIfNotExists(ddlClient, "raw.receipts", start);
        await this.createPartitionIfNotExists(ddlClient, "raw.logs", start);
      }

      // BRIN for physically ordered data (~100x smaller than B-tree)
      await ddlClient.query(`CREATE INDEX IF NOT EXISTS idx_blocks_timestamp
        ON raw.blocks USING BRIN (block_timestamp)`);
      await ddlClient.query(`CREATE INDEX IF NOT EXISTS idx_tx_block
        ON raw.transactions USING BRIN (block_number)`);
      await ddlClient.query(`CREATE INDEX IF NOT EXISTS idx_rcpt_block
        ON raw.receipts USING BRIN (block_number)`);
      await ddlClient.query(`CREATE INDEX IF NOT EXISTS idx_logs_block
        ON raw.logs USING BRIN (block_number)`);

      // B-tree for random-access point lookups
      await ddlClient.query(`CREATE INDEX IF NOT EXISTS idx_tx_from
        ON raw.transactions (from_address)`);
      await ddlClient.query(`CREATE INDEX IF NOT EXISTS idx_tx_to
        ON raw.transactions (to_address)`);
      await ddlClient.query(`CREATE INDEX IF NOT EXISTS idx_logs_address
        ON raw.logs (address)`);

      // GIN for `topics @> '["0xddf252..."]'` (ERC-20 Transfer filtering)
      await ddlClient.query(`
        CREATE INDEX IF NOT EXISTS idx_logs_topics
          ON raw.logs USING GIN (topics);
      `);

      log("info", "Schema migration complete");
    } finally {
      ddlClient.release();
    }
  }

  private async createPartitionIfNotExists(
    client: PoolClient,
    parentTable: string,
    rangeStart: number
  ): Promise<void> {
    const rangeEnd = rangeStart + PARTITION_RANGE;
    const suffix = `p${rangeStart}_${rangeEnd}`;
    const partitionName = `${parentTable.replace(".", "_")}_${suffix}`;

    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${partitionName}
          PARTITION OF ${parentTable}
          FOR VALUES FROM (${rangeStart}) TO (${rangeEnd})
      `);
    } catch (err: any) {
      if (err.code !== "42P07") throw err; // 42P07 = duplicate_table
    }
  }

  /** Ensure partition exists, using in-memory cache to skip repeated pg_class queries. */
  private async ensurePartition(blockNumber: number): Promise<void> {
    const rangeStart =
      Math.floor(blockNumber / PARTITION_RANGE) * PARTITION_RANGE;

    const cacheKey = `p${rangeStart}`;
    if (this.knownPartitions.has(cacheKey)) return;

    const rangeEnd = rangeStart + PARTITION_RANGE;
    const blocksSuffix = `raw_blocks_p${rangeStart}_${rangeEnd}`;
    const txSuffix = `raw_transactions_p${rangeStart}_${rangeEnd}`;
    const rcptSuffix = `raw_receipts_p${rangeStart}_${rangeEnd}`;
    const logsSuffix = `raw_logs_p${rangeStart}_${rangeEnd}`;

    const { rows } = await this.pool.query(
      `SELECT relname FROM pg_class
       WHERE relname IN ($1, $2, $3, $4)`,
      [blocksSuffix, txSuffix, rcptSuffix, logsSuffix]
    );

    if (rows.length >= 4) {
      this.knownPartitions.add(cacheKey);
      return;
    }

    // DDL outside main transaction to avoid deadlocks with concurrent workers
    const client = await this.pool.connect();
    try {
      await this.createPartitionIfNotExists(client, "raw.blocks", rangeStart);
      await this.createPartitionIfNotExists(client, "raw.transactions", rangeStart);
      await this.createPartitionIfNotExists(client, "raw.receipts", rangeStart);
      await this.createPartitionIfNotExists(client, "raw.logs", rangeStart);
      this.knownPartitions.add(cacheKey);
    } finally {
      client.release();
    }
  }

  async insertBlocks(
    batch: Array<{ block: BlockData; receipts: ReceiptData[]; workerId: string }>
  ): Promise<void> {
    if (batch.length === 0) return;

    const partitionStarts = new Set<number>();
    for (const { block } of batch) {
      const blockNumber = parseInt(block.number, 16);
      const rangeStart = Math.floor(blockNumber / PARTITION_RANGE) * PARTITION_RANGE;
      partitionStarts.add(rangeStart);
    }
    for (const rangeStart of partitionStarts) {
      await this.ensurePartition(rangeStart);
    }

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      for (const { block, receipts, workerId } of batch) {
        await this.writeBlockData(client, block, receipts, workerId);
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async insertBlock(
    block: BlockData,
    receipts: ReceiptData[],
    workerId: string
  ): Promise<void> {
    const blockNumber = parseInt(block.number, 16);
    await this.ensurePartition(blockNumber);

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await this.writeBlockData(client, block, receipts, workerId);
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  private async writeBlockData(
    client: PoolClient,
    block: BlockData,
    receipts: ReceiptData[],
    workerId: string
  ): Promise<void> {
    const blockNumber = parseInt(block.number, 16);
    const timestamp = new Date(parseInt(block.timestamp, 16) * 1000);

    await client.query(
      `INSERT INTO raw.blocks
        (block_number, block_hash, parent_hash, block_timestamp,
         gas_used, gas_limit, base_fee_per_gas, tx_count, indexed_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (block_number) DO NOTHING`,
      [
        blockNumber,
        block.hash,
        block.parentHash,
        timestamp,
        hexToBigInt(block.gasUsed),
        hexToBigInt(block.gasLimit),
        block.baseFeePerGas ? hexToBigInt(block.baseFeePerGas) : null,
        block.transactions.length,
        workerId,
      ]
    );

    const receiptMap = new Map<string, ReceiptData>();
    for (const r of receipts) {
      receiptMap.set(r.transactionHash, r);
    }

    if (block.transactions.length > 0) {
      for (let i = 0; i < block.transactions.length; i += BATCH_CHUNK_SIZE) {
        const chunk = block.transactions.slice(i, i + BATCH_CHUNK_SIZE);
        await this.batchInsertTransactions(client, chunk, blockNumber);
        await this.batchInsertReceipts(client, chunk, receiptMap, blockNumber);
        await this.batchInsertLogs(client, chunk, receiptMap, blockNumber);
      }
    }
  }

  private async batchInsertTransactions(
    client: PoolClient,
    transactions: TransactionData[],
    blockNumber: number
  ): Promise<void> {
    const values: unknown[] = [];
    const placeholders: string[] = [];

    for (let i = 0; i < transactions.length; i++) {
      const tx = transactions[i]!;
      const offset = i * 10;
      placeholders.push(
        `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10})`
      );
      values.push(
        tx.hash,
        blockNumber,
        parseInt(tx.transactionIndex, 16),
        tx.from,
        tx.to,
        hexToBigInt(tx.value),
        tx.gasPrice ? hexToBigInt(tx.gasPrice) : null,
        hexToBigInt(tx.gas),
        tx.input === "0x" ? null : tx.input,
        parseInt(tx.nonce, 16)
      );
    }

    await client.query(
      `INSERT INTO raw.transactions
        (tx_hash, block_number, tx_index, from_address, to_address,
         value, gas_price, gas_limit, input_data, nonce)
       VALUES ${placeholders.join(", ")}
       ON CONFLICT (block_number, tx_hash) DO NOTHING`,
      values
    );
  }

  private async batchInsertReceipts(
    client: PoolClient,
    transactions: TransactionData[],
    receiptMap: Map<string, ReceiptData>,
    blockNumber: number
  ): Promise<void> {
    const values: unknown[] = [];
    const placeholders: string[] = [];
    let paramIdx = 0;

    for (const tx of transactions) {
      const receipt = receiptMap.get(tx.hash);
      if (!receipt) continue;

      const offset = paramIdx * 7;
      placeholders.push(
        `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7})`
      );
      values.push(
        receipt.transactionHash,
        blockNumber,
        receipt.status ? parseInt(receipt.status, 16) : null,
        hexToBigInt(receipt.gasUsed),
        hexToBigInt(receipt.cumulativeGasUsed),
        receipt.contractAddress,
        receipt.logs.length
      );
      paramIdx++;
    }

    if (placeholders.length === 0) return;

    await client.query(
      `INSERT INTO raw.receipts
        (tx_hash, block_number, status, gas_used, cumulative_gas_used,
         contract_address, log_count)
       VALUES ${placeholders.join(", ")}
       ON CONFLICT (block_number, tx_hash) DO NOTHING`,
      values
    );
  }

  private async batchInsertLogs(
    client: PoolClient,
    transactions: TransactionData[],
    receiptMap: Map<string, ReceiptData>,
    blockNumber: number
  ): Promise<void> {
    // Logs have 1:N fanout from transactions (DeFi swaps emit 5-10 each).
    // Chunking by tx count doesn't bound log count — flush per-log instead.
    let values: unknown[] = [];
    let placeholders: string[] = [];
    let paramIdx = 0;

    for (const tx of transactions) {
      const receipt = receiptMap.get(tx.hash);
      if (!receipt) continue;

      for (const logEntry of receipt.logs) {
        const offset = paramIdx * 6;
        placeholders.push(
          `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`
        );
        values.push(
          logEntry.transactionHash,
          parseInt(logEntry.logIndex, 16),
          logEntry.address,
          JSON.stringify(logEntry.topics),
          logEntry.data,
          blockNumber
        );
        paramIdx++;

        if (paramIdx >= BATCH_CHUNK_SIZE) {
          await client.query(
            `INSERT INTO raw.logs
              (tx_hash, log_index, address, topics, data, block_number)
             VALUES ${placeholders.join(", ")}
             ON CONFLICT (block_number, tx_hash, log_index) DO NOTHING`,
            values
          );
          values = [];
          placeholders = [];
          paramIdx = 0;
        }
      }
    }

    if (placeholders.length === 0) return;

    await client.query(
      `INSERT INTO raw.logs
        (tx_hash, log_index, address, topics, data, block_number)
       VALUES ${placeholders.join(", ")}
       ON CONFLICT (block_number, tx_hash, log_index) DO NOTHING`,
      values
    );
  }

  async updateWatermark(
    chainId: number,
    blockNumber: number,
    workerId: string
  ): Promise<void> {
    await this.pool.query(
      {
        name: "upsert_watermark",
        text: `INSERT INTO raw.indexer_state (chain_id, high_water_mark, last_indexed_at, worker_id)
         VALUES ($1, $2, NOW(), $3)
         ON CONFLICT (chain_id) DO UPDATE SET
           high_water_mark = GREATEST(raw.indexer_state.high_water_mark, EXCLUDED.high_water_mark),
           last_indexed_at = EXCLUDED.last_indexed_at,
           worker_id = EXCLUDED.worker_id`,
        values: [chainId, blockNumber, workerId],
      }
    );
  }

  async getWatermark(chainId: number): Promise<number> {
    const { rows } = await this.pool.query(
      {
        name: "get_watermark",
        text: `SELECT high_water_mark FROM raw.indexer_state WHERE chain_id = $1`,
        values: [chainId],
      }
    );
    return rows.length > 0 ? Number(rows[0].high_water_mark) : 0;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

function hexToBigInt(hex: string): string {
  return BigInt(hex).toString();
}
