import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';
import type { TableTemplate } from './entities/table-template.entity';
import { chunkRows, discoverQueuedGroups, fetchQueuedRows, fetchRowsByIds } from './write-sweep-query.util';
import { ParamList } from '../core/sql/sql-params.util';

/**
 * Schedule-mode counterpart to WriteSweepProcessor: instead of one debounced
 * job for one already-known batch group (event mode), this is invoked
 * directly by the hub's cron and must sweep *every* group of a template in
 * one go — there is no incoming groupValues to key off, so it discovers which
 * groups currently have `queued` rows before fetching/submitting each.
 *
 * Deliberately NOT restricted to `write.trigger==='schedule'` templates:
 * BullMQ can silently drop an edit's debounce enqueue when it arrives while
 * the previous sweep job for that group is already `active` (mid-HTTP-call —
 * see the plan's open risk on same-jobId races), which would otherwise leave
 * that row `queued` forever with nothing left to wake it up. The hub is meant
 * to also schedule `table.write.batchSubmit` at a low cadence (e.g. every few
 * minutes) for `event`-mode tables as a safety net — this method works
 * identically for either trigger mode, since it only ever looks at what's
 * currently `queued` in the database, never at how a template is configured
 * to trigger.
 */
@Injectable()
export class TableWriteBatchService {
  private readonly logger = new Logger(TableWriteBatchService.name);

  constructor(
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  /**
   * Validate synchronously (so a bad tableKey/missing write config surfaces
   * as an immediate 4xx to the hub caller), then run the actual sweep in the
   * background — mirrors SourcePollService.poll()'s split, for the same
   * reason: submitting several batches sequentially to a possibly-slow
   * external system is not something the triggering HTTP request should
   * block on.
   *
   * `connectionId` — forwarded verbatim by the hub from the Flow origin that
   * scheduled this tick — scopes the sweep to one connection's queued rows on
   * a perConnection table, so the same table (e.g. shared across several AEAT
   * company connections) can have an independent cron cadence per connection.
   * Ignored for non-perConnection templates, which always sweep every row.
   */
  async trigger(tableKey: string, trigger: 'schedule' | 'manual' = 'schedule', connectionId?: string): Promise<{ queued: number }> {
    const template = await this.templates.getByKey(tableKey);
    if (!template.write) {
      throw new BadRequestException(`Table "${tableKey}" has no write config to submit rows through`);
    }
    const scopedConnectionId = template.perConnection ? connectionId : undefined;
    // Synchronous pre-count so the (fire-and-forget) caller gets an immediate,
    // accurate "how much am I submitting" number for its UI/toast, without
    // waiting on the actual outbound batches.
    const p = new ParamList();
    const where = [`table_key = ${p.push(tableKey)}`, `submission_status = 'queued'`];
    if (scopedConnectionId) where.push(`connection_id = ${p.push(scopedConnectionId)}`);
    const [{ count }]: { count: string }[] = await this.dataSource.query(
      `SELECT count(*)::text AS count FROM table_rows WHERE ${where.join(' AND ')}`,
      p.all,
    );
    void this.submitAllQueued(template, trigger, scopedConnectionId).catch((err) => {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error(`Batch submit table=${tableKey} failed: ${message}`);
    });
    return { queued: Number(count) };
  }

  /**
   * The actual full sweep for an already-validated template — public so tests
   * can await it directly instead of racing the fire-and-forget in
   * `trigger()`. When the template is perConnection, each discovered group is
   * submitted through the connection its rows were ingested under (optionally
   * narrowed to one `connectionId`), never through a single fixed connection.
   */
  async submitAllQueued(template: TableTemplate, trigger: 'schedule' | 'manual' = 'schedule', connectionId?: string): Promise<void> {
    const groupBy = template.write?.batch?.groupBy ?? [];
    const maxBatchSize = template.write?.batch?.maxBatchSize;
    const groups = await discoverQueuedGroups(this.dataSource, template.key, groupBy, {
      perConnection: template.perConnection,
      connectionId,
    });

    for (const group of groups) {
      const queued = await fetchQueuedRows(this.dataSource, template.key, group.groupValues, group.connectionId);
      if (!queued.length) continue;

      const chunks = maxBatchSize ? chunkRows(queued, maxBatchSize) : [queued];
      for (const rowsChunk of chunks) {
        const result = await this.rows.submitGroup(template, rowsChunk, { trigger, groupValues: group.groupValues, connectionId: group.connectionId });
        if (result?.status === 'error') {
          this.logger.warn(`Batch submit table=${template.key} batch=${result.batchId} failed: ${result.error}`);
        }
      }
    }
  }

  /**
   * Force-submit a specific selection of rows (the FE "Forzar envío" over
   * checked rows), bypassing the poll/debounce. Only rows still eligible —
   * `submission_status IN ('queued','error')` — are sent; the rest of the
   * selection is counted as `skipped` so nothing already accepted/pending is
   * re-presented to the external system. Eligible rows are partitioned exactly
   * like the queued sweeps (by ingestion connection on perConnection tables and
   * by `write.batch.groupBy`), so each partition ships through its own
   * connection and honours `maxBatchSize`.
   */
  async submitByIds(
    tableKey: string,
    ids: string[],
    connectionId?: string,
  ): Promise<{ submitted: number; skipped: number }> {
    const template = await this.templates.getByKey(tableKey);
    if (!template.write) {
      throw new BadRequestException(`Table "${tableKey}" has no write config to submit rows through`);
    }
    const scopedConnectionId = template.perConnection ? connectionId : undefined;
    const eligible = await fetchRowsByIds(this.dataSource, tableKey, ids, scopedConnectionId);
    const skipped = ids.length - eligible.length;
    if (!eligible.length) return { submitted: 0, skipped };

    const groupBy = template.write.batch?.groupBy ?? [];
    const maxBatchSize = template.write.batch?.maxBatchSize;

    // Partition in memory by (connection, groupBy values) — same shape as
    // discoverQueuedGroups, but over the explicit selection instead of a query.
    const partitions = new Map<string, { connectionId: string | null; groupValues: Record<string, string>; rows: { id: string; data: Record<string, unknown> }[] }>();
    for (const row of eligible) {
      const connId = template.perConnection ? row.connectionId : null;
      const groupValues: Record<string, string> = {};
      for (const col of groupBy) groupValues[col] = String(row.data[col] ?? '');
      const key = `${connId ?? ''}::${JSON.stringify(groupValues)}`;
      const bucket = partitions.get(key) ?? { connectionId: connId, groupValues, rows: [] };
      bucket.rows.push({ id: row.id, data: row.data });
      partitions.set(key, bucket);
    }

    for (const part of partitions.values()) {
      const chunks = maxBatchSize ? chunkRows(part.rows, maxBatchSize) : [part.rows];
      for (const rowsChunk of chunks) {
        const result = await this.rows.submitGroup(template, rowsChunk, {
          trigger: 'manual',
          groupValues: groupBy.length ? part.groupValues : null,
          connectionId: part.connectionId,
        });
        if (result?.status === 'error') {
          this.logger.warn(`Force submit table=${tableKey} batch=${result.batchId} failed: ${result.error}`);
        }
      }
    }

    return { submitted: eligible.length, skipped };
  }
}
