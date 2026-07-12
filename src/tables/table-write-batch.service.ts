import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';
import type { TableTemplate } from './entities/table-template.entity';
import { chunkRows, fetchQueuedRowsCapped, fetchRowsByIds } from './write-sweep-query.util';
import type { RowWithConnection } from './write-sweep-query.util';
import { ParamList } from '../core/sql/sql-params.util';

/** Tope por defecto de filas `queued` sacadas por tabla en cada pasada del cron. */
const DEFAULT_MAX_RECORDS_PER_POLL = 10_000;

/**
 * Tope duro de filas por llamada HTTP saliente, independiente de
 * `write.batch.maxBatchSize` (que solo agrupa; el usuario podría no
 * configurarlo, o configurarlo por encima de esto). Sin este tope, un sweep de
 * hasta `DEFAULT_MAX_RECORDS_PER_POLL` filas en una partición sin `maxBatchSize`
 * iría entero en una sola petición al sistema externo.
 */
const HARD_MAX_BATCH_SIZE = 1000;

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
   * background: submitting several batches sequentially to a possibly-slow
   * external system is not something the triggering HTTP request should
   * block on.
   *
   * `connectionId` — forwarded verbatim by the hub from the Flow origin that
   * scheduled this tick — scopes the sweep to one connection's queued rows, so
   * the same table (e.g. shared across several SII company connections) can
   * have an independent cron cadence per connection.
   */
  async trigger(tableKey: string, trigger: 'schedule' | 'manual' = 'schedule', connectionId?: string): Promise<{ queued: number }> {
    const template = await this.templates.getByKey(tableKey);
    if (!template.write) {
      throw new BadRequestException(`Table "${tableKey}" has no write config to submit rows through`);
    }
    // Synchronous pre-count so the (fire-and-forget) caller gets an immediate,
    // accurate "how much am I submitting" number for its UI/toast, without
    // waiting on the actual outbound batches.
    const p = new ParamList();
    const where = [`table_key = ${p.push(tableKey)}`, `submission_status = 'queued'`];
    if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
    const [{ count }]: { count: string }[] = await this.dataSource.query(
      `SELECT count(*)::text AS count FROM table_rows WHERE ${where.join(' AND ')}`,
      p.all,
    );
    void this.submitAllQueued(template, trigger, connectionId).catch((err) => {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error(`Batch submit table=${tableKey} failed: ${message}`);
    });
    return { queued: Number(count) };
  }

  /**
   * The actual full sweep for an already-validated template — public so tests
   * can await it directly instead of racing the fire-and-forget in
   * `trigger()`. Each discovered group is submitted through the connection
   * its rows were ingested under (optionally narrowed to one `connectionId`),
   * never through a single fixed connection.
   */
  async submitAllQueued(template: TableTemplate, trigger: 'schedule' | 'manual' = 'schedule', connectionId?: string): Promise<void> {
    // Tope por tabla y pasada: solo se sacan hasta `maxRecordsPerPoll` filas
    // `queued` (las más antiguas primero); el resto espera a la siguiente
    // pasada. El troceo por grupo/`maxBatchSize` se aplica *dentro* de ese tope.
    const limit = template.write?.batch?.maxRecordsPerPoll ?? DEFAULT_MAX_RECORDS_PER_POLL;
    const queued = await fetchQueuedRowsCapped(this.dataSource, template.key, limit, connectionId);
    if (!queued.length) return;
    await this.partitionAndSubmit(template, queued, trigger);
  }

  /**
   * Force-submit a specific selection of rows (the FE "Forzar envío" over
   * checked rows), bypassing the poll/debounce. Only rows still eligible —
   * `submission_status IN ('queued','error')` — are sent; the rest of the
   * selection is counted as `skipped` so nothing already accepted/pending is
   * re-presented to the external system. Eligible rows are partitioned exactly
   * like the queued sweeps (by ingestion connection and by
   * `write.batch.groupBy`), so each partition ships through its own
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
    const eligible = await fetchRowsByIds(this.dataSource, tableKey, ids, connectionId);
    const skipped = ids.length - eligible.length;
    if (!eligible.length) return { submitted: 0, skipped };

    await this.partitionAndSubmit(template, eligible, 'manual');
    return { submitted: eligible.length, skipped };
  }

  /**
   * Particiona un conjunto de filas por (conexión de ingesta, `write.batch.groupBy`)
   * y submite cada partición como uno o más batches, troceados por
   * `write.batch.maxBatchSize` — nunca por encima de `HARD_MAX_BATCH_SIZE` (1000),
   * así que aunque no se configure `maxBatchSize` (o se configure más alto), ninguna
   * llamada saliente lleva más de 1000 filas. Núcleo compartido por el barrido
   * programado (`submitAllQueued`, hasta 10.000 filas `queued` por pasada) y el
   * force-submit (`submitByIds`, selección por checkbox — ya acotada a 1000 por el FE
   * al limitarse a la página actual), de modo que ambos particionan/trocean igual.
   */
  private async partitionAndSubmit(
    template: TableTemplate,
    rows: RowWithConnection[],
    trigger: 'schedule' | 'manual',
  ): Promise<void> {
    const groupBy = template.write?.batch?.groupBy ?? [];
    const maxBatchSize = Math.min(template.write?.batch?.maxBatchSize ?? HARD_MAX_BATCH_SIZE, HARD_MAX_BATCH_SIZE);

    const partitions = new Map<
      string,
      { connectionId: string | null; groupValues: Record<string, string>; rows: { id: string; data: Record<string, unknown> }[] }
    >();
    for (const row of rows) {
      const connId = row.connectionId;
      const groupValues: Record<string, string> = {};
      for (const col of groupBy) groupValues[col] = String(row.data[col] ?? '');
      const key = `${connId ?? ''}::${JSON.stringify(groupValues)}`;
      const bucket = partitions.get(key) ?? { connectionId: connId, groupValues, rows: [] };
      bucket.rows.push({ id: row.id, data: row.data });
      partitions.set(key, bucket);
    }

    for (const part of partitions.values()) {
      const chunks = chunkRows(part.rows, maxBatchSize);
      for (const rowsChunk of chunks) {
        const result = await this.rows.submitGroup(template, rowsChunk, {
          trigger,
          groupValues: groupBy.length ? part.groupValues : null,
          connectionId: part.connectionId,
        });
        if (result?.status === 'error') {
          this.logger.warn(`Submit table=${template.key} batch=${result.batchId} failed: ${result.error}`);
        }
      }
    }
  }
}
