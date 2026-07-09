import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { InjectQueue } from '@nestjs/bullmq';
import { DataSource } from 'typeorm';
import type { Queue } from 'bullmq';
import { createHash, randomUUID } from 'node:crypto';
import { TableTemplate } from './entities/table-template.entity';
import { DatasetDeleteParams, DatasetPage, DatasetQuery, DatasetUpdateResult } from '../datasets/dataset.types';
import { SourceConnectionsService } from '../connections/source-connections.service';
import { SourceHttpClient } from '../connections/source-http.client';
import { QUEUES, DEFAULT_JOB_OPTS } from '../core/queues/queues.constants';
import { WriteSweepJobData } from './write-sweep.types';
import { TableWriteRunService } from './table-write-run.service';
import { assertColumnKey, assertTableKey, escapeLike, isUuid, ParamList, sqlStringLiteral } from '../core/sql/sql-params.util';

interface TableRowRow {
  id: string;
  data: Record<string, unknown>;
  created_at: Date;
  updated_at: Date;
  write_status: 'sent' | 'error' | null;
  write_error: string | null;
  last_written_at: Date | null;
  external_ref: string | null;
  submission_status: string | null;
  aeat_response: Record<string, unknown> | null;
}

@Injectable()
export class TableRowsService {
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly connections: SourceConnectionsService,
    private readonly client: SourceHttpClient,
    @InjectQueue(QUEUES.WRITE_SWEEP) private readonly writeSweep: Queue<WriteSweepJobData>,
    private readonly writeRuns: TableWriteRunService,
  ) {}

  /**
   * Store one or more rows for a template. Upserts by `idField` (scoped to
   * connectionId) when the template declares one; otherwise appends.
   */
  async ingest(
    template: TableTemplate,
    rows: Record<string, unknown>[],
    connectionId: string,
    traceId?: string,
  ): Promise<{ inserted: number; upserted: number }> {
    let inserted = 0;
    let upserted = 0;
    const scope = template.perConnection ? connectionId : '';
    const affected: { id: string; data: Record<string, unknown> }[] = [];

    if (template.idField) {
      // idField/table.key are already validated by the DTO when the template
      // was saved (and by TableIndexManagerService when the unique index was
      // built); re-checked here as defense in depth since they're interpolated
      // as literal SQL text below — Postgres requires the ON CONFLICT partial
      // index predicate to match the index's predicate verbatim, not bound.
      assertColumnKey(template.idField);
      assertTableKey(template.key);
      const idExpr = `(data ->> ${sqlStringLiteral(template.idField)})`;
      const tableKeyLit = sqlStringLiteral(template.key);

      for (const data of rows) {
        const idValue = data[template.idField];
        if (idValue === undefined || idValue === null || idValue === '') {
          // No id present → fall back to insert so the row is not lost.
          const [{ id }]: { id: string }[] = await this.dataSource.query(
            `INSERT INTO table_rows (table_key, connection_id, data, trace_id) VALUES ($1, $2, $3::jsonb, $4) RETURNING id`,
            [template.key, scope, JSON.stringify(data), traceId ?? null],
          );
          affected.push({ id, data });
          inserted++;
          continue;
        }
        const [{ id }]: { id: string }[] = await this.dataSource.query(
          `INSERT INTO table_rows (table_key, connection_id, data, trace_id)
           VALUES ($1, $2, $3::jsonb, $4)
           ON CONFLICT (connection_id, ${idExpr}) WHERE table_key = ${tableKeyLit}
           DO UPDATE SET data = EXCLUDED.data, trace_id = EXCLUDED.trace_id
           RETURNING id`,
          [template.key, scope, JSON.stringify(data), traceId ?? null],
        );
        affected.push({ id, data });
        upserted++;
      }
    } else {
      if (rows.length) {
        const values: string[] = [];
        const params: unknown[] = [];
        rows.forEach((data, idx) => {
          const base = idx * 4;
          values.push(`($${base + 1}, $${base + 2}, $${base + 3}::jsonb, $${base + 4})`);
          params.push(template.key, scope, JSON.stringify(data), traceId ?? null);
        });
        const returned: { id: string }[] = await this.dataSource.query(
          `INSERT INTO table_rows (table_key, connection_id, data, trace_id) VALUES ${values.join(',')} RETURNING id`,
          params,
        );
        // A single multi-VALUES INSERT with no ON CONFLICT/trigger returns rows
        // in VALUES order — safe to zip back against the input by index.
        returned.forEach((r, idx) => affected.push({ id: r.id, data: rows[idx] }));
      }
      inserted += rows.length;
    }

    await this.markQueuedAndScheduleSweep(template, affected, template.perConnection ? connectionId : null);

    return { inserted, upserted };
  }

  /** Query rows for a template honoring only filterable/sortable columns. */
  async query(template: TableTemplate, params: DatasetQuery): Promise<DatasetPage> {
    const filterable = new Set(template.columns.filter((c) => c.filterable).map((c) => c.key));
    const sortable = new Set(template.columns.filter((c) => c.sortable).map((c) => c.key));

    const p = new ParamList();
    const where: string[] = [`table_key = ${p.push(template.key)}`];
    if (template.perConnection) where.push(`connection_id = ${p.push(params.connectionId ?? '')}`);

    const dateRangeBounds = new Map<string, { from?: string; until?: string }>();

    // Per-column filters: substring match for strings, exact otherwise. Date
    // columns arrive as `<key>_from`/`<key>_until` (see table-dataset.bridge)
    // and are combined into a single range on `data->>'<key>'`. Values are
    // compared as text — ingested rows keep whatever the source sent (ISO date
    // strings in practice), never cast to a real timestamp.
    if (params.filters) {
      for (const [k, v] of Object.entries(params.filters)) {
        if (v === '') continue;
        const rangeMatch = /^(.+)_(from|until)$/.exec(k);
        if (rangeMatch) {
          const [, baseKey, edge] = rangeMatch;
          if (!filterable.has(baseKey)) continue;
          if (template.columns.find((c) => c.key === baseKey)?.type !== 'date') continue;
          const bounds = dateRangeBounds.get(baseKey) ?? {};
          bounds[edge as 'from' | 'until'] = v;
          dateRangeBounds.set(baseKey, bounds);
          continue;
        }

        if (!filterable.has(k)) continue;
        const col = template.columns.find((c) => c.key === k);
        if (col?.type === 'number') {
          const num = Number(v);
          // NaN never matches (mirrors the old BSON NaN !== NaN behavior)
          // instead of letting the ::numeric cast throw.
          where.push(Number.isNaN(num) ? 'false' : `(data ->> ${p.push(k)})::numeric = ${p.push(num)}`);
        } else if (col?.type === 'boolean') {
          where.push(`(data ->> ${p.push(k)})::boolean = ${p.push(v === 'true')}`);
        } else {
          where.push(`(data ->> ${p.push(k)}) ILIKE ${p.push(`%${escapeLike(v)}%`)}`);
        }
      }
    }
    for (const [baseKey, bounds] of dateRangeBounds) {
      if (bounds.from !== undefined) where.push(`(data ->> ${p.push(baseKey)}) >= ${p.push(bounds.from)}`);
      if (bounds.until !== undefined) where.push(`(data ->> ${p.push(baseKey)}) <= ${p.push(bounds.until)}`);
    }

    // Free-text search via the generated tsvector column (GIN-indexed) —
    // reemplaza el índice `$text` wildcard de Mongo.
    if (params.search) {
      where.push(`search_vector @@ plainto_tsquery('simple', ${p.push(params.search)})`);
    }

    // Snapshot here — the count query's SQL only ever references the WHERE
    // params above; any params pushed later (sort/limit/offset) must never
    // leak into its bind list, or Postgres rejects the mismatched param count.
    const whereSql = where.join(' AND ');
    const countParams = [...p.all];

    // Sort: requested sortable column, else the template default, else newest first.
    let orderBy = 'created_at DESC';
    if (params.sort && sortable.has(params.sort.key)) {
      orderBy = `(data ->> ${p.push(params.sort.key)}) ${params.sort.dir === 'asc' ? 'ASC' : 'DESC'}`;
    } else if (template.defaultSort && sortable.has(template.defaultSort.key)) {
      orderBy = `(data ->> ${p.push(template.defaultSort.key)}) ${template.defaultSort.dir === 'asc' ? 'ASC' : 'DESC'}`;
    }

    const limitPh = p.push(params.pageSize);
    const offsetPh = p.push((params.page - 1) * params.pageSize);

    const [rows, countRows] = await Promise.all([
      this.dataSource.query(
        `SELECT id, data, created_at, updated_at, write_status, write_error, last_written_at, external_ref, submission_status, aeat_response
         FROM table_rows WHERE ${whereSql} ORDER BY ${orderBy} LIMIT ${limitPh} OFFSET ${offsetPh}`,
        p.all,
      ) as Promise<TableRowRow[]>,
      this.dataSource.query(`SELECT count(*)::int AS total FROM table_rows WHERE ${whereSql}`, countParams) as Promise<
        { total: number }[]
      >,
    ]);

    return {
      rows: rows.map((r) => ({
        _id: r.id,
        ...r.data,
        _ingestedAt: r.created_at,
        _updatedAt: r.updated_at,
        _writeStatus: r.write_status,
        _writeError: r.write_error,
        _lastWrittenAt: r.last_written_at,
        _externalRef: r.external_ref,
        _submissionStatus: r.submission_status,
        _aeatResponse: r.aeat_response,
      })),
      total: countRows[0].total,
      page: params.page,
      pageSize: params.pageSize,
    };
  }

  async deleteRows(template: TableTemplate, params: DatasetDeleteParams): Promise<{ affected: number }> {
    const p = new ParamList();
    const where: string[] = [`table_key = ${p.push(template.key)}`];
    if (template.perConnection && params.connectionId) where.push(`connection_id = ${p.push(params.connectionId)}`);

    if (params.ids?.length) {
      const validIds = params.ids.filter(isUuid);
      if (!validIds.length) return { affected: 0 };
      where.push(`id = ANY(${p.push(validIds)}::uuid[])`);
      // TypeORM's raw query() returns [rows, rowCount] for DELETE/UPDATE (unlike
      // SELECT/INSERT, which return the rows array directly) — destructure, don't
      // treat the tuple itself as the rows array.
      const [, affected] = await this.dataSource.query(`DELETE FROM table_rows WHERE ${where.join(' AND ')}`, p.all);
      return { affected };
    }

    if (params.olderThanDays !== undefined) {
      const cutoff = new Date(Date.now() - params.olderThanDays * 24 * 3600 * 1000);
      where.push(`created_at < ${p.push(cutoff)}`);
      const [, affected] = await this.dataSource.query(`DELETE FROM table_rows WHERE ${where.join(' AND ')}`, p.all);
      return { affected };
    }

    return { affected: 0 };
  }

  /**
   * Save an edited row and, when the template declares `write`, queue it for
   * submission. The local save is unconditional and happens first — the edit
   * must persist even if queuing somehow fails, so the user doesn't lose
   * their correction (optimistic local update). The external push itself is
   * never inline here: even a single-row "event" submission can be slow
   * enough on the AEAT side to risk an HTTP timeout, so every write funnels
   * through the same queued/debounced path as a batch of one (see
   * `submitGroup`/`WriteSweepProcessor`) — there is no synchronous code path.
   */
  async updateAndWrite(
    template: TableTemplate,
    connectionId: string | undefined,
    rowId: string,
    data: Record<string, unknown>,
  ): Promise<DatasetUpdateResult> {
    if (!isUuid(rowId)) throw new BadRequestException(`Invalid row id "${rowId}"`);

    const p = new ParamList();
    const dataPh = p.push(JSON.stringify(data));
    const where: string[] = [`id = ${p.push(rowId)}`, `table_key = ${p.push(template.key)}`];
    if (template.perConnection && connectionId) where.push(`connection_id = ${p.push(connectionId)}`);

    // UPDATE returns [rows, rowCount] via TypeORM's raw query() — see deleteRows().
    const [rows]: [TableRowRow[], number] = await this.dataSource.query(
      `UPDATE table_rows SET data = ${dataPh}::jsonb WHERE ${where.join(' AND ')}
       RETURNING id, data, created_at, updated_at, write_status, write_error, last_written_at, external_ref, submission_status, aeat_response`,
      p.all,
    );
    const updated = rows[0];
    if (!updated) throw new NotFoundException(`Row "${rowId}" not found for table "${template.key}"`);

    const flatten = (): Record<string, unknown> => ({
      _id: updated.id,
      ...updated.data,
      _ingestedAt: updated.created_at,
      _updatedAt: updated.updated_at,
      _writeStatus: updated.write_status,
      _writeError: updated.write_error,
      _lastWrittenAt: updated.last_written_at,
      _externalRef: updated.external_ref,
      _submissionStatus: updated.submission_status,
      _aeatResponse: updated.aeat_response,
    });

    if (!template.write) return { row: flatten() };

    await this.markQueuedAndScheduleSweep(template, [{ id: updated.id, data: updated.data }], template.perConnection ? (connectionId ?? null) : null);
    // markQueuedAndScheduleSweep() updates the DB but not this in-memory row —
    // reflect the same fresh-attempt reset here so the response isn't stale.
    updated.submission_status = 'queued';
    updated.aeat_response = null;
    return { row: flatten(), external: { attempted: true, status: 'queued' } };
  }

  /**
   * Send one outbound batch (one HTTP call, whole array as the body) for rows
   * already queued for submission, and record the transport ack for all of
   * them in a single UPDATE. Never touches `data` and never resolves the real
   * AEAT result — that only ever arrives later via the inbound callback,
   * correlated by `external_ref`. `batch_id` here is purely for
   * traceability/stuck-batch detection, never for deciding a per-row outcome.
   *
   * A 2xx is only a provider ACK ("received"), so rows move to
   * `submission_status='pending'` (awaiting the real result), never straight
   * to a terminal state. A non-2xx or transport failure puts them back to
   * `'queued'` so the next sweep retries them.
   */
  async submitGroup(
    template: TableTemplate,
    rows: { id: string; data: Record<string, unknown> }[],
    opts?: { trigger?: 'event' | 'schedule' | 'manual'; groupValues?: Record<string, string> | null; connectionId?: string | null },
  ): Promise<{ batchId: string; status: 'sent' | 'error'; error?: string } | null> {
    if (!rows.length) return null;
    if (!template.write) {
      throw new BadRequestException(`Table "${template.key}" has no write config to submit rows through`);
    }

    const batchId = randomUUID();
    const ids = rows.map((r) => r.id);
    const trigger = opts?.trigger ?? template.write.trigger ?? 'event';
    const groupValues = opts?.groupValues ?? null;
    // perConnection tables submit each group through the connection its rows
    // were ingested under, not the template's single fixed write connection —
    // otherwise every company/tenant's queued rows would ship through the same
    // one regardless of which one they actually belong to.
    const connectionId = template.perConnection && opts?.connectionId ? opts.connectionId : template.write.connectionId;
    let connectionName: string | null = null;

    try {
      const conn = await this.connections.resolveById(connectionId);
      connectionName = conn.name;
      const { status } = await this.client.send(
        conn,
        { method: template.write.method, path: template.write.path, query: template.write.query },
        rows.map((r) => r.data),
      );

      if (status >= 200 && status < 300) {
        await this.markGroupResult(ids, batchId, 'sent', null, 'pending');
        await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: rows.length, status: 'sent', httpStatus: status, batchId });
        return { batchId, status: 'sent' };
      }

      const message = `External system responded ${status}`;
      await this.markGroupResult(ids, batchId, 'error', message, 'queued');
      await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: rows.length, status: 'error', httpStatus: status, errorMessage: message, batchId });
      return { batchId, status: 'error', error: message };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.markGroupResult(ids, batchId, 'error', message, 'queued');
      await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: rows.length, status: 'error', httpStatus: null, errorMessage: message, batchId });
      return { batchId, status: 'error', error: message };
    }
  }

  /**
   * Best-effort write-run history. Never let a logging failure break (or change
   * the return of) an actual submission — the run row is trace, not truth.
   */
  private async recordRun(args: {
    template: TableTemplate;
    connectionId: string | null;
    connectionName: string | null;
    trigger: 'event' | 'schedule' | 'manual';
    groupValues: Record<string, string> | null;
    rowCount: number;
    status: 'sent' | 'error';
    httpStatus: number | null;
    errorMessage?: string;
    batchId?: string;
  }): Promise<void> {
    try {
      await this.writeRuns.record({
        tableKey: args.template.key,
        connectionId: args.connectionId,
        connectionName: args.connectionName,
        trigger: args.trigger,
        status: args.status,
        batchId: args.batchId ?? null,
        groupValues: args.groupValues,
        rowCount: args.rowCount,
        httpStatus: args.httpStatus,
        errorMessage: args.errorMessage ?? null,
      });
    } catch {
      /* history is best-effort; a submission must never fail because of it */
    }
  }

  private async markGroupResult(
    ids: string[],
    batchId: string,
    writeStatus: 'sent' | 'error',
    writeError: string | null,
    submissionStatus: 'pending' | 'queued',
  ): Promise<void> {
    await this.dataSource.query(
      `UPDATE table_rows
         SET batch_id = $1, write_status = $2, write_error = $3, last_written_at = now(), submission_status = $4
         WHERE id = ANY($5::uuid[])`,
      [batchId, writeStatus, writeError, submissionStatus, ids],
    );
  }

  /**
   * Mark freshly ingested/edited rows as a fresh submission attempt
   * (`submission_status='queued'`, clearing any stale `batch_id`/`aeat_response`
   * from a previous attempt), then — only for `trigger==='event'` templates —
   * enqueue one debounced sweep job per distinct batch group present among
   * `affected`. No-op when the template has no `write` config.
   */
  private async markQueuedAndScheduleSweep(
    template: TableTemplate,
    affected: { id: string; data: Record<string, unknown> }[],
    connectionId: string | null,
  ): Promise<void> {
    if (!template.write || !affected.length) return;

    const ids = affected.map((r) => r.id);
    await this.dataSource.query(
      `UPDATE table_rows SET submission_status = 'queued', batch_id = NULL, aeat_response = NULL WHERE id = ANY($1::uuid[])`,
      [ids],
    );

    if ((template.write.trigger ?? 'event') !== 'event') return;

    const groupBy = template.write.batch?.groupBy ?? [];
    const seenGroups = new Set<string>();
    for (const row of affected) {
      const groupValues: Record<string, string> = {};
      for (const col of groupBy) groupValues[col] = String(row.data[col] ?? '');
      const dedupeKey = JSON.stringify(groupValues);
      if (seenGroups.has(dedupeKey)) continue;
      seenGroups.add(dedupeKey);
      await this.enqueueWriteSweep(template.key, groupValues, connectionId);
    }
  }

  /**
   * Debounce a burst of concurrent inserts/edits on the same batch group into
   * one sweep: a stable `jobId` per (tableKey, connectionId, groupValues) means
   * BullMQ silently drops every `add()` call after the first while a job with
   * that id is still waiting/delayed, so this relies entirely on the sweep
   * re-querying `queued` rows at execution time rather than trusting
   * `groupValues` as anything but a WHERE-clause key (see WriteSweepProcessor).
   * `connectionId` is folded into the jobId too so two perConnection tenants
   * sharing the same groupBy values never collapse into one debounce job.
   */
  private async enqueueWriteSweep(tableKey: string, groupValues: Record<string, string>, connectionId: string | null): Promise<void> {
    const hash = shortHash(`${tableKey}:${connectionId ?? ''}:${JSON.stringify(groupValues)}`);
    const debounceMs = Number(process.env.WRITE_SWEEP_DEBOUNCE_MS) || 5000;
    await this.writeSweep.add(
      'sweep',
      { tableKey, groupValues, connectionId },
      { ...DEFAULT_JOB_OPTS, jobId: `write-sweep:${tableKey}:${hash}`, delay: debounceMs },
    );
  }
}

function shortHash(input: string): string {
  return createHash('sha256').update(input).digest('hex').slice(0, 16);
}
