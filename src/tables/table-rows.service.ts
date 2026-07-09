import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { InjectQueue } from '@nestjs/bullmq';
import { DataSource } from 'typeorm';
import type { Queue } from 'bullmq';
import { randomUUID } from 'node:crypto';
import { TableTemplate } from './entities/table-template.entity';
import { DatasetDeleteParams, DatasetPage, DatasetQuery, DatasetUpdateResult } from '../datasets/dataset.types';
import { resolveClave, SourceConnectionsService } from '../connections/source-connections.service';
import { SourceHttpClient } from '../connections/source-http.client';
import { QUEUES, DEFAULT_JOB_OPTS } from '../core/queues/queues.constants';
import { WriteEventJobData } from './write-event.types';
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
  sii_response: Record<string, unknown> | null;
}

@Injectable()
export class TableRowsService {
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly connections: SourceConnectionsService,
    private readonly client: SourceHttpClient,
    @InjectQueue(QUEUES.WRITE_EVENT) private readonly writeEvent: Queue<WriteEventJobData>,
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
            [template.key, connectionId, JSON.stringify(data), traceId ?? null],
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
          [template.key, connectionId, JSON.stringify(data), traceId ?? null],
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
          params.push(template.key, connectionId, JSON.stringify(data), traceId ?? null);
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

    // Creation never sends — rows land `queued` and wait for the per-connection
    // internal cron (or an explicit force-submit). Only an *edit* (updateAndWrite)
    // triggers an immediate event send. Rows are created in batch, so a send here
    // would be a batch-on-create, which is exactly what we don't want.
    await this.markQueued(template, affected.map((r) => r.id));

    return { inserted, upserted };
  }

  /** Query rows for a template honoring only filterable/sortable columns. */
  async query(template: TableTemplate, params: DatasetQuery): Promise<DatasetPage> {
    const filterable = new Set(template.columns.filter((c) => c.filterable).map((c) => c.key));
    const sortable = new Set(template.columns.filter((c) => c.sortable).map((c) => c.key));

    const p = new ParamList();
    const where: string[] = [`table_key = ${p.push(template.key)}`];
    where.push(`connection_id = ${p.push(params.connectionId ?? '')}`);

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
        `SELECT id, data, created_at, updated_at, write_status, write_error, last_written_at, external_ref, submission_status, sii_response
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
        _siiResponse: r.sii_response,
      })),
      total: countRows[0].total,
      page: params.page,
      pageSize: params.pageSize,
    };
  }

  async deleteRows(template: TableTemplate, params: DatasetDeleteParams): Promise<{ affected: number }> {
    const p = new ParamList();
    const where: string[] = [`table_key = ${p.push(template.key)}`];
    if (params.connectionId) where.push(`connection_id = ${p.push(params.connectionId)}`);

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
   * enough on the SII side to risk an HTTP timeout, so every write funnels
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
    if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);

    // UPDATE returns [rows, rowCount] via TypeORM's raw query() — see deleteRows().
    const [rows]: [TableRowRow[], number] = await this.dataSource.query(
      `UPDATE table_rows SET data = ${dataPh}::jsonb WHERE ${where.join(' AND ')}
       RETURNING id, data, created_at, updated_at, write_status, write_error, last_written_at, external_ref, submission_status, sii_response`,
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
      _siiResponse: updated.sii_response,
    });

    if (!template.write) return { row: flatten() };

    const rowConnectionId = connectionId ?? null;
    await this.markQueued(template, [updated.id]);
    // Event mode: send exactly this edited row now (array of 1), via an immediate
    // targeted job — never inline (an SII round-trip can outlast the form-save
    // HTTP request). Schedule mode: leave it `queued` for the internal cron.
    if ((template.write.trigger ?? 'event') === 'event') {
      await this.enqueueEventSend(template.key, updated.id, rowConnectionId);
    }
    // markQueued() updates the DB but not this in-memory row — reflect the same
    // fresh-attempt reset here so the response isn't stale.
    updated.submission_status = 'queued';
    updated.sii_response = null;
    return { row: flatten(), external: { attempted: true, status: 'queued' } };
  }

  /**
   * Send one outbound batch (one HTTP call, body `{clientId, payload}` — payload
   * is ALWAYS an array, even for a single-row event send, and every row carries
   * `internal_ref` = its own row id) for rows already queued for submission,
   * and record the transport ack for all of them in a single UPDATE. Never
   * touches `data` and never resolves the real SII result — that only ever
   * arrives later via the inbound callback, correlated by the echoed-back
   * `internal_ref`. `batch_id` here is purely for traceability/stuck-batch
   * detection, never for deciding a per-row outcome.
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
    // Each group submits through the connection its rows were ingested under,
    // not the template's single fixed write connection — otherwise every
    // company/tenant's queued rows would ship through the same one regardless
    // of which one they actually belong to. `template.write.connectionId` is
    // only the fallback for when that can't be determined.
    const connectionId = opts?.connectionId ?? template.write.connectionId;
    let connectionName: string | null = null;

    // Payload is ALWAYS an array — a single-row event send still ships as an
    // array of 1, per the external-system contract. Keys are normalized to
    // camelCase for the outbound submission. `internal_ref` carries our own
    // row id so the external system can echo it back on its result callback —
    // that's what correlates the callback to a row, instead of relying on the
    // vendor's own externalRefPath-plucked id.
    const payload = rows.map((r) => ({
      internal_ref: r.id,
      ...(toCamelCase(r.data) as Record<string, unknown>),
    }));
    let payloadPreview: unknown;

    try {
      const conn = await this.connections.resolveById(connectionId);
      connectionName = conn.name;
      const clientId = resolveClave(conn);
      payloadPreview = { clientId, payload };
      const { status, data } = await this.client.send(
        conn,
        { method: template.write.method, path: template.write.path, query: template.write.query },
        payloadPreview,
      );

      if (status >= 200 && status < 300) {
        await this.markGroupResult(ids, batchId, 'sent', null, 'pending');
        await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: rows.length, status: 'sent', httpStatus: status, batchId, payloadPreview });
        return { batchId, status: 'sent' };
      }

      // Detailed error: include the external system's response body, not just the
      // bare status — "responded 400" alone is useless for diagnosing a rejection.
      const message = `External system responded ${status}: ${truncate(safeStringify(data))}`;
      await this.markGroupResult(ids, batchId, 'error', message, 'queued');
      await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: rows.length, status: 'error', httpStatus: status, errorMessage: message, batchId, payloadPreview, responseBody: data ?? null });
      return { batchId, status: 'error', error: message };
    } catch (err) {
      // Transport failure (network/DNS/timeout): send() only throws here, since
      // non-2xx responses are returned, not thrown (validateStatus always true).
      const responseBody = axiosResponseBody(err);
      const base = err instanceof Error ? err.message : String(err);
      const message = responseBody !== null ? `${base}: ${truncate(safeStringify(responseBody))}` : base;
      await this.markGroupResult(ids, batchId, 'error', message, 'queued');
      await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: rows.length, status: 'error', httpStatus: null, errorMessage: message, batchId, payloadPreview, responseBody });
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
    payloadPreview?: unknown;
    responseBody?: unknown;
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
        payloadPreview: args.payloadPreview ?? null,
        responseBody: args.responseBody ?? null,
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
   * Mark rows as a fresh submission attempt (`submission_status='queued'`,
   * clearing any stale `batch_id`/`sii_response` from a previous attempt).
   * Never sends anything itself — creation waits for the internal cron, edits
   * are pushed separately via `enqueueEventSend`. No-op when the template has
   * no `write` config or there are no ids.
   */
  private async markQueued(template: TableTemplate, ids: string[]): Promise<void> {
    if (!template.write || !ids.length) return;
    await this.dataSource.query(
      `UPDATE table_rows SET submission_status = 'queued', batch_id = NULL, sii_response = NULL WHERE id = ANY($1::uuid[])`,
      [ids],
    );
  }

  /**
   * Enqueue an immediate, row-targeted send of a single edited row (event mode).
   * `delay: 0` so it fires right away; a stable `jobId` per row dedupes rapid
   * re-edits while one is still in flight. The processor re-checks the row is
   * still `queued` before sending, so a stale/duplicate job simply no-ops.
   */
  private async enqueueEventSend(tableKey: string, rowId: string, connectionId: string | null): Promise<void> {
    await this.writeEvent.add(
      'event',
      { tableKey, rowId, connectionId },
      { ...DEFAULT_JOB_OPTS, jobId: `write-event-${rowId}`, delay: 0 },
    );
  }
}

/** Convert snake_case keys to camelCase recursively. */
function toCamelCase(obj: unknown): unknown {
  if (Array.isArray(obj)) {
    return obj.map(toCamelCase);
  }
  if (obj !== null && typeof obj === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj as Record<string, unknown>)) {
      const camelKey = key.replace(/_([a-z])/g, (_, char) => char.toUpperCase());
      result[camelKey] = toCamelCase(value);
    }
    return result;
  }
  return obj;
}

/** JSON.stringify that never throws (circular refs → fallback to String()). */
function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

/** Cap an error/response snippet so a huge body never bloats the run history. */
function truncate(s: string, max = 2000): string {
  return s.length > max ? `${s.slice(0, max)}… (${s.length} chars)` : s;
}

/** Pull `response.data` out of an axios-style error, or null if there's none. */
function axiosResponseBody(err: unknown): unknown {
  if (err && typeof err === 'object' && 'response' in err) {
    const response = (err as { response?: unknown }).response;
    if (response && typeof response === 'object' && 'data' in response) {
      return (response as { data?: unknown }).data ?? null;
    }
  }
  return null;
}
