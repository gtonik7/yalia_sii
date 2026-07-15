import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { InjectQueue } from '@nestjs/bullmq';
import { DataSource, EntityManager } from 'typeorm';
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

/** Loosely matches ISO 8601 date/datetime strings — guards the timestamptz cast in query()'s date-range filter. */
const ISO_DATETIME_RE = String.raw`^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$`;

/** Tope del COUNT(*) de paginación — ver comentario en query(). */
const COUNT_CAP = 20_000;

/** Tope duro de grupos devueltos por `aggregate()` — acota el coste de un group-by ad-hoc. */
const AGG_ROW_CAP = 5_000;

/** Máximo de dimensiones de agrupación combinables en un informe ad-hoc. */
const MAX_AGG_DIMENSIONS = 4;

/** Tope defensivo de ids comprobables en una llamada a findMissingIds() (el hub ya capa antes de llamar). */
const MAX_FIND_MISSING_IDS = 20_000;

/**
 * Derives the same coarse tri-state the records grid paints for `_writeStatus`
 * ('queued'/'sent'/'error') from the physical write_status/write_error/
 * submission_status columns, so filter and sort agree with what's displayed.
 */
const WRITE_STATUS_CASE_SQL = `CASE
      WHEN (write_error IS NOT NULL AND write_error <> '') OR write_status = 'error' THEN 'error'
      WHEN write_status = 'sent' OR submission_status = 'pending' THEN 'sent'
      WHEN submission_status = 'queued' THEN 'queued'
      ELSE NULL
    END`;

/** Reserved (non-template) sort keys, mapped to the physical column/expression they sort by. */
const RESERVED_SORT_EXPR: Record<string, string> = {
    _updatedAt: 'updated_at',
    _submissionStatus: 'submission_status',
    _writeStatus: WRITE_STATUS_CASE_SQL,
};

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

/** One table's KPI breakdown — see `TableRowsService.getWriteSummary()`. */
export interface TableWriteSummaryEntry {
    tableKey: string;
    tableLabel: string;
    /** Row counts keyed by the derived tri-state (see WRITE_STATUS_CASE_SQL); 'none' = neither queued, sent nor error. */
    byWriteStatus: Record<string, number>;
    /** Row counts keyed by the raw SII submission_status value; 'none' = never queued. */
    bySubmissionStatus: Record<string, number>;
    /** Top write_error signatures for this table, most frequent first (capped). */
    errors: { error: string; count: number }[];
}

/** One grouping dimension of an ad-hoc report; `granularity` only applies to date columns / `_updatedAt`. */
export interface TableAggregateGroupBy {
    column: string;
    granularity?: 'day' | 'month' | 'year';
}

/** One optional numeric aggregation of an ad-hoc report (COUNT is always implicit). */
export interface TableAggregateMetric {
    column: string;
    fn: 'sum' | 'avg' | 'min' | 'max';
}

export interface TableAggregateParams {
    connectionId?: string;
    filters?: Record<string, string>;
    groupBy: TableAggregateGroupBy[];
    metrics?: TableAggregateMetric[];
}

/**
 * Shape of an ad-hoc report (`aggregate()`): self-describing so the FE can paint
 * a generic table without knowing the columns in advance. Each result row keys
 * its dimension values under `dims` (dim_0…dim_N) and its numeric aggregates
 * under `metrics` (metric_0…metric_N), matching `columns[].key`.
 */
export interface TableAggregateResult {
    columns: { key: string; label: string; kind: 'dimension' | 'count' | 'metric' }[];
    rows: { dims: Record<string, string | null>; count: number; metrics: Record<string, number | null> }[];
    /** True when the number of groups hit AGG_ROW_CAP and the tail was dropped. */
    truncated: boolean;
}

/** Reserved (non-template) grouping dimensions and their physical expression/label. */
const RESERVED_AGG_DIMS: Record<string, { expr: string; label: string; isDate: boolean }> = {
    _writeStatus: { expr: WRITE_STATUS_CASE_SQL, label: 'Estado envío', isDate: false },
    _submissionStatus: { expr: 'submission_status', label: 'Estado SII', isDate: false },
    _updatedAt: { expr: 'updated_at', label: 'Actualizado', isDate: true },
};

@Injectable()
export class TableRowsService {
    constructor(
        @InjectDataSource() private readonly dataSource: DataSource,
        private readonly connections: SourceConnectionsService,
        private readonly client: SourceHttpClient,
        @InjectQueue(QUEUES.WRITE_EVENT) private readonly writeEvent: Queue<WriteEventJobData>,
        private readonly writeRuns: TableWriteRunService
    ) {}

    /**
     * Store one or more rows for a template. Upserts by `idField` (scoped to
     * connectionId) when the template declares one; otherwise appends.
     */
    async ingest(template: TableTemplate, rows: Record<string, unknown>[], connectionId: string, traceId?: string): Promise<{ inserted: number; upserted: number; skippedStale: number }> {
        if (!rows.length) return { inserted: 0, upserted: 0, skippedStale: 0 };
        let inserted = 0;
        let upserted = 0;
        // Rows dropped by the "newest wins" upsert guard because the incoming copy is
        // older than the one already stored (only possible when recencyField is set).
        let skippedStale = 0;
        const affectedIds: string[] = [];

        // The inserts (across every chunk) and the trailing markQueued() run inside
        // one transaction: without it, a crash/exception between the last INSERT and
        // markQueued() left rows permanently committed with submission_status NULL —
        // invisible to both the cron sweep and manual force-submit (neither treats
        // NULL as sendable), with nothing to ever revisit and repair them.
        await this.dataSource.transaction(async (manager) => {
            if (template.idField) {
                // idField/table.key are already validated by the DTO when the template
                // was saved (and by TableIndexManagerService when the unique index was
                // built); re-checked here as defense in depth since they're interpolated
                // as literal SQL text below — Postgres requires the ON CONFLICT partial
                // index predicate to match the index's predicate verbatim, not bound.
                assertColumnKey(template.idField);
                assertTableKey(template.key);
                if (template.recencyField) assertColumnKey(template.recencyField);
                const idField = template.idField;
                const idExpr = `(data ->> ${sqlStringLiteral(idField)})`;
                const tableKeyLit = sqlStringLiteral(template.key);
                const recencyField = template.recencyField;
                // Numeric recency of a row: missing/non-numeric = -1, so an unstamped row
                // never wins over one that does carry a real recency value.
                const recencyOf = (data: Record<string, unknown>): number => {
                    if (!recencyField) return 0;
                    const n = Number(data[recencyField]);
                    return Number.isFinite(n) ? n : -1;
                };

                // Split rows: those with a usable id go through the batched ON CONFLICT
                // upsert (deduped so a single statement never hits the same conflict
                // target twice — Postgres rejects that); those without an id fall back
                // to a plain append so the row is never lost. Without recencyField the
                // dedup keeps the last occurrence (historical behavior); with it, the
                // occurrence with the greatest recency value wins regardless of order.
                const withId = new Map<string, Record<string, unknown>>();
                const withoutId: Record<string, unknown>[] = [];
                for (const data of rows) {
                    const idValue = data[idField];
                    if (idValue === undefined || idValue === null || idValue === '') {
                        withoutId.push(data);
                        continue;
                    }
                    const key = String(idValue);
                    const prev = withId.get(key);
                    if (!prev || recencyOf(data) >= recencyOf(prev)) withId.set(key, data);
                }

                for (const chunk of chunk1000([...withId.values()])) {
                    const values: string[] = [];
                    const params: unknown[] = [];
                    chunk.forEach((data, idx) => {
                        const b = idx * 4;
                        values.push(`($${b + 1}, $${b + 2}, $${b + 3}::jsonb, $${b + 4})`);
                        params.push(template.key, connectionId, JSON.stringify(data), traceId ?? null);
                    });
                    // `(xmax = 0)` distinguishes a freshly inserted row from one the
                    // ON CONFLICT updated, so the inserted/upserted split stays accurate.
                    // The WHERE guard on DO UPDATE only overwrites when the incoming row's
                    // recency is >= the stored one; a row that loses the guard is simply
                    // absent from RETURNING (Postgres treats a no-op conflict as neither
                    // inserted nor updated) — that gap is what skippedStale counts.
                    const recencyGuard = recencyField
                        ? ` WHERE COALESCE((table_rows.data ->> ${sqlStringLiteral(recencyField)})::numeric, -1)
                 <= COALESCE((EXCLUDED.data ->> ${sqlStringLiteral(recencyField)})::numeric, -1)`
                        : '';
                    const returned: { id: string; inserted: boolean }[] = await manager.query(
                        `INSERT INTO table_rows (table_key, connection_id, data, trace_id)
             VALUES ${values.join(',')}
             ON CONFLICT (connection_id, ${idExpr}) WHERE table_key = ${tableKeyLit}
             DO UPDATE SET data = EXCLUDED.data, trace_id = EXCLUDED.trace_id${recencyGuard}
             RETURNING id, (xmax = 0) AS inserted`,
                        params
                    );
                    for (const r of returned) {
                        affectedIds.push(r.id);
                        if (r.inserted) inserted++;
                        else upserted++;
                    }
                    skippedStale += chunk.length - returned.length;
                }

                for (const chunk of chunk1000(withoutId)) {
                    affectedIds.push(...(await this.insertAppend(manager, template.key, connectionId, chunk, traceId)));
                }
                inserted += withoutId.length;
            } else {
                for (const chunk of chunk1000(rows)) {
                    affectedIds.push(...(await this.insertAppend(manager, template.key, connectionId, chunk, traceId)));
                }
                inserted += rows.length;
            }

            // Creation never sends — rows land `queued` and wait for the per-connection
            // internal cron (or an explicit force-submit). Only an *edit* (updateAndWrite)
            // triggers an immediate event send. Rows are created in batch, so a send here
            // would be a batch-on-create, which is exactly what we don't want.
            await this.markQueued(manager, template, affectedIds);
        });

        return { inserted, upserted, skippedStale };
    }

    /** Plain multi-VALUES append (no id to upsert on); returns the new row ids. */
    private async insertAppend(manager: EntityManager, tableKey: string, connectionId: string, rows: Record<string, unknown>[], traceId?: string): Promise<string[]> {
        if (!rows.length) return [];
        const values: string[] = [];
        const params: unknown[] = [];
        rows.forEach((data, idx) => {
            const b = idx * 4;
            values.push(`($${b + 1}, $${b + 2}, $${b + 3}::jsonb, $${b + 4})`);
            params.push(tableKey, connectionId, JSON.stringify(data), traceId ?? null);
        });
        const returned: { id: string }[] = await manager.query(`INSERT INTO table_rows (table_key, connection_id, data, trace_id) VALUES ${values.join(',')} RETURNING id`, params);
        return returned.map((r) => r.id);
    }

    /**
     * Reconciliation stats for one template (optionally scoped to a connection):
     * stored row count, distinct-id count (when idField is set — the gap between
     * the two is expected upsert dedup collapse), rows missing a recency stamp
     * (when recencyField is set — these predate the "newest wins" guard and are
     * still first-write-wins until re-ingested), and a delete counter.
     */
    async getStats(
        template: TableTemplate,
        connectionId?: string
    ): Promise<{ rowCount: number; distinctIds: number | null; deletedSinceLoad: number; voluntaryDeletes: number; uncontrolledDeletes: number; missingRecency: number | null }> {
        assertTableKey(template.key);
        const p = new ParamList();
        const where: string[] = [`table_key = ${p.push(template.key)}`];
        if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
        const whereSql = where.join(' AND ');

        const [countRow]: { n: number }[] = await this.dataSource.query(`SELECT count(*)::int AS n FROM table_rows WHERE ${whereSql}`, p.all);

        let distinctIds: number | null = null;
        if (template.idField) {
            assertColumnKey(template.idField);
            const idExpr = `(data ->> ${sqlStringLiteral(template.idField)})`;
            const [row]: { n: number }[] = await this.dataSource.query(`SELECT count(DISTINCT ${idExpr})::int AS n FROM table_rows WHERE ${whereSql}`, p.all);
            distinctIds = row.n;
        }

        let missingRecency: number | null = null;
        if (template.recencyField) {
            assertColumnKey(template.recencyField);
            const recExpr = `(data ->> ${sqlStringLiteral(template.recencyField)})`;
            const [row]: { n: number }[] = await this.dataSource.query(`SELECT count(*)::int AS n FROM table_rows WHERE ${whereSql} AND (${recExpr} IS NULL OR ${recExpr} = '')`, p.all);
            missingRecency = row.n;
        }

        // pg_stat_user_tables is per physical table, not per table_key partition —
        // table_rows is shared by every template, so this counts deletes across
        // ALL templates ever stored there, not just this one. A coarse global
        // signal ("have rows been deleted at all recently"), not an exact figure
        // scoped to this template — the caller's report should present it as such.
        const statRows: { n: number }[] = await this.dataSource.query(`SELECT n_tup_del::int AS n FROM pg_stat_user_tables WHERE relname = 'table_rows'`);
        const deletedSinceLoad = statRows[0]?.n ?? 0;

        // Voluntary deletions the app recorded (bulk/ids/retention + the baseline
        // snapshot of pre-feature deletes) — global, like n_tup_del, so the two are
        // apples-to-apples. `uncontrolled = deletedSinceLoad - voluntary`, floored at
        // 0: a pg_stat_reset leaves n_tup_del below the ledger, which the max(0,…)
        // turns into "no uncontrolled loss" (the safe direction) rather than a
        // negative or a false alarm. Coarse by design — same "aprox." caveat as
        // deletedSinceLoad itself.
        const [ledgerRow]: { n: number }[] = await this.dataSource.query(`SELECT COALESCE(SUM(affected), 0)::int AS n FROM table_delete_events`);
        const voluntaryDeletes = ledgerRow?.n ?? 0;
        const uncontrolledDeletes = Math.max(0, deletedSinceLoad - voluntaryDeletes);

        return { rowCount: countRow.n, distinctIds, deletedSinceLoad, voluntaryDeletes, uncontrolledDeletes, missingRecency };
    }

    /**
     * Given business-key ids the caller (the hub) knows it sent, reports which
     * are NOT currently present in table_rows for this template+connection —
     * the reconciliation counterpart to getStats()'s coarse global counter,
     * scoped and precise instead of approximate. Index-backed by the dynamic
     * `ux_tr_<hash>` unique index TableIndexManagerService builds on
     * `(connection_id, data->>idField) WHERE table_key=...` whenever idField is set.
     */
    async findMissingIds(
        template: TableTemplate,
        connectionId: string,
        ids: string[]
    ): Promise<{ missingIds: string[]; checkedCount: number; deletedInfo: Record<string, { reason: string; at: string }> }> {
        assertTableKey(template.key);
        assertColumnKey(template.idField);
        const uniqueIds = [...new Set(ids)].slice(0, MAX_FIND_MISSING_IDS);
        if (uniqueIds.length === 0) return { missingIds: [], checkedCount: 0, deletedInfo: {} };

        const idExpr = `(data ->> ${sqlStringLiteral(template.idField)})`;
        const p = new ParamList();
        const tableKeyPh = p.push(template.key);
        const connectionIdPh = p.push(connectionId);
        const idsPh = p.push(uniqueIds);
        const rows: { id: string }[] = await this.dataSource.query(
            `SELECT ${idExpr} AS id FROM table_rows WHERE table_key = ${tableKeyPh} AND connection_id = ${connectionIdPh} AND ${idExpr} = ANY(${idsPh}::text[])`,
            p.all
        );
        const present = new Set(rows.map((r) => r.id));
        const missingIds = uniqueIds.filter((id) => !present.has(id));
        const deletedInfo = missingIds.length ? await this.findDeletedInfo(template.key, connectionId, missingIds) : {};
        return { missingIds, checkedCount: uniqueIds.length, deletedInfo };
    }

    /**
     * For a set of ids already known to be missing, checks whether the app's own
     * `table_delete_events` ledger recorded removing them — the precise answer to
     * "why is this gone", vs. the coarse global `n_tup_del` counter in getStats().
     * The `&&` overlap check hits the GIN index on `record_ids` first, so only the
     * handful of matching delete events (not the whole ledger) get unnested.
     * Multiple matches per id keep the most recent event.
     */
    private async findDeletedInfo(tableKey: string, connectionId: string, missingIds: string[]): Promise<Record<string, { reason: string; at: string }>> {
        const p = new ParamList();
        const tableKeyPh = p.push(tableKey);
        const connectionIdPh = p.push(connectionId);
        const idsPh = p.push(missingIds);
        const rows: { record_id: string; reason: string; created_at: string }[] = await this.dataSource.query(
            `SELECT DISTINCT ON (id_val) id_val AS record_id, reason, created_at
       FROM (
         SELECT unnest(record_ids) AS id_val, reason, created_at
         FROM table_delete_events
         WHERE table_key = ${tableKeyPh} AND (connection_id = ${connectionIdPh} OR connection_id IS NULL)
           AND record_ids && ${idsPh}::text[]
       ) t
       ORDER BY id_val, created_at DESC`,
            p.all
        );
        const out: Record<string, { reason: string; at: string }> = {};
        for (const row of rows) out[row.record_id] = { reason: row.reason, at: new Date(row.created_at).toISOString() };
        return out;
    }

    /**
     * Satellite-wide KPI breakdown across the given write-configured templates:
     * row counts by derived write status and raw SII submission status, plus
     * the top write-error signatures per table — backs the "Resumen SII"
     * dashboard in the Tablas tab. One pair of grouped queries across every
     * requested table instead of a per-template round trip.
     */
    async getWriteSummary(templates: TableTemplate[], connectionId?: string): Promise<TableWriteSummaryEntry[]> {
        if (!templates.length) return [];

        const p = new ParamList();
        const keysPh = p.push(templates.map((t) => t.key));
        const where: string[] = [`table_key = ANY(${keysPh}::varchar[])`];
        if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
        const whereSql = where.join(' AND ');

        const [statusRows, errorRows] = await Promise.all([
            this.dataSource.query(
                `SELECT table_key, ${WRITE_STATUS_CASE_SQL} AS write_status, submission_status, count(*)::int AS count
         FROM table_rows WHERE ${whereSql}
         GROUP BY table_key, (${WRITE_STATUS_CASE_SQL}), submission_status`,
                p.all
            ) as Promise<{ table_key: string; write_status: string | null; submission_status: string | null; count: number }[]>,
            // Ordered by count DESC globally — filtering this list down to one
            // table_key preserves that same descending order for that table, so
            // capping at 10 while iterating below yields each table's own top 10.
            this.dataSource.query(
                `SELECT table_key, write_error AS error, count(*)::int AS count
         FROM table_rows WHERE ${whereSql} AND write_error IS NOT NULL AND write_error <> ''
         GROUP BY table_key, write_error
         ORDER BY count DESC`,
                p.all
            ) as Promise<{ table_key: string; error: string; count: number }[]>,
        ]);

        const byKey = new Map<string, TableWriteSummaryEntry>();
        for (const t of templates) {
            byKey.set(t.key, { tableKey: t.key, tableLabel: t.label, byWriteStatus: {}, bySubmissionStatus: {}, errors: [] });
        }
        for (const r of statusRows) {
            const entry = byKey.get(r.table_key);
            if (!entry) continue;
            const ws = r.write_status ?? 'none';
            entry.byWriteStatus[ws] = (entry.byWriteStatus[ws] ?? 0) + r.count;
            const ss = r.submission_status ?? 'none';
            entry.bySubmissionStatus[ss] = (entry.bySubmissionStatus[ss] ?? 0) + r.count;
        }
        for (const r of errorRows) {
            const entry = byKey.get(r.table_key);
            if (entry && entry.errors.length < 10) entry.errors.push({ error: r.error, count: r.count });
        }

        return templates.map((t) => byKey.get(t.key)!);
    }

    /**
     * Shared WHERE-builder for template-declared filters, used by both `query()`
     * (listing) and `deleteRows()` (mass delete by filter) so there's exactly
     * one filter DSL in this satellite instead of two.
     */
    private applyFilters(template: TableTemplate, filters: Record<string, string> | undefined, where: string[], p: ParamList): void {
        const filterable = new Set(template.columns.filter((c) => c.filterable).map((c) => c.key));
        const dateRangeBounds = new Map<string, { from?: string; until?: string }>();

        // Per-column filters: substring match for strings, exact otherwise. Date
        // columns arrive as `<key>_from`/`<key>_until` (see table-dataset.bridge)
        // and are combined into a single range on `data->>'<key>'`.
        if (filters) {
            for (const [k, v] of Object.entries(filters)) {
                if (v === '') continue;

                // Reserved (non-template) filters: write-back status/timestamp columns
                // are physical table_rows columns, not part of template.columns, so
                // they're intercepted here rather than going through the generic
                // filterable/type-based branches below (see table-dataset.bridge for
                // the filter defs the FE renders for these keys).
                if (k === '_writeStatus') {
                    where.push(`${WRITE_STATUS_CASE_SQL} = ${p.push(v)}`);
                    continue;
                }
                if (k === '_submissionStatus') {
                    where.push(`submission_status ILIKE ${p.push(`%${escapeLike(v)}%`)}`);
                    continue;
                }
                if (k === '_updatedAt_from') {
                    where.push(`updated_at >= ${p.push(v)}::timestamptz`);
                    continue;
                }
                if (k === '_updatedAt_until') {
                    where.push(`updated_at <= ${p.push(v)}::timestamptz`);
                    continue;
                }

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
            const dateExpr = this.dateCastExpr(baseKey, p);
            if (bounds.from !== undefined) where.push(`${dateExpr} >= ${p.push(bounds.from)}::timestamptz`);
            if (bounds.until !== undefined) where.push(`${dateExpr} <= ${p.push(bounds.until)}::timestamptz`);
        }
    }

    /**
     * Safe timestamptz cast of a JSON text field. Ingested rows keep whatever the
     * source sent, so `data->>key` may be an ISO string (bare date "2026-01-05" or
     * full timestamp), epoch milliseconds (numeric text like "1709420400000"), or
     * plain garbage (violates the declared type). The CASE short-circuits so a
     * non-conforming row yields NULL and is excluded rather than throwing and
     * failing the whole query. Shared by the date-range filter (applyFilters) and
     * the date grouping dimensions of `aggregate()`.
     */
    private dateCastExpr(key: string, p: ParamList): string {
        const keyPh = p.push(key);
        const rePh = p.push(ISO_DATETIME_RE);
        return `CASE
        WHEN (data ->> ${keyPh}) ~ ${rePh} THEN (data ->> ${keyPh})::timestamptz
        WHEN (data ->> ${keyPh}) ~ '^\\d+$' THEN to_timestamp((data ->> ${keyPh})::bigint / 1000.0)
        ELSE NULL
      END`;
    }

    /** Safe numeric cast of a JSON text field for metric aggregation — non-numeric text yields NULL (excluded), never throws. */
    private numericCastExpr(key: string, p: ParamList): string {
        const keyPh = p.push(key);
        return `CASE WHEN (data ->> ${keyPh}) ~ '^-?\\d+(\\.\\d+)?$' THEN (data ->> ${keyPh})::numeric ELSE NULL END`;
    }

    /** Truncate a timestamptz expression to a stable text bucket (granularity is a fixed enum, safe to inline). */
    private dateTruncExpr(dateExpr: string, granularity: 'day' | 'month' | 'year'): string {
        const fmt = granularity === 'year' ? 'YYYY' : granularity === 'month' ? 'YYYY-MM' : 'YYYY-MM-DD';
        return `to_char(date_trunc('${granularity}', ${dateExpr}), '${fmt}')`;
    }

    /**
     * Ad-hoc group-by report over one template's rows: same WHERE engine as
     * `query()`/`countFiltered()` (so "las condiciones agrupadas" match Registros
     * exactly), grouped by 1..MAX_AGG_DIMENSIONS chosen dimensions with an optional
     * date granularity, returning COUNT(*) plus any requested numeric metrics.
     * Read-only; no free-text search (retired — see query()).
     */
    async aggregate(template: TableTemplate, params: TableAggregateParams): Promise<TableAggregateResult> {
        assertTableKey(template.key);
        const groupBy = params.groupBy ?? [];
        if (!groupBy.length) throw new BadRequestException('Se requiere al menos una dimensión de agrupación');
        if (groupBy.length > MAX_AGG_DIMENSIONS) throw new BadRequestException(`Máximo ${MAX_AGG_DIMENSIONS} dimensiones de agrupación`);

        const p = new ParamList();
        const where: string[] = [`table_key = ${p.push(template.key)}`];
        if (params.connectionId) where.push(`connection_id = ${p.push(params.connectionId)}`);
        this.applyFilters(template, params.filters, where, p);

        const columns: TableAggregateResult['columns'] = [];
        // Each dimension expression is built (and its params pushed) once, then
        // reused verbatim in both the SELECT and the GROUP BY — grouping by the full
        // expression, never by the alias (Postgres rejects aliasing a CASE in GROUP
        // BY; the exact bug fixed in getWriteSummary).
        const selectParts: string[] = [];
        const groupExprs: string[] = [];

        groupBy.forEach((g, i) => {
            if (g.granularity && !['day', 'month', 'year'].includes(g.granularity)) {
                throw new BadRequestException(`Granularidad no soportada: ${g.granularity}`);
            }
            const dimKey = `dim_${i}`;
            let expr: string;
            let label: string;

            if (g.column.startsWith('_')) {
                const reserved = RESERVED_AGG_DIMS[g.column];
                if (!reserved) throw new BadRequestException(`Dimensión reservada no permitida: ${g.column}`);
                label = reserved.label;
                expr = reserved.isDate && g.granularity ? this.dateTruncExpr(reserved.expr, g.granularity) : reserved.expr;
            } else {
                assertColumnKey(g.column);
                const col = template.columns.find((c) => c.key === g.column);
                if (!col) throw new BadRequestException(`Columna de agrupación desconocida: ${g.column}`);
                label = col.label;
                expr = col.type === 'date' && g.granularity ? this.dateTruncExpr(this.dateCastExpr(g.column, p), g.granularity) : `(data ->> ${p.push(g.column)})`;
            }
            selectParts.push(`${expr} AS ${dimKey}`);
            groupExprs.push(expr);
            columns.push({ key: dimKey, label, kind: 'dimension' });
        });

        selectParts.push('count(*)::int AS count');
        columns.push({ key: 'count', label: 'Registros', kind: 'count' });

        const metrics = params.metrics ?? [];
        metrics.forEach((m, i) => {
            if (!['sum', 'avg', 'min', 'max'].includes(m.fn)) throw new BadRequestException(`Función de métrica no soportada: ${m.fn}`);
            assertColumnKey(m.column);
            const col = template.columns.find((c) => c.key === m.column);
            if (!col) throw new BadRequestException(`Columna de métrica desconocida: ${m.column}`);
            if (col.type !== 'number') throw new BadRequestException(`La métrica '${m.fn}' requiere una columna numérica: ${m.column}`);
            const metricKey = `metric_${i}`;
            // ::float8 so every aggregate (numeric sum/avg/min/max) comes back as a plain JS number.
            selectParts.push(`${m.fn}(${this.numericCastExpr(m.column, p)})::float8 AS ${metricKey}`);
            columns.push({ key: metricKey, label: `${col.label} (${m.fn})`, kind: 'metric' });
        });

        // Fetch one past the cap so we can tell "exactly at cap" from "truncated".
        const limitPh = p.push(AGG_ROW_CAP + 1);
        const sql = `SELECT ${selectParts.join(', ')}
        FROM table_rows
        WHERE ${where.join(' AND ')}
        GROUP BY ${groupExprs.join(', ')}
        ORDER BY count DESC
        LIMIT ${limitPh}`;

        const raw: Record<string, unknown>[] = await this.dataSource.query(sql, p.all);
        const truncated = raw.length > AGG_ROW_CAP;
        const rows = (truncated ? raw.slice(0, AGG_ROW_CAP) : raw).map((r) => {
            const dims: Record<string, string | null> = {};
            groupBy.forEach((_g, i) => {
                const v = r[`dim_${i}`];
                dims[`dim_${i}`] = v == null ? null : String(v);
            });
            const metricsOut: Record<string, number | null> = {};
            metrics.forEach((_m, i) => {
                const v = r[`metric_${i}`];
                metricsOut[`metric_${i}`] = v == null ? null : Number(v);
            });
            return { dims, count: Number(r.count), metrics: metricsOut };
        });

        return { columns, rows, truncated };
    }

    /** Query rows for a template honoring only filterable/sortable columns. */
    async query(template: TableTemplate, params: DatasetQuery): Promise<DatasetPage> {
        const sortable = new Set(template.columns.filter((c) => c.sortable).map((c) => c.key));

        const p = new ParamList();
        const where: string[] = [`table_key = ${p.push(template.key)}`];
        where.push(`connection_id = ${p.push(params.connectionId ?? '')}`);

        this.applyFilters(template, params.filters, where, p);

        // Free-text search retirada: la columna STORED `search_vector` (+ GIN)
        // ~duplicaba el tamaño de la tabla y para datos fiscales la búsqueda útil
        // es por columna (los filtros de arriba). `params.search` se ignora — ver
        // migración DropTableRowsSearchVector.

        // Snapshot here — the count query's SQL only ever references the WHERE
        // params above; any params pushed later (sort/limit/offset) must never
        // leak into its bind list, or Postgres rejects the mismatched param count.
        const whereSql = where.join(' AND ');
        const countParams = [...p.all];
        // Tope del conteo: `table_rows` es compartida por todas las tablas de
        // usuario y algunas conexiones acumulan >1M filas (p.ej. facturas
        // emitidas) — un COUNT(*) exacto ahí escanea todo el índice coincidente
        // y en frío (caché de Postgres vacía) puede superar el timeout del proxy
        // del hub. Al envolver en un LIMIT, el planner para en cuanto encuentra
        // COUNT_CAP filas, acotando el coste sin importar el tamaño real ni los
        // filtros aplicados. `totalIsApproximate` avisa al FE de que es un
        // mínimo, no el total exacto.
        const countCapPh = `$${countParams.length + 1}`;

        // Sort: requested sortable column, else the template default, else newest first.
        let orderBy = 'created_at DESC';
        if (params.sort && RESERVED_SORT_EXPR[params.sort.key]) {
            orderBy = `${RESERVED_SORT_EXPR[params.sort.key]} ${params.sort.dir === 'asc' ? 'ASC' : 'DESC'}`;
        } else if (params.sort && sortable.has(params.sort.key)) {
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
                p.all
            ) as Promise<TableRowRow[]>,
            this.dataSource.query(`SELECT count(*)::int AS total FROM (SELECT 1 FROM table_rows WHERE ${whereSql} LIMIT ${countCapPh}) t`, [...countParams, COUNT_CAP]) as Promise<{ total: number }[]>,
        ]);

        const total = countRows[0].total;

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
            total,
            page: params.page,
            pageSize: params.pageSize,
            totalIsApproximate: total >= COUNT_CAP,
        };
    }

    /**
     * Exact, uncapped row count under the given filters — unlike `query()`'s
     * paginated total (capped at COUNT_CAP to bound worst-case scan cost, see
     * comment above), this always scans to completion. Meant to be called on
     * demand by callers that explicitly want the true number regardless of
     * table size (before a mass delete, or a "contar exacto" click) rather
     * than on every list load.
     */
    async countFiltered(template: TableTemplate, filters: Record<string, string> | undefined, connectionId?: string): Promise<number> {
        const p = new ParamList();
        const where: string[] = [`table_key = ${p.push(template.key)}`];
        if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
        this.applyFilters(template, filters, where, p);
        const [{ n }]: { n: number }[] = await this.dataSource.query(`SELECT count(*)::int AS n FROM table_rows WHERE ${where.join(' AND ')}`, p.all);
        return n;
    }

    async deleteRows(template: TableTemplate, params: DatasetDeleteParams): Promise<{ affected: number }> {
        const p = new ParamList();
        const where: string[] = [`table_key = ${p.push(template.key)}`];
        if (params.connectionId) where.push(`connection_id = ${p.push(params.connectionId)}`);
        // Captured alongside the affected count so the ledger can say WHICH ids
        // were removed, not just how many — see recordDeleteEvent.
        const returning = template.idField ? ` RETURNING (data ->> ${sqlStringLiteral(template.idField)}) AS record_id` : '';

        if (params.ids?.length) {
            const validIds = params.ids.filter(isUuid);
            if (!validIds.length) return { affected: 0 };
            where.push(`id = ANY(${p.push(validIds)}::uuid[])`);
            const result = await this.dataSource.query(`DELETE FROM table_rows WHERE ${where.join(' AND ')}${returning}`, p.all);
            const [affected, recordIds] = this.deleteResult(result, returning);
            await this.recordDeleteEvent(template.key, params.connectionId, affected, 'ids', recordIds);
            return { affected };
        }

        if (params.olderThanDays !== undefined) {
            const cutoff = new Date(Date.now() - params.olderThanDays * 24 * 3600 * 1000);
            where.push(`created_at < ${p.push(cutoff)}`);
            const result = await this.dataSource.query(`DELETE FROM table_rows WHERE ${where.join(' AND ')}${returning}`, p.all);
            const [affected, recordIds] = this.deleteResult(result, returning);
            await this.recordDeleteEvent(template.key, params.connectionId, affected, 'retention', recordIds);
            return { affected };
        }

        const nonEmptyFilters = Object.fromEntries(Object.entries(params.filters ?? {}).filter(([, v]) => v !== ''));
        if (Object.keys(nonEmptyFilters).length) {
            this.applyFilters(template, nonEmptyFilters, where, p);
            const result = await this.dataSource.query(`DELETE FROM table_rows WHERE ${where.join(' AND ')}${returning}`, p.all);
            const [affected, recordIds] = this.deleteResult(result, returning);
            await this.recordDeleteEvent(template.key, params.connectionId, affected, 'bulk', recordIds);
            return { affected };
        }

        // Never allow an unconditional delete through this method — every caller
        // must narrow by id, age or at least one non-empty filter.
        throw new BadRequestException('deleteRows requiere ids, olderThanDays o al menos un filtro no vacío');
    }

    /**
     * TypeORM's raw query() always returns `[rows, rowCount]` for a DELETE —
     * RETURNING does NOT change that shape, it only makes `rows` non-empty (see
     * PostgresQueryRunner.query(): `result.raw = [raw.rows, raw.rowCount]` for
     * every DELETE/UPDATE, structured result or not). `affected` must come from
     * `rowCount`, not `rows.length` — with RETURNING but no idField, `rows` is
     * still `[]` while real rows were deleted.
     */
    private deleteResult(result: unknown, returning: string): [affected: number, recordIds: string[] | null] {
        const [rows, affected] = result as [{ record_id: string | null }[], number];
        if (!returning) return [affected, null];
        return [affected, rows.map((r) => r.record_id).filter((id): id is string => id != null)];
    }

    /**
     * Record an app-initiated deletion in the `table_delete_events` ledger so
     * reconciliation can treat it as voluntary (subtracted from the physical
     * table's global delete counter) and, when `recordIds` is available,
     * attribute individual missing-record gaps to this exact event (see
     * findMissingIds). Best-effort and no-op for 0 rows — a bookkeeping failure
     * must never mask a delete that already committed.
     */
    private async recordDeleteEvent(tableKey: string, connectionId: string | undefined, affected: number, reason: 'bulk' | 'ids' | 'retention', recordIds: string[] | null): Promise<void> {
        if (!affected) return;
        const p = new ParamList();
        const cols = `(${p.push(tableKey)}, ${p.push(connectionId ?? null)}, ${p.push(affected)}, ${p.push(reason)}, ${p.push(recordIds && recordIds.length ? recordIds : null)})`;
        try {
            await this.dataSource.query(`INSERT INTO table_delete_events (table_key, connection_id, affected, reason, record_ids) VALUES ${cols}`, p.all);
        } catch (err) {
            // Ledger is advisory; swallow so the (already-committed) delete still succeeds.
            void err;
        }
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
    async updateAndWrite(template: TableTemplate, connectionId: string | undefined, rowId: string, data: Record<string, unknown>): Promise<DatasetUpdateResult> {
        if (!isUuid(rowId)) throw new BadRequestException(`Invalid row id "${rowId}"`);

        if (template.write && (!connectionId || !template.write.connections.some((r) => r.connectionId === connectionId))) {
            throw new BadRequestException(`Connection "${connectionId ?? ''}" is not allowed to write back for table "${template.key}"`);
        }

        const p = new ParamList();
        const dataPh = p.push(JSON.stringify(data));
        const where: string[] = [`id = ${p.push(rowId)}`, `table_key = ${p.push(template.key)}`];
        if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);

        // UPDATE returns [rows, rowCount] via TypeORM's raw query() — see deleteRows().
        const [rows]: [TableRowRow[], number] = await this.dataSource.query(
            `UPDATE table_rows SET data = ${dataPh}::jsonb WHERE ${where.join(' AND ')}
       RETURNING id, data, created_at, updated_at, write_status, write_error, last_written_at, external_ref, submission_status, sii_response`,
            p.all
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
        await this.markQueued(this.dataSource.manager, template, [updated.id]);
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
        opts?: { trigger?: 'event' | 'schedule' | 'manual'; groupValues?: Record<string, string> | null; connectionId?: string | null }
    ): Promise<{ batchId: string; status: 'sent' | 'error'; error?: string } | null> {
        if (!rows.length) return null;
        if (!template.write) {
            throw new BadRequestException(`Table "${template.key}" has no write config to submit rows through`);
        }

        const batchId = randomUUID();
        const ids = rows.map((r) => r.id);
        const trigger = opts?.trigger ?? template.write.trigger ?? 'event';
        const groupValues = opts?.groupValues ?? null;
        // Each group submits through the connection its rows were ingested under
        // (see `partitionAndSubmit`) — there is no fallback connection anymore.
        const connectionId = opts?.connectionId ?? null;
        let connectionName: string | null = null;

        // Defense in depth: `updateAndWrite` already blocks edits from a
        // disallowed connection before rows are ever queued, but a connection
        // can be dropped from the allowlist after rows are queued (or via the
        // cron/force-submit paths, which don't go through updateAndWrite) — so
        // the gate is re-checked at the one place that actually sends.
        const rule = connectionId ? template.write.connections.find((r) => r.connectionId === connectionId) : undefined;
        if (!rule) {
            const message = `Connection "${connectionId ?? ''}" is not allowed to write back for table "${template.key}"`;
            await this.markGroupResult(ids, batchId, 'error', message, 'queued');
            await this.recordRun({
                template,
                connectionId,
                connectionName: null,
                trigger,
                groupValues,
                rowCount: rows.length,
                status: 'error',
                httpStatus: null,
                errorMessage: message,
                batchId,
            });
            return { batchId, status: 'error', error: message };
        }

        // Payload is ALWAYS an array — a single-row event send still ships as an
        // array of 1, per the external-system contract. Keys are normalized to
        // camelCase for the outbound submission. `internal_ref` carries our own
        // row id so the external system can echo it back on its result callback —
        // that's what correlates the callback to a row, instead of relying on a
        // vendor-issued id plucked from the response.
        const excludedKeys = new Set(template.columns.filter((c) => c.excludeFromPayload).map((c) => c.key));
        const payload = rows.map((r) => ({
            internal_ref: r.id,
            ...(toCamelCase(excludedKeys.size ? Object.fromEntries(Object.entries(r.data).filter(([k]) => !excludedKeys.has(k))) : r.data) as Record<string, unknown>),
        }));
        let payloadPreview: unknown;

        try {
            const conn = await this.connections.resolveById(rule.connectionId);
            connectionName = conn.name;
            const clientId = resolveClave(conn);
            payloadPreview = { clientId, payload };
            const { status, data } = await this.client.send(conn, { method: rule.method, path: rule.path, query: rule.query }, payloadPreview);

            if (status >= 200 && status < 300) {
                await this.markGroupResult(ids, batchId, 'sent', null, 'pending');
                await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: rows.length, status: 'sent', httpStatus: status, batchId, payloadPreview });
                return { batchId, status: 'sent' };
            }

            // Detailed error: include the external system's response body, not just the
            // bare status — "responded 400" alone is useless for diagnosing a rejection.
            const message = `External system responded ${status}: ${truncate(safeStringify(data))}`;
            await this.markGroupResult(ids, batchId, 'error', message, 'queued');
            await this.recordRun({
                template,
                connectionId,
                connectionName,
                trigger,
                groupValues,
                rowCount: rows.length,
                status: 'error',
                httpStatus: status,
                errorMessage: message,
                batchId,
                payloadPreview,
                responseBody: data ?? null,
            });
            return { batchId, status: 'error', error: message };
        } catch (err) {
            // Transport failure (network/DNS/timeout): send() only throws here, since
            // non-2xx responses are returned, not thrown (validateStatus always true).
            const responseBody = axiosResponseBody(err);
            const base = err instanceof Error ? err.message : String(err);
            const message = responseBody !== null ? `${base}: ${truncate(safeStringify(responseBody))}` : base;
            await this.markGroupResult(ids, batchId, 'error', message, 'queued');
            await this.recordRun({
                template,
                connectionId,
                connectionName,
                trigger,
                groupValues,
                rowCount: rows.length,
                status: 'error',
                httpStatus: null,
                errorMessage: message,
                batchId,
                payloadPreview,
                responseBody,
            });
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

    private async markGroupResult(ids: string[], batchId: string, writeStatus: 'sent' | 'error', writeError: string | null, submissionStatus: 'pending' | 'queued'): Promise<void> {
        await this.dataSource.query(
            `UPDATE table_rows
         SET batch_id = $1, write_status = $2, write_error = $3, last_written_at = now(), submission_status = $4
         WHERE id = ANY($5::uuid[])`,
            [batchId, writeStatus, writeError, submissionStatus, ids]
        );
    }

    /**
     * Mark rows as a fresh submission attempt (`submission_status='queued'`,
     * clearing any stale `batch_id`/`sii_response` from a previous attempt).
     * Never sends anything itself — creation waits for the internal cron, edits
     * are pushed separately via `enqueueEventSend`. No-op when the template has
     * no `write` config or there are no ids.
     */
    private async markQueued(manager: EntityManager, template: TableTemplate, ids: string[]): Promise<void> {
        if (!template.write || !ids.length) return;
        await manager.query(`UPDATE table_rows SET submission_status = 'queued', batch_id = NULL, sii_response = NULL WHERE id = ANY($1::uuid[])`, [ids]);
    }

    /**
     * Enqueue an immediate, row-targeted send of a single edited row (event mode).
     * `delay: 0` so it fires right away; a stable `jobId` per row dedupes rapid
     * re-edits while one is still in flight. The processor re-checks the row is
     * still `queued` before sending, so a stale/duplicate job simply no-ops.
     */
    private async enqueueEventSend(tableKey: string, rowId: string, connectionId: string | null): Promise<void> {
        await this.writeEvent.add('event', { tableKey, rowId, connectionId }, { ...DEFAULT_JOB_OPTS, jobId: `write-event-${rowId}`, delay: 0 });
    }
}

/**
 * Split rows into chunks of ≤1000 so a single multi-VALUES INSERT (4 params/row)
 * stays well under Postgres' 65535 bind-parameter ceiling.
 */
function chunk1000<T>(rows: T[]): T[][] {
    const out: T[][] = [];
    for (let i = 0; i < rows.length; i += 1000) out.push(rows.slice(i, i + 1000));
    return out;
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
