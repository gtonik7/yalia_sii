import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource, EntityManager } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { TableTemplate, WriteConnectionRule } from './entities/table-template.entity';
import { DatasetDeleteParams, DatasetPage, DatasetQuery, DatasetUpdateResult } from '../datasets/dataset.types';
import { resolveClave, SourceConnectionsService } from '../connections/source-connections.service';
import { SourceHttpClient } from '../connections/source-http.client';
import { TableWriteRunService } from './table-write-run.service';
import { DomainEmitterService } from '../outbox/domain-emitter.service';
import type { DomainEvent } from '../outbox/domain-event.types';
import { assertColumnKey, assertTableKey, escapeLike, isUuid, ParamList, sqlStringLiteral } from '../core/sql/sql-params.util';
import { buildCollapsedPayloadItem } from './write-collapse.util';

/** Loosely matches ISO 8601 date/datetime strings — guards the timestamptz cast in query()'s date-range filter. */
const ISO_DATETIME_RE = String.raw`^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$`;

/** Tope del COUNT(*) de paginación — ver comentario en query(). */
const COUNT_CAP = 20_000;

/** Tope duro de grupos devueltos por `aggregate()` — acota el coste de un group-by ad-hoc. */
const AGG_ROW_CAP = 5_000;

/** Máximo de dimensiones de agrupación combinables en un informe ad-hoc. */
const MAX_AGG_DIMENSIONS = 4;

/**
 * Sentinel filter value meaning "column has no value" (missing/null/empty).
 * Mirrored in yalia_hub_fe (NULL_FILTER_VALUE) and yalia_datatable; kept
 * distinct from '' so it survives the `v === ''` skip in applyFilters().
 */
const NULL_FILTER_VALUE = '__null__';

/** Tope defensivo de ids comprobables en una llamada a findMissingIds() (el hub ya capa antes de llamar). */
const MAX_FIND_MISSING_IDS = 20_000;

/**
 * Derives the same coarse state the records grid paints for `_writeStatus`
 * ('queued'/'review'/'sent'/'error') from the physical write_status/write_error/
 * submission_status columns, so filter and sort agree with what's displayed.
 * 'review' (submission_status='revisado') is kept apart from 'queued': a row
 * a human edited and re-queued, vs. one that just arrived via ingest —
 * otherwise identical everywhere downstream (write-sweep eligibility, etc.).
 */
const WRITE_STATUS_CASE_SQL = `CASE
      WHEN (write_error IS NOT NULL AND write_error <> '') OR write_status = 'error' THEN 'error'
      WHEN write_status = 'sent' OR submission_status = 'pending' THEN 'sent'
      WHEN submission_status = 'revisado' THEN 'review'
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
    batch_id: string | null;
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

export type TableAggregateHavingOp = 'gt' | 'gte' | 'lt' | 'lte' | 'eq' | 'ne';

/**
 * One post-aggregation filter condition of an ad-hoc report (SQL HAVING): keeps only
 * groups where `metric` compares true against `value`. `metric` is either `'count'`
 * (the always-implicit COUNT(*)) or an index into the report's `metrics` array —
 * that metric must already be requested. Multiple conditions combine with AND, same
 * as every other filter DSL in this satellite (see `applyFilters`).
 */
export interface TableAggregateHaving {
    metric: 'count' | number;
    op: TableAggregateHavingOp;
    value: number;
}

export interface TableAggregateParams {
    connectionId?: string;
    filters?: Record<string, string>;
    groupBy: TableAggregateGroupBy[];
    metrics?: TableAggregateMetric[];
    having?: TableAggregateHaving[];
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
        private readonly writeRuns: TableWriteRunService,
        private readonly emitter: DomainEmitterService
    ) {}

    /**
     * Store one or more rows for a template. Upserts by `idField`/`idFields`
     * (scoped to connectionId) when the template declares one; otherwise appends.
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
            const idFields = template.idFields?.length ? template.idFields : template.idField ? [template.idField] : [];
            if (idFields.length) {
                // idFields/table.key are already validated by the DTO when the template
                // was saved (and by TableIndexManagerService when the unique index was
                // built); re-checked here as defense in depth since they're interpolated
                // as literal SQL text below — Postgres requires the ON CONFLICT partial
                // index predicate to match the index's predicate verbatim, not bound.
                // With a single field this reproduces the plain `(data ->> 'x')` expression
                // byte-for-byte (matching the existing unique index), so single-idField
                // templates are unaffected by the composite generalization below.
                idFields.forEach(assertColumnKey);
                assertTableKey(template.key);
                if (template.recencyField) assertColumnKey(template.recencyField);
                const idExpr = idFields.map((f) => `(data ->> ${sqlStringLiteral(f)})`).join(', ');
                const tableKeyLit = sqlStringLiteral(template.key);
                const recencyField = template.recencyField;
                // Numeric recency of a row: missing/non-numeric = -1, so an unstamped row
                // never wins over one that does carry a real recency value.
                const recencyOf = (data: Record<string, unknown>): number => {
                    if (!recencyField) return 0;
                    const n = Number(data[recencyField]);
                    return Number.isFinite(n) ? n : -1;
                };

                // Composite key of a row: joined by a control char that can't appear in
                // ingested JSON string values, so distinct field-value tuples never collide.
                const keyOf = (data: Record<string, unknown>): string | undefined => {
                    const parts = idFields.map((f) => data[f]);
                    if (parts.some((v) => v === undefined || v === null || v === '')) return undefined;
                    return parts.map((v) => String(v)).join('');
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
                    const key = keyOf(data);
                    if (key === undefined) {
                        withoutId.push(data);
                        continue;
                    }
                    const prev = withId.get(key);
                    if (!prev || recencyOf(data) >= recencyOf(prev)) withId.set(key, data);
                }

                // Sorted by conflict key (not arrival order) so two concurrent ingest()
                // transactions whose row sets overlap always take table_rows' row locks
                // in the same order — arrival order comes straight from the source
                // payload (SFTP/webhook), which two concurrent batches for the same
                // connection have no reason to agree on. Without this, transaction A
                // could lock id X then wait on Y while transaction B locks Y then waits
                // on X, and Postgres kills one with "deadlock detected".
                const sortedWithId = [...withId.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([, data]) => data);

                for (const chunk of chunk1000(sortedWithId)) {
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
                    // recency is >= the stored one AND the stored row isn't already
                    // `correcto` (SII-accepted rows are immutable — a reload must never
                    // touch them again, ever); a row that loses the guard is simply
                    // absent from RETURNING (Postgres treats a no-op conflict as neither
                    // inserted nor updated) — that gap is what skippedStale counts.
                    const correctoGuard = `lower(coalesce(table_rows.submission_status, '')) <> 'correcto'`;
                    const recencyGuard = recencyField
                        ? ` AND COALESCE((table_rows.data ->> ${sqlStringLiteral(recencyField)})::numeric, -1)
                 <= COALESCE((EXCLUDED.data ->> ${sqlStringLiteral(recencyField)})::numeric, -1)`
                        : '';
                    const returned: { id: string; inserted: boolean }[] = await manager.query(
                        `INSERT INTO table_rows (table_key, connection_id, data, trace_id)
             VALUES ${values.join(',')}
             ON CONFLICT (connection_id, ${idExpr}) WHERE table_key = ${tableKeyLit}
             DO UPDATE SET data = EXCLUDED.data, trace_id = EXCLUDED.trace_id WHERE ${correctoGuard}${recencyGuard}
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

    /**
     * Manually create one row for a write-enabled, `creatable` table (the
     * "Nuevo registro" action). Mirrors ingest of a single row: the row lands
     * `queued` and is sent later by the connection's write cron or an explicit
     * "Forzar envío" using the rule's create target (method/path) — creation
     * never sends inline, exactly like data-load ingest. Gated on `creatable`
     * (independent of `editable`) and the connection resolving to a write rule.
     */
    async createRow(
        template: TableTemplate,
        connectionId: string | undefined,
        data: Record<string, unknown>
    ): Promise<{ ok: boolean; inserted: number; upserted: number; skippedStale: number }> {
        if (!template.write) {
            throw new BadRequestException(`Table "${template.key}" has no write config — manual record creation is not available`);
        }
        if (!template.write.creatable) {
            throw new BadRequestException(`Table "${template.key}" does not allow manual record creation`);
        }
        if (!connectionId || !resolveWriteRule(template.write.connections, connectionId)) {
            throw new BadRequestException(`Connection "${connectionId ?? ''}" is not allowed to write back for table "${template.key}"`);
        }
        if (!data || typeof data !== 'object' || Array.isArray(data)) {
            throw new BadRequestException('El registro debe ser un objeto JSON');
        }
        const { inserted, upserted, skippedStale } = await this.ingest(template, [data], connectionId);
        return { ok: true, inserted, upserted, skippedStale };
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
        const statsIdFields = template.idFields?.length ? template.idFields : template.idField ? [template.idField] : [];
        if (statsIdFields.length) {
            statsIdFields.forEach(assertColumnKey);
            const parts = statsIdFields.map((f) => `(data ->> ${sqlStringLiteral(f)})`);
            // A single field stays a plain scalar expression; 2+ fields form a row
            // value so `count(DISTINCT (...))` counts distinct field-value tuples.
            const idExpr = parts.length === 1 ? parts[0] : `(${parts.join(', ')})`;
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
     * Re-baselina el contador global de "borrados no controlados": inserta una fila
     * `baseline` que sube Σ(affected) del ledger hasta el `n_tup_del` actual, dejando
     * `uncontrolled = max(0, n_tup_del − Σ)` en 0. Pensado para cuando se borra todo
     * por fuera de la app (TRUNCATE / SQL directo) y se resincroniza: eso dispara
     * `n_tup_del` sin registrar borrados voluntarios, inflando el contador de forma
     * permanente. Append-only (no toca el histórico del ledger) y global —igual que
     * `n_tup_del`, que es un stat de la tabla física y no admite scope por template.
     */
    async resetDeleteBaseline(): Promise<{ deletedSinceLoad: number; voluntaryDeletes: number; uncontrolledDeletes: number; rebaselinedBy: number }> {
        const [statRow]: { n: number }[] = await this.dataSource.query(`SELECT n_tup_del::int AS n FROM pg_stat_user_tables WHERE relname = 'table_rows'`);
        const deletedSinceLoad = statRow?.n ?? 0;
        const [ledgerRow]: { n: number }[] = await this.dataSource.query(`SELECT COALESCE(SUM(affected), 0)::int AS n FROM table_delete_events`);
        const before = ledgerRow?.n ?? 0;
        const delta = Math.max(0, deletedSinceLoad - before);
        if (delta > 0) {
            await this.dataSource.query(`INSERT INTO table_delete_events (table_key, connection_id, affected, reason) VALUES ('*', NULL, $1, 'baseline')`, [delta]);
        }
        const voluntaryDeletes = before + delta;
        return { deletedSinceLoad, voluntaryDeletes, uncontrolledDeletes: Math.max(0, deletedSinceLoad - voluntaryDeletes), rebaselinedBy: delta };
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
                    where.push(`submission_status = ${p.push(v)}`);
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
                if (v === NULL_FILTER_VALUE) {
                    where.push(`((data ->> ${p.push(k)}) IS NULL OR (data ->> ${p.push(k)}) = '')`);
                } else if (col?.type === 'number') {
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

        // Post-aggregation filter (HAVING): Postgres can't see SELECT-list aliases in a
        // real HAVING clause, so instead the group-by is wrapped as a subquery and the
        // conditions run as a plain WHERE over its already-typed numeric output columns
        // (`count`, `metric_i`) — no re-casting jsonb, no duplicating the aggregate exprs.
        const havingOpSql: Record<TableAggregateHavingOp, string> = { gt: '>', gte: '>=', lt: '<', lte: '<=', eq: '=', ne: '<>' };
        const having = params.having ?? [];
        const havingWhere = having.map((h) => {
            if (!havingOpSql[h.op]) throw new BadRequestException(`Operador de filtro de grupo no soportado: ${h.op}`);
            if (typeof h.value !== 'number' || Number.isNaN(h.value)) throw new BadRequestException('El valor del filtro de grupo debe ser numérico');
            let col: string;
            if (h.metric === 'count') {
                col = 'count';
            } else {
                if (!Number.isInteger(h.metric) || h.metric < 0 || h.metric >= metrics.length) {
                    throw new BadRequestException(`Métrica de filtro de grupo desconocida: ${h.metric}`);
                }
                col = `metric_${h.metric}`;
            }
            return `${col} ${havingOpSql[h.op]} ${p.push(h.value)}`;
        });

        // Fetch one past the cap so we can tell "exactly at cap" from "truncated".
        const limitPh = p.push(AGG_ROW_CAP + 1);
        const innerSql = `SELECT ${selectParts.join(', ')}
        FROM table_rows
        WHERE ${where.join(' AND ')}
        GROUP BY ${groupExprs.join(', ')}`;
        const sql = `SELECT * FROM (${innerSql}) agg
        ${havingWhere.length ? `WHERE ${havingWhere.join(' AND ')}` : ''}
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
                `SELECT id, data, created_at, updated_at, write_status, write_error, last_written_at, external_ref, submission_status, sii_response, batch_id
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
                _batchId: r.batch_id,
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
        const deleteIdFields = template.idFields?.length ? template.idFields : template.idField ? [template.idField] : [];
        const returning = deleteIdFields.length
            ? ` RETURNING ${deleteIdFields.length === 1 ? `(data ->> ${sqlStringLiteral(deleteIdFields[0])})` : `concat_ws(':', ${deleteIdFields.map((f) => `(data ->> ${sqlStringLiteral(f)})`).join(', ')})`} AS record_id`
            : '';

        if (params.ids?.length) {
            const validIds = params.ids.filter(isUuid);
            if (!validIds.length) return { affected: 0 };
            where.push(`id = ANY(${p.push(validIds)}::uuid[])`);
            // Propagate a DELETE to the external system for each affected row BEFORE
            // removing it locally (the row's data is needed to target the resource).
            await this.propagateExternalDeletes(template, where.join(' AND '), p.all);
            const result = await this.dataSource.query(`DELETE FROM table_rows WHERE ${where.join(' AND ')}${returning}`, p.all);
            const [affected, recordIds] = this.deleteResult(result, returning);
            await this.recordDeleteEvent(template.key, params.connectionId, affected, 'ids', recordIds);
            return { affected };
        }

        if (params.olderThanDays !== undefined) {
            const cutoff = new Date(Date.now() - params.olderThanDays * 24 * 3600 * 1000);
            where.push(`created_at < ${p.push(cutoff)}`);
            await this.propagateExternalDeletes(template, where.join(' AND '), p.all);
            const result = await this.dataSource.query(`DELETE FROM table_rows WHERE ${where.join(' AND ')}${returning}`, p.all);
            const [affected, recordIds] = this.deleteResult(result, returning);
            await this.recordDeleteEvent(template.key, params.connectionId, affected, 'retention', recordIds);
            return { affected };
        }

        const nonEmptyFilters = Object.fromEntries(Object.entries(params.filters ?? {}).filter(([, v]) => v !== ''));
        if (Object.keys(nonEmptyFilters).length) {
            this.applyFilters(template, nonEmptyFilters, where, p);
            await this.propagateExternalDeletes(template, where.join(' AND '), p.all);
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
     * When the template opts in (`write.deleteEnabled`), propagate a DELETE to the
     * external system for every row matched by a pending local deletion — called
     * from all three `deleteRows` paths (ids/retention/bulk) BEFORE the local
     * DELETE, since each request needs the row's data to target its resource.
     * Rows are grouped by ingest connection (the rule supplies method/path/query,
     * the connection supplies baseUrl/auth), one request per row with `{id}`
     * substituted from `idField` (fallback internal id). Best-effort: outcomes
     * (incl. failures) are logged to the write-run history but never throw, so a
     * local delete/purge is never blocked by an external error.
     *
     * Cost note: one HTTP DELETE per row — a large retention/bulk purge fans out
     * accordingly. Acceptable because bulk runs in the background (operation_runs,
     * see maintenance.processor) and retention on the daily cron; row-selection
     * deletes are bounded by the UI selection.
     */
    private async propagateExternalDeletes(template: TableTemplate, whereSql: string, whereParams: unknown[]): Promise<void> {
        if (!template.write?.deleteEnabled) return;
        const rows: { id: string; data: Record<string, unknown>; connection_id: string | null }[] = await this.dataSource.query(
            `SELECT id, data, connection_id FROM table_rows WHERE ${whereSql}`,
            whereParams
        );
        if (!rows.length) return;

        const byConnection = new Map<string, { id: string; data: Record<string, unknown> }[]>();
        for (const r of rows) {
            const cid = r.connection_id ?? '';
            const bucket = byConnection.get(cid);
            if (bucket) bucket.push({ id: r.id, data: r.data });
            else byConnection.set(cid, [{ id: r.id, data: r.data }]);
        }

        for (const [connectionId, group] of byConnection) {
            const rule = resolveWriteRule(template.write.connections, connectionId || null);
            // No rule for this connection (or no connection at all) — nothing to
            // target externally; the local delete still proceeds.
            if (!rule) continue;
            let conn;
            try {
                conn = await this.connections.resolveById(connectionId);
            } catch {
                continue;
            }
            const target = resolveSendTarget(rule, 'delete');
            for (const row of group) {
                const batchId = randomUUID();
                const path = applyIdSubstitution(target.path, row, template.idField, template.idFields);
                try {
                    const { status, data } = await this.client.send(conn, { method: target.method, path, query: target.query });
                    const ok = status >= 200 && status < 300;
                    await this.recordRun({
                        template,
                        connectionId,
                        connectionName: conn.name,
                        trigger: 'manual',
                        groupValues: null,
                        rowCount: 1,
                        status: ok ? 'sent' : 'error',
                        httpStatus: status,
                        errorMessage: ok ? undefined : `External system responded ${status}: ${truncate(safeStringify(data))}`,
                        batchId,
                        responseBody: ok ? undefined : (data ?? null),
                    });
                } catch (err) {
                    const responseBody = axiosResponseBody(err);
                    const base = err instanceof Error ? err.message : String(err);
                    const message = responseBody !== null ? `${base}: ${truncate(safeStringify(responseBody))}` : base;
                    await this.recordRun({
                        template,
                        connectionId,
                        connectionName: conn.name,
                        trigger: 'manual',
                        groupValues: null,
                        rowCount: 1,
                        status: 'error',
                        httpStatus: null,
                        errorMessage: message,
                        batchId,
                        responseBody,
                    });
                }
            }
        }
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

        if (template.write) {
            // Defense in depth: the dataset only registers `update` when
            // `editable` is true (see table-dataset.bridge.ts), but this method
            // is also reachable directly, so re-check here.
            if (!template.write.editable) {
                throw new BadRequestException(`Table "${template.key}" is not editable`);
            }
            if (!connectionId || !resolveWriteRule(template.write.connections, connectionId)) {
                throw new BadRequestException(`Connection "${connectionId ?? ''}" is not allowed to write back for table "${template.key}"`);
            }
        }

        const p = new ParamList();
        const dataPh = p.push(JSON.stringify(data));
        const where: string[] = [`id = ${p.push(rowId)}`, `table_key = ${p.push(template.key)}`];
        if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
        // A row already accepted by SII ('correcto', terminal) is immutable —
        // the FE already disables the form for it, this is defense in depth
        // for a direct call to this endpoint.
        where.push(`lower(coalesce(submission_status, '')) <> 'correcto'`);

        // UPDATE returns [rows, rowCount] via TypeORM's raw query() — see deleteRows().
        const [rows]: [TableRowRow[], number] = await this.dataSource.query(
            `UPDATE table_rows SET data = ${dataPh}::jsonb WHERE ${where.join(' AND ')}
       RETURNING id, data, created_at, updated_at, write_status, write_error, last_written_at, external_ref, submission_status, sii_response`,
            p.all
        );
        const updated = rows[0];
        if (!updated) {
            const existsPh = new ParamList();
            const existsWhere: string[] = [`id = ${existsPh.push(rowId)}`, `table_key = ${existsPh.push(template.key)}`];
            if (connectionId) existsWhere.push(`connection_id = ${existsPh.push(connectionId)}`);
            const [existing]: { submission_status: string | null }[] = await this.dataSource.query(
                `SELECT submission_status FROM table_rows WHERE ${existsWhere.join(' AND ')}`,
                existsPh.all
            );
            if (existing) throw new BadRequestException(`Row "${rowId}" is already "correcto" and cannot be modified`);
            throw new NotFoundException(`Row "${rowId}" not found for table "${template.key}"`);
        }

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

        // 'revisado' (not 'queued'): distinguishes a manually-corrected row from
        // one that just arrived via data load, while behaving identically for
        // every downstream write-sweep/eligibility check. `markQueued` also
        // (re)stamps `group_id` from the row's current `groupBy` values.
        // Editing never sends anymore: the row waits for the connection's write
        // cron (if the table is `scheduled`) or an explicit "Forzar envío".
        await this.markQueued(this.dataSource.manager, template, [updated.id], 'revisado');
        // markQueued() updates the DB but not this in-memory row — reflect the same
        // fresh-attempt reset here so the response isn't stale.
        updated.submission_status = 'revisado';
        updated.sii_response = null;
        return { row: flatten(), external: { attempted: true, status: 'revisado' } };
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
        opts?: { trigger?: 'schedule' | 'manual'; groupValues?: Record<string, string> | null; connectionId?: string | null; markRowIds?: string[]; phase?: SendPhase }
    ): Promise<{ batchId: string; status: 'sent' | 'error'; error?: string } | null> {
        if (!rows.length) return null;
        if (!template.write) {
            throw new BadRequestException(`Table "${template.key}" has no write config to submit rows through`);
        }

        const batchId = randomUUID();
        // `rows` is the full payload (with `collapse`, the whole group incl.
        // already-`correcto` context lines). `markIds` is the subset whose
        // status we actually advance — when omitted, every payload row is marked
        // (cron/non-collapse). Everything that WRITES status uses `markIds`, so
        // context lines are shipped but never re-marked or downgraded on error.
        const ids = rows.map((r) => r.id);
        const markIds = opts?.markRowIds ?? ids;
        const trigger = opts?.trigger ?? 'schedule';
        const groupValues = opts?.groupValues ?? null;
        // Each group submits through the connection its rows were ingested under
        // (see `partitionAndSubmit`), resolved to a rule via `resolveWriteRule`
        // (specific match first, generic/fallback rule second).
        const connectionId = opts?.connectionId ?? null;
        // 'create' unless the caller (partitionAndSubmit) says otherwise — rows
        // fetched by direct id/legacy callers with no phase info default to
        // 'create', matching the pre-existing single-target behavior.
        const phase = opts?.phase ?? 'create';
        let connectionName: string | null = null;

        // Defense in depth: `updateAndWrite` already blocks edits from a
        // disallowed connection before rows are ever queued, but a connection
        // can be dropped from the allowlist after rows are queued (or via the
        // cron/force-submit paths, which don't go through updateAndWrite) — so
        // the gate is re-checked at the one place that actually sends.
        const rule = resolveWriteRule(template.write.connections, connectionId);
        if (!rule) {
            const message = `Connection "${connectionId ?? ''}" is not allowed to write back for table "${template.key}"`;
            await this.commitOutcome(markIds, batchId, 'error', message, 'queued',
                this.buildEmittedEvent({ template, batchId, connectionId, connectionName: null, trigger, groupValues, rowIds: markIds, status: 'error', httpStatus: null, errorMessage: message }));
            await this.recordRun({
                template,
                connectionId,
                connectionName: null,
                trigger,
                groupValues,
                rowCount: markIds.length,
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
        const camelRows = rows.map((r) => ({
            id: r.id,
            data: toCamelCase(excludedKeys.size ? Object.fromEntries(Object.entries(r.data).filter(([k]) => !excludedKeys.has(k))) : r.data) as Record<string, unknown>,
        }));
        // When `collapse` is configured, the whole chunk ships as ONE payload
        // item (the `groupBy` columns lifted to the top level, the rest
        // nested per-row) instead of one item per row — see buildCollapsedPayloadItem.
        const collapse = template.write.batch?.collapse;
        const groupByKeys = (template.write.batch?.groupBy || []).map((key) => key.replace(/_([a-z])/g, (_, char) => char.toUpperCase()));
        const payload = collapse
            ? [buildCollapsedPayloadItem(camelRows, groupByKeys, collapse.rowsField || 'rows', batchId)]
            : camelRows.map((r) => ({ internal_ref: r.id, ...r.data }));
        let payloadPreview: unknown;

        try {
            // Always resolve the actual ingest connection (not `rule.connectionId`,
            // which is absent for the generic/fallback rule) — the rule only
            // supplies method/path/query, the connection supplies baseUrl/auth.
            const conn = await this.connections.resolveById(connectionId!);
            connectionName = conn.name;
            const clientId = resolveClave(conn);
            payloadPreview = { clientId, payload };
            const { status, data } = await this.client.send(conn, resolveSendTarget(rule, phase), payloadPreview);

            if (status >= 200 && status < 300) {
                await this.commitOutcome(markIds, batchId, 'sent', null, 'pending',
                    this.buildEmittedEvent({ template, batchId, connectionId, connectionName, trigger, groupValues, rowIds: markIds, status: 'sent', httpStatus: status }));
                await this.recordRun({ template, connectionId, connectionName, trigger, groupValues, rowCount: markIds.length, status: 'sent', httpStatus: status, batchId, payloadPreview });
                return { batchId, status: 'sent' };
            }

            // Detailed error: include the external system's response body, not just the
            // bare status — "responded 400" alone is useless for diagnosing a rejection.
            const message = `External system responded ${status}: ${truncate(safeStringify(data))}`;
            await this.commitOutcome(markIds, batchId, 'error', message, 'queued',
                this.buildEmittedEvent({ template, batchId, connectionId, connectionName, trigger, groupValues, rowIds: markIds, status: 'error', httpStatus: status, errorMessage: message }));
            await this.recordRun({
                template,
                connectionId,
                connectionName,
                trigger,
                groupValues,
                rowCount: markIds.length,
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
            await this.commitOutcome(markIds, batchId, 'error', message, 'queued',
                this.buildEmittedEvent({ template, batchId, connectionId, connectionName, trigger, groupValues, rowIds: markIds, status: 'error', httpStatus: null, errorMessage: message }));
            await this.recordRun({
                template,
                connectionId,
                connectionName,
                trigger,
                groupValues,
                rowCount: markIds.length,
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
        trigger: 'schedule' | 'manual';
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

    private async markGroupResult(ids: string[], batchId: string, writeStatus: 'sent' | 'error', writeError: string | null, submissionStatus: 'pending' | 'queued', manager?: EntityManager): Promise<void> {
        await (manager ?? this.dataSource).query(
            `UPDATE table_rows
         SET batch_id = $1, write_status = $2, write_error = $3, last_written_at = now(), submission_status = $4
         WHERE id = ANY($5::uuid[])`,
            [batchId, writeStatus, writeError, submissionStatus, ids]
        );
    }

    /**
     * Ata en UNA transacción la transición de estado de las filas del batch
     * (`markGroupResult`) y el spool del evento de dominio saliente
     * (`emitida.sent`/`emitida.error`) en el outbox. Si el commit falla, ni el
     * estado ni el evento quedan — no hay estado "enviado" sin su evento, ni
     * evento de un envío que no se registró. El `recordRun()` (traza best-effort)
     * queda deliberadamente FUERA, como hasta ahora.
     */
    private async commitOutcome(
        ids: string[],
        batchId: string,
        status: 'sent' | 'error',
        message: string | null,
        submissionStatus: 'pending' | 'queued',
        event: DomainEvent
    ): Promise<void> {
        await this.dataSource.transaction(async (m) => {
            await this.markGroupResult(ids, batchId, status, message, submissionStatus, m);
            await this.emitter.emit(event, m);
        });
    }

    /** Construye el evento de dominio saliente de un desenlace de `submitGroup`. */
    private buildEmittedEvent(args: {
        template: TableTemplate;
        batchId: string;
        connectionId: string | null;
        connectionName: string | null;
        trigger: 'schedule' | 'manual';
        groupValues: Record<string, string> | null;
        rowIds: string[];
        status: 'sent' | 'error';
        httpStatus: number | null;
        errorMessage?: string;
    }): DomainEvent {
        return {
            operation: args.status === 'sent' ? 'emitida.sent' : 'emitida.error',
            // La conexión de origen resuelve el flow en el hub; el emitter de egress
            // usa su propia conexión, fijada en el destino del flow.
            connectionId: args.connectionId ?? '',
            idempotencyKey: args.batchId,
            payload: {
                tableKey: args.template.key,
                batchId: args.batchId,
                connectionId: args.connectionId,
                connectionName: args.connectionName,
                trigger: args.trigger,
                groupValues: args.groupValues,
                rowCount: args.rowIds.length,
                httpStatus: args.httpStatus,
                status: args.status,
                errorMessage: args.errorMessage ?? null,
                rowIds: args.rowIds,
            },
        };
    }

    /**
     * Mark rows as a fresh submission attempt (`submission_status` reset to
     * `status` — `'queued'` for rows landing via ingest, `'revisado'` for a
     * manual edit via updateAndWrite — clearing any stale `batch_id`/
     * `sii_response` from a previous attempt). `'revisado'` behaves exactly
     * like `'queued'` everywhere downstream (write-sweep queries, the derived
     * `_writeStatus`), it only exists so a manually-corrected row is
     * distinguishable from one that arrived untouched via data load. Never
     * sends anything itself — creation waits for the connection's write cron,
     * an edit waits for that cron or an explicit "Forzar envío". Also
     * (re)stamps `group_id` from the row's current `write.batch.groupBy` values
     * (NULL when the table doesn't group), so a group can later be shipped
     * complete (see `fetchGroupMembers` / `submitByIds`). No-op when the
     * template has no `write` config or there are no ids.
     */
    private async markQueued(manager: EntityManager, template: TableTemplate, ids: string[], status: 'queued' | 'revisado' = 'queued'): Promise<void> {
        if (!template.write || !ids.length) return;
        const groupIdSql = buildGroupIdSql(template.write.batch?.groupBy ?? []);
        await manager.query(
            `UPDATE table_rows
         SET submission_status = $2, batch_id = NULL, sii_response = NULL,
             write_status = NULL, write_error = NULL, last_written_at = NULL,
             group_id = ${groupIdSql}
         WHERE id = ANY($1::uuid[])`,
            [ids, status]
        );
    }
}

/**
 * Resolves which `WriteConnectionRule` applies for a given connection: a rule
 * whose `connectionId` matches exactly, falling back to the generic rule
 * (the one entry with no `connectionId`, if any). Returns `undefined` when
 * neither exists — the caller treats that as "not allowed to write back".
 */
function resolveWriteRule(connections: WriteConnectionRule[], connectionId: string | null): WriteConnectionRule | undefined {
    // No ingest connection to send through at all — the generic rule can't help either.
    if (!connectionId) return undefined;
    return connections.find((r) => r.connectionId === connectionId) ?? connections.find((r) => !r.connectionId);
}

/**
 * A row's send phase: 'create' for its first-ever send (submission_status='queued'),
 * 'update' for an edit (submission_status='revisado'), 'delete' for propagating a
 * local deletion to the external system (only when `write.deleteEnabled`).
 */
export type SendPhase = 'create' | 'update' | 'delete';

/** Effective method allowed per phase — only the 'delete' phase can resolve to 'DELETE'. */
type SendMethod = 'PUT' | 'PATCH' | 'POST' | 'DELETE';

/**
 * Resolves the effective method/path/query for a rule given the send phase.
 * `phase === 'update'` uses the rule's `update*` overrides and `phase ===
 * 'delete'` the `delete*` overrides (method defaulting to 'DELETE'), each
 * falling back to the base `method`/`path`/`query` field-by-field when an
 * override is absent — so a rule with no phase overrides behaves identically
 * to the base send (the pre-existing behavior for create/update).
 */
function resolveSendTarget(rule: WriteConnectionRule, phase: SendPhase): { method: SendMethod; path: string; query?: Record<string, string> } {
    if (phase === 'update') {
        return { method: rule.updateMethod ?? rule.method, path: rule.updatePath ?? rule.path, query: rule.updateQuery ?? rule.query };
    }
    if (phase === 'delete') {
        return { method: rule.deleteMethod ?? 'DELETE', path: rule.deletePath ?? rule.path, query: rule.deleteQuery ?? rule.query };
    }
    return { method: rule.method, path: rule.path, query: rule.query };
}

/**
 * Substitutes `{id}` in a write-back path with a row's external identifier —
 * `data[idField]`, or `data[idFields[0]]-data[idFields[1]]-...` when the
 * template declares a composite key, falling back to the row's internal id
 * when any part is missing. URL-encoded so the value is a single safe path
 * segment. Used by the per-row delete propagation, where each request must
 * target one specific external resource (unlike create/update, which batch
 * many rows to one URL).
 */
function applyIdSubstitution(path: string, row: { id: string; data: Record<string, unknown> }, idField: string, idFields?: string[] | null): string {
    if (!path.includes('{id}')) return path;
    const fields = idFields?.length ? idFields : idField ? [idField] : [];
    const parts = fields.map((f) => row.data[f]);
    const idValue = fields.length && parts.every((v) => v !== undefined && v !== null && v !== '') ? parts.map((v) => String(v)).join('-') : row.id;
    return path.replace(/\{id\}/g, encodeURIComponent(idValue));
}

/**
 * SQL expression that materializes a row's `group_id`: an md5 of `connection_id`
 * and each `write.batch.groupBy` column value, joined by an ASCII unit separator
 * (chr(31), which can't appear in the JSON string values). Rows sharing the same
 * groupBy values under the same connection collapse to the same id. Returns the
 * literal `NULL` when there are no groupBy columns. Column keys are validated
 * (assertColumnKey) since they are interpolated as literal SQL — MUST stay
 * byte-for-byte identical to the backfill in the `AddTableRowsGroupId` migration
 * so a re-stamp on edit never disagrees with the backfilled value.
 */
function buildGroupIdSql(groupBy: string[]): string {
    if (!groupBy.length) return 'NULL';
    const parts = [`coalesce(connection_id, '')`];
    for (const key of groupBy) {
        assertColumnKey(key);
        parts.push(`coalesce(data ->> ${sqlStringLiteral(key)}, '')`);
    }
    return `md5(${parts.join(` || chr(31) || `)})`;
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
