import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';
import { DatasetColumnType } from '../../datasets/dataset.types';

export interface NumberFormat {
    /** When true, render the stored value as-is (no numeric parsing/formatting); overrides every option below. */
    raw?: boolean;
    /** Number of decimal places to display (e.g., 2 for currency). */
    decimals?: number;
    /** Thousands separator; omit to suppress thousands grouping. */
    separator?: string;
    /** Decimal separator; defaults to '.' when omitted. */
    decimalSeparator?: string;
    /** Prefix to prepend (e.g., '€', '$'). */
    prefix?: string;
    /** Suffix to append (e.g., '%', ' units'). */
    suffix?: string;
}

export interface DateFormat {
    /** When true, render the stored value as-is (no date parsing/formatting); overrides `pattern`. */
    raw?: boolean;
    /** Custom pattern using tokens yyyy, MM, dd, HH, mm, ss (e.g. 'dd/MM/yyyy HH:mm'). Omitted = default es-ES datetime format. */
    pattern?: string;
}

export interface TableColumnDef {
    key: string;
    label: string;
    type: DatasetColumnType;
    filterable?: boolean;
    sortable?: boolean;
    /** When true, hidden from the records grid by default (still stored/queryable). */
    hidden?: boolean;
    /** When true, shown in the grid and edit form but the value can't be edited. */
    readOnly?: boolean;
    /** When true, this field is omitted from the outbound payload sent on write-back — informational only. */
    excludeFromPayload?: boolean;
    /** Number formatting rules for type='number' columns (display only, doesn't alter stored value). */
    numberFormat?: NumberFormat;
    /** Date formatting rules for type='date' columns (display only, doesn't alter stored value). */
    dateFormat?: DateFormat;
}

export interface TableSortDef {
    key: string;
    dir: 'asc' | 'desc';
}

/**
 * Partitions queued rows into separate outbound batches by one or more
 * column values (e.g. counterparty NIF + invoice type), instead of always
 * submitting everything queued for a template as one batch.
 */
export interface BatchConfig {
    /** Column keys (must exist in template.columns) that partition queued rows into distinct batches. Empty = one global batch. */
    groupBy: string[];
    /** Split a partition into smaller sub-batches once it exceeds this size. */
    maxBatchSize?: number;
    /**
     * Máximo de filas `queued` sacadas por tabla en cada pasada del cron; el
     * resto espera a la siguiente pasada. Default 10.000. Aplica al total de la
     * tabla (todos los grupos), antes de trocear por `maxBatchSize`.
     */
    maxRecordsPerPoll?: number;
    /**
     * When present, each outbound chunk is sent as ONE payload item instead of
     * one item per row: the `groupBy` columns are lifted to the top level
     * ("cabecera"), every other field is nested per-row under `rowsField`
     * (default `'rows'`), each still carrying its own `internal_ref`. Requires
     * `groupBy` to be non-empty — collapsing without a grouping key doesn't
     * make sense. See `buildCollapsedPayloadItem`.
     */
    collapse?: {
        /** Key under which the per-row differing fields are nested. Default `'rows'`. */
        rowsField?: string;
    };
}

/**
 * Binds one source connection to the endpoint its rows are pushed back to.
 * Method/path/query are per-connection (not a shared base) because the same
 * table can be exposed on connections whose external systems expect
 * different endpoints.
 */
export interface WriteConnectionRule {
    /**
     * Absent = the generic/fallback rule, used for any connection with no
     * more specific rule of its own. At most one rule per `connections` array
     * may omit `connectionId` (enforced in `TableTemplatesService.validate`);
     * see `resolveWriteRule` in `table-rows.service.ts` for the lookup order
     * (specific match first, generic fallback second).
     */
    connectionId?: string;
    /** Used for a row's first send (submission_status='queued'), and as the fallback for edits when the update* fields below are absent. */
    method: 'PUT' | 'PATCH' | 'POST';
    /**
     * Path appended to the connection baseUrl. `{id}` is replaced with
     * data[idField] (falling back to the row's internal id when idField is
     * unset or the value is missing).
     */
    path: string;
    /** Static query params merged into every write request for this connection. */
    query?: Record<string, string>;
    /** Override used instead of `method` for an edited row (submission_status='revisado'); absent = reuse `method`. */
    updateMethod?: 'PUT' | 'PATCH' | 'POST';
    /** Override used instead of `path` for an edited row; absent = reuse `path`. Same `{id}` substitution rules apply. */
    updatePath?: string;
    /** Override used instead of `query` for an edited row; absent = reuse `query`. */
    updateQuery?: Record<string, string>;
    /** Override used instead of `method` when propagating a delete (phase='delete'); absent = default 'DELETE'. Only used when `WriteConfig.deleteEnabled`. */
    deleteMethod?: 'PUT' | 'PATCH' | 'POST' | 'DELETE';
    /** Override used instead of `path` when propagating a delete; absent = reuse `path`. Same `{id}` substitution rules apply (targets the specific external resource). */
    deletePath?: string;
    /** Override used instead of `query` when propagating a delete; absent = reuse `query`. */
    deleteQuery?: Record<string, string>;
}

/**
 * Binds a table to source connections so a row is pushed back to the
 * external system. A row ingested under a connection with no matching
 * specific rule AND no generic rule in `connections` is rejected before it's
 * saved (`TableRowsService.updateAndWrite`) or sent
 * (`TableRowsService.submitGroup`).
 */
export interface WriteConfig {
    /**
     * When `true`, the connection's internal write cron (`WriteCronService`)
     * sweeps this table's queued rows on its cadence; when `false`/absent the
     * table is never swept automatically and only sends via "Forzar envío"
     * (`submitByIds`). There is no per-edit send anymore — an edit just leaves
     * the row `revisado`, waiting for a sweep or a manual force-submit.
     * Optional at this type level only for templates predating the field (the
     * `WriteConfigTriggerToScheduled` migration backfills it: old `'schedule'`
     * → `true`, old `'event'` → `false`); the DTO requires it on create/update.
     */
    scheduled?: boolean;
    /**
     * When `true`, rows are editable in the explorer (subject to each
     * column's own `readOnly`); when `false`/absent, the row detail is shown
     * read-only (same view as a table with no `write` at all) but sending
     * (`connections`/`scheduled`/`batch`) still applies to rows as they're
     * ingested/re-ingested — editability and outbound sending are independent.
     * Optional at this type level only for templates predating the field (the
     * `WriteConfigAddEditable` migration backfills existing writable
     * templates to `true`, preserving their current behavior); the DTO
     * requires it on create/update.
     */
    editable?: boolean;
    /**
     * When `true`, the explorer exposes a "Nuevo registro" action that lets a
     * user manually add a row (`TableRowsService.createRow`); the row lands
     * `queued` and goes out via the connection's write cron or "Forzar envío"
     * using the rule's create target, same as ingested rows — creating never
     * sends inline. Independent of `editable`: a table can allow manual
     * creation without allowing edits to existing rows, or vice versa.
     * Off/absent = no manual creation (the historical behavior).
     */
    creatable?: boolean;
    /**
     * When `true`, a local deletion of a row (any path: row-selection, mass
     * delete-by-filter, or retention purge) also propagates a DELETE request to
     * the external system for each affected row, using the resolved rule's
     * delete target (see `resolveSendTarget` phase 'delete': `deleteMethod`
     * default 'DELETE', over `deletePath ?? path` with `{id}` substitution).
     * Off/absent = deletes stay local (the historical behavior). Best-effort:
     * an external failure is recorded to the write-run history but never blocks
     * the local delete.
     */
    deleteEnabled?: boolean;
    /**
     * When `false`, the "Estado SII" column/field and every reference to it
     * (grid column, hidden "Respuesta SII" column, detail dialog badge,
     * SII message/error code, raw JSON response) are hidden from the explorer
     * and edit form — purely a display gate, the underlying
     * `submission_status`/`sii_response` columns and callback processing are
     * unaffected. Default/absent = `true` (existing behavior): tables whose
     * external system doesn't return an SII-style callback should set this to
     * `false` so the vendor-specific vocabulary doesn't leak into their UI.
     */
    showSiiStatus?: boolean;
    /** Per-connection rules, plus at most one generic/fallback rule (see `WriteConnectionRule.connectionId`). */
    connections: WriteConnectionRule[];
    /** Present when queued rows must be partitioned before submitting. */
    batch?: BatchConfig;
}

/**
 * User-managed template that turns an arbitrary pushed payload into a queryable
 * dataset. Defines which fields become columns and which of those are filterable
 * and/or sortable. Stored in Postgres and editable at runtime (CRUD).
 */
@Entity('table_templates')
export class TableTemplate {
    @PrimaryGeneratedColumn('uuid')
    id!: string;

    /** Stable identifier; also the dataset key in the explorer URL. */
    @Index({ unique: true })
    @Column({ type: 'varchar', length: 128 })
    key!: string;

    @Column({ type: 'varchar', length: 256 })
    label!: string;

    @Column({ type: 'text', nullable: true })
    description!: string | null;

    /**
     * Column key that uniquely identifies a row. When set, ingest upserts by it
     * (scoped to connectionId) instead of always inserting. Empty = append-only.
     */
    @Column({ type: 'varchar', length: 128, name: 'id_field', default: '' })
    idField!: string;

    /**
     * Composite upsert key: two or more column keys whose combined value
     * uniquely identifies a row (e.g. invoice + line number), used instead of
     * `idField` when set (mutually exclusive — `TableTemplatesService.validate`
     * enforces it). Every SQL site that keys off `idField` (ingest's ON
     * CONFLICT, the unique index, `getStats`' distinct count, `{id}`
     * substitution) treats `idFields` as the same concept generalized to N
     * columns joined instead of one. Null/empty = not composite (the
     * historical single-`idField` behavior).
     */
    @Column({ type: 'jsonb', name: 'id_fields', nullable: true })
    idFields!: string[] | null;

    /**
     * Column key that decides which duplicate wins the upsert: when set (and
     * `idField` is also set), ingest keeps the row with the greatest numeric
     * value of this field for a given id (both within a single call and across
     * calls), instead of the last one processed. Empty = last-write-wins (the
     * historical behavior). For the SII `emitidas` table this is
     * `source_modify_at` (the SFTP file's modifyTime stamped by the transform),
     * so a reprocess of an older extract can never overwrite a newer one.
     */
    @Column({ type: 'varchar', length: 128, name: 'recency_field', default: '' })
    recencyField!: string;

    @Column({ type: 'jsonb', default: [] })
    columns!: TableColumnDef[];

    @Column({ type: 'jsonb', name: 'default_sort', nullable: true })
    defaultSort!: TableSortDef | null;

    /** Present when edited rows should be pushed back to an external source. */
    @Column({ type: 'jsonb', nullable: true })
    write!: WriteConfig | null;

    /**
     * Opt-in automatic purge: rows older than this many days are deleted by
     * `TableRetentionCron` (daily sweep, reuses `TableRowsService.deleteRows`).
     * Null/unset = no automatic retention (default — fiscal tables must opt in
     * explicitly; there is no implicit expiry of SII data).
     */
    @Column({ type: 'int', name: 'retention_days', nullable: true })
    retentionDays!: number | null;

    /** Gate for the mass delete-by-filter operation (`table.bulkDelete`) — off by default, must be explicitly enabled per table. */
    @Column({ type: 'boolean', name: 'allow_bulk_delete', default: false })
    allowBulkDelete!: boolean;

    @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
    createdAt!: Date;

    @UpdateDateColumn({ type: 'timestamptz', name: 'updated_at' })
    updatedAt!: Date;
}
