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
}

/**
 * Binds one source connection to the endpoint its rows are pushed back to.
 * Method/path/query are per-connection (not a shared base) because the same
 * table can be exposed on connections whose external systems expect
 * different endpoints.
 */
export interface WriteConnectionRule {
    connectionId: string;
    method: 'PUT' | 'PATCH' | 'POST';
    /**
     * Path appended to the connection baseUrl. `{id}` is replaced with
     * data[idField] (falling back to the row's internal id when idField is
     * unset or the value is missing).
     */
    path: string;
    /** Static query params merged into every write request for this connection. */
    query?: Record<string, string>;
}

/**
 * Binds a table to source connections so an edited row is pushed back to the
 * external system. A row ingested under a connection with no matching rule
 * in `connections` is rejected before it's saved
 * (`TableRowsService.updateAndWrite`) or sent (`TableRowsService.submitGroup`)
 * — there is no implicit fallback connection or endpoint.
 */
export interface WriteConfig {
    /**
     * What enqueues a submission sweep: `'event'` debounces a sweep right after
     * each insert/edit; `'schedule'` relies solely on the hub calling
     * `table.write.batchSubmit` on a cron. One mode per template — both funnel
     * into the same submission core, never two parallel code paths.
     * Optional at this type level only for templates predating this field
     * (migration backfills `'event'` on save/read, but code consuming this
     * directly should still treat a missing value as `'event'`); the DTO layer
     * (`WriteConfigDto`) requires it on every create/update.
     */
    trigger?: 'event' | 'schedule';
    /** One rule per allowed connection — configured one by one, no shared base. */
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
     * Restricts which connections expose this table in the explorer (the
     * connection picker is limited to these ids). Empty/null = exposed on
     * every connection. Every table's rows are always scoped by connectionId.
     */
    @Column({ type: 'jsonb', name: 'connection_ids', nullable: true })
    connectionIds!: string[] | null;

    /**
     * Column key that uniquely identifies a row. When set, ingest upserts by it
     * (scoped to connectionId) instead of always inserting. Empty = append-only.
     */
    @Column({ type: 'varchar', length: 128, name: 'id_field', default: '' })
    idField!: string;

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
