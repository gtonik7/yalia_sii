import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';
import { DatasetColumnType } from '../../datasets/dataset.types';

export interface TableColumnDef {
  key: string;
  label: string;
  type: DatasetColumnType;
  filterable?: boolean;
  sortable?: boolean;
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
 * Binds a table to a source connection so an edited row is pushed back to the
 * external system. The endpoint lives here (per table); auth lives on the
 * connection (per API).
 */
export interface WriteConfig {
  /**
   * Default source connection id (source_connections) this table pushes
   * edits to. Each group of queued rows is actually submitted through the
   * connection it was *ingested* under (see `TableRowsService.submitGroup`)
   * — this field is only the fallback used when that can't be determined.
   */
  connectionId: string;
  method: 'PUT' | 'PATCH' | 'POST';
  /**
   * Path appended to the connection baseUrl. `{id}` is replaced with
   * data[idField] (falling back to the row's internal id when idField is
   * unset or the value is missing).
   */
  path: string;
  /** Static query params merged into every write request. */
  query?: Record<string, string>;
  /** Dot-path in the response body to pluck an external reference (optional). */
  externalRefPath?: string;
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

  @Column({ type: 'jsonb', default: [] })
  columns!: TableColumnDef[];

  @Column({ type: 'jsonb', name: 'default_sort', nullable: true })
  defaultSort!: TableSortDef | null;

  /** Present when edited rows should be pushed back to an external source. */
  @Column({ type: 'jsonb', nullable: true })
  write!: WriteConfig | null;

  @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
  createdAt!: Date;

  @UpdateDateColumn({ type: 'timestamptz', name: 'updated_at' })
  updatedAt!: Date;
}
