import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

/**
 * A single row pushed into a table. `data` is the opaque payload object; columns
 * declared by the template are read from it (data->>'<columnKey>') for filter/sort.
 *
 * Deliberately NOT a Timescale hypertable — TableIndexManagerService needs a
 * genuine Postgres unique index per template's `idField` (the ON CONFLICT
 * arbiter for ingest() upserts), and TimescaleDB requires every unique index
 * on a hypertable to include the partitioning column, which would defeat the
 * whole point of that index. See `TableRowsSearchIndex` migration for the
 * full story. Plain Postgres table with regular b-tree/GIN/expression
 * indexes instead — same pattern as every non-`audit_events` table in
 * yalia_hub. The generated `search_vector` column used for free-text search
 * is deliberately NOT declared here: it's created purely by migration DDL
 * and only ever touched via raw SQL.
 */
@Entity('table_rows')
@Index(['tableKey', 'connectionId', 'createdAt'])
@Index(['tableKey', 'connectionId', 'externalRef'], { where: '"externalRef" IS NOT NULL' })
export class TableRow {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  /** Template key this row belongs to. */
  @Column({ type: 'varchar', length: 128, name: 'table_key' })
  tableKey!: string;

  /** Tenant scope: the connection this row was ingested under. */
  @Column({ type: 'varchar', length: 64, name: 'connection_id', default: '' })
  connectionId!: string;

  @Column({ type: 'jsonb', default: {} })
  data!: Record<string, unknown>;

  /** traceId of the flow hop that delivered this row (audit/debug). */
  @Column({ type: 'varchar', length: 64, name: 'trace_id', nullable: true })
  traceId!: string | null;

  /** Outcome of the last write-back attempt to the external system (see WriteConfig). */
  @Column({ type: 'varchar', length: 8, name: 'write_status', nullable: true })
  writeStatus!: 'sent' | 'error' | null;

  @Column({ type: 'text', name: 'write_error', nullable: true })
  writeError!: string | null;

  @Column({ type: 'timestamptz', name: 'last_written_at', nullable: true })
  lastWrittenAt!: Date | null;

  /** Reference plucked from the external system's response (via externalRefPath), if any. Also the callback's correlation key. */
  @Column({ type: 'text', name: 'external_ref', nullable: true })
  externalRef!: string | null;

  /**
   * Real SII outcome of the last submission attempt (distinct from
   * `writeStatus`, which is only the transport ack of the outbound call).
   * `null` = not applicable (no `write` configured, or never submitted).
   * `'queued'` = edited/ingested, pending a sweep to send. `'pending'` = sent
   * with a provider ACK, awaiting SII's real result. Terminal values
   * (success literal TBD with the provider) land here via the inbound
   * callback, correlated by `externalRef`.
   */
  @Column({ type: 'varchar', length: 16, name: 'submission_status', nullable: true })
  submissionStatus!: string | null;

  /** Outbound batch this row was last sent in — trazability only, never used to decide a per-row result. */
  @Column({ type: 'varchar', length: 64, name: 'batch_id', nullable: true })
  batchId!: string | null;

  /** Last raw callback payload for this row; overwritten wholesale on every callback (no history kept). */
  @Column({ type: 'jsonb', name: 'sii_response', nullable: true })
  siiResponse!: Record<string, unknown> | null;

  @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
  createdAt!: Date;

  /**
   * Última modificación de la fila (edición, re-ingesta o cambio de estado de
   * presentación). La mantiene un trigger `BEFORE UPDATE` en Postgres, no
   * TypeORM: todas las escrituras del módulo son SQL crudo (por eso es un
   * `@Column` plano, no `@UpdateDateColumn`). Ver migración TableRowsUpdatedAt.
   */
  @Column({ type: 'timestamptz', name: 'updated_at' })
  updatedAt!: Date;
}
