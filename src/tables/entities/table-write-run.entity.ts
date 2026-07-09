import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

/**
 * Summary of one outbound submission batch (one `submitGroup()` call = one HTTP
 * request to the external system for a group/chunk of `queued` rows). The
 * write-side analog of `SourcePollRun`: it lands in the explorer as the
 * `table-write-runs` dataset so the operator can see, per table, what was
 * submitted, when, by which trigger, and whether the provider ACKed it.
 *
 * Unlike a poll run there is no live "running" phase to track: a batch is a
 * single awaited HTTP call, so the row is written once with its terminal
 * outcome. `status='sent'` means the provider ACKed (2xx) and the rows moved to
 * `submission_status='pending'` awaiting the async AEAT callback; `status='error'`
 * means a non-2xx/transport failure put them back to `queued` for retry.
 *
 * Kept a plain table (not a Timescale hypertable like `source_poll_runs`) — the
 * volume is one row per outbound batch and a plain btree on `created_at` is
 * enough; retention, if ever needed, is a follow-up migration.
 */
@Entity('table_write_runs')
@Index(['tableKey'])
@Index(['createdAt'])
export class TableWriteRun {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ type: 'varchar', length: 128, name: 'table_key' })
  tableKey!: string;

  @Column({ type: 'varchar', length: 64, name: 'connection_id', nullable: true })
  connectionId!: string | null;

  @Column({ type: 'varchar', length: 256, name: 'connection_name', nullable: true })
  connectionName!: string | null;

  /** How the sweep that produced this batch was invoked. */
  @Column({ type: 'varchar', length: 16 })
  trigger!: 'event' | 'schedule' | 'manual';

  @Column({ type: 'varchar', length: 16 })
  status!: 'sent' | 'error';

  /** The batch id stamped on every row of this submission (`table_rows.batch_id`). */
  @Column({ type: 'varchar', length: 64, name: 'batch_id', nullable: true })
  batchId!: string | null;

  /** The batch-group tuple (write.batch.groupBy → value) this submission covered. */
  @Column({ type: 'jsonb', name: 'group_values', nullable: true })
  groupValues!: Record<string, string> | null;

  /** Rows included in this outbound batch. */
  @Column({ type: 'int', name: 'row_count', default: 0 })
  rowCount!: number;

  /** HTTP status the external system replied with (null on transport failure). */
  @Column({ type: 'int', name: 'http_status', nullable: true })
  httpStatus!: number | null;

  @Column({ type: 'text', name: 'error_message', nullable: true })
  errorMessage!: string | null;

  @Column({ type: 'timestamptz', name: 'completed_at', nullable: true })
  completedAt!: Date | null;

  @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
  createdAt!: Date;
}
