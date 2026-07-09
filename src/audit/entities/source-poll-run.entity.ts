import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

/**
 * Summary of one audit/polling execution against an external source. Lands in
 * the explorer as the `source-poll-runs` dataset so the operator can see what
 * each run fetched and upserted into the hot table.
 *
 * Physically a Timescale hypertable partitioned on `createdAt` with a fixed
 * 365-day retention policy (see the `SourcePollRunsTimescale` migration) —
 * the direct analog of Mongo's TTL index on this collection.
 */
@Entity('source_poll_runs')
@Index(['tableKey'])
@Index(['connectionId'])
export class SourcePollRun {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ type: 'varchar', length: 128, name: 'table_key' })
  tableKey!: string;

  @Column({ type: 'varchar', length: 64, name: 'connection_id' })
  connectionId!: string;

  @Column({ type: 'varchar', length: 256, name: 'connection_name', nullable: true })
  connectionName!: string | null;

  @Column({ type: 'varchar', length: 16 })
  trigger!: 'manual' | 'scheduled';

  @Column({ type: 'varchar', length: 16, default: 'running' })
  status!: 'running' | 'completed' | 'empty' | 'error';

  /** Effective `since` floor used (watermark) — null when full re-scan. */
  @Column({ type: 'text', nullable: true })
  since!: string | null;

  @Column({ type: 'int', default: 0 })
  pages!: number;

  /** Records seen across all pages. */
  @Column({ type: 'int', default: 0 })
  fetched!: number;

  @Column({ type: 'int', default: 0 })
  inserted!: number;

  @Column({ type: 'int', default: 0 })
  upserted!: number;

  @Column({ type: 'text', name: 'error_message', nullable: true })
  errorMessage!: string | null;

  @Column({ type: 'timestamptz', name: 'completed_at', nullable: true })
  completedAt!: Date | null;

  @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
  createdAt!: Date;
}
