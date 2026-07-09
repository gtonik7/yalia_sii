import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

/**
 * Incremental watermark for an audited table, keyed by (tableKey, connectionId).
 * Holds the max `updatedAtField` seen so the next run only pulls newer records.
 * Re-runs reconcile by `idField` upsert, so a stale/overlapping watermark never
 * duplicates rows — it only risks re-fetching a few already-stored ones.
 *
 * Ordinary (non-hypertable) table — low cardinality, one row per audited
 * (table, connection) pair, mutated in place.
 */
@Entity('source_poll_states')
@Index(['tableKey', 'connectionId'], { unique: true })
export class SourcePollState {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ type: 'varchar', length: 128, name: 'table_key' })
  tableKey!: string;

  @Column({ type: 'varchar', length: 64, name: 'connection_id' })
  connectionId!: string;

  /** Highest `updatedAtField` value processed so far (ISO string). */
  @Column({ type: 'text', name: 'last_updated_at', nullable: true })
  lastUpdatedAt!: string | null;

  @Column({ type: 'int', name: 'total_seen', default: 0 })
  totalSeen!: number;

  @Column({ type: 'timestamptz', name: 'last_run_at', nullable: true })
  lastRunAt!: Date | null;

  @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
  createdAt!: Date;

  @UpdateDateColumn({ type: 'timestamptz', name: 'updated_at' })
  updatedAt!: Date;
}
