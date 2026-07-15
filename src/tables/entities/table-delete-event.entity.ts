import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn } from 'typeorm';

/**
 * Append-only ledger of rows deleted *by the app itself* — every delete funnels
 * through `TableRowsService.deleteRows()` (bulk delete, delete-by-ids, retention
 * purge), which records one event here. Reconciliation subtracts the running sum
 * of `affected` from the physical table's global delete counter
 * (`pg_stat_user_tables.n_tup_del`) to tell voluntary deletions apart from
 * uncontrolled loss, so an intentional bulk delete no longer trips the warning.
 * The `'baseline'` seed row (see TableDeleteEvents migration) folds all
 * pre-feature deletions into "voluntary" so the counter starts reconciled.
 *
 * Written/read via raw SQL like the rest of TableRowsService; declared as an
 * entity only so a future `migration:generate` doesn't flag the table for DROP.
 */
@Entity('table_delete_events')
export class TableDeleteEvent {
    @PrimaryGeneratedColumn('uuid')
    id!: string;

    /** Template key the rows belonged to, or `'*'` for the global baseline seed. */
    @Column({ type: 'varchar', length: 128, name: 'table_key' })
    tableKey!: string;

    @Column({ type: 'varchar', length: 64, name: 'connection_id', nullable: true })
    connectionId!: string | null;

    /** Rows removed by this delete (or the baseline n_tup_del snapshot). */
    @Column({ type: 'int' })
    affected!: number;

    @Column({ type: 'varchar', length: 16 })
    reason!: 'bulk' | 'ids' | 'retention' | 'baseline';

    /** Business-key (`idField`) values actually removed by this event, when the template has one configured. Null for events recorded before this column existed, or for tables without an idField. */
    @Column({ type: 'text', array: true, name: 'record_ids', nullable: true })
    recordIds!: string[] | null;

    @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
    createdAt!: Date;
}
