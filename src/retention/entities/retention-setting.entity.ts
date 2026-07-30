import { Column, Entity, PrimaryColumn, UpdateDateColumn } from 'typeorm';

/**
 * Config de retención por target físico del satélite (tablas que NO son `table_rows`:
 * `table_delete_events`, `domain_event_outbox`, `table_write_runs`). `table_rows`
 * mantiene su retención en `TableTemplate.retentionDays` (por plantilla) y no vive aquí.
 *
 * Una fila por target; si no existe, se aplican los defaults del catálogo. Editable en
 * caliente desde la pestaña "Retención" del satélite.
 */
@Entity('retention_settings')
export class RetentionSetting {
    /** Clave del target (ver RETENTION_CATALOG del satélite). */
    @PrimaryColumn({ type: 'varchar', length: 64, name: 'target_key' })
    targetKey!: string;

    @Column({ type: 'int', name: 'retention_days' })
    retentionDays!: number;

    /** Cadencia del barrido automático (días). */
    @Column({ type: 'int', name: 'interval_days', default: 1 })
    intervalDays!: number;

    @Column({ type: 'boolean', default: true })
    enabled!: boolean;

    /** Último barrido correcto (para la cuenta atrás). */
    @Column({ type: 'timestamptz', name: 'last_run_at', nullable: true })
    lastRunAt!: Date | null;

    @UpdateDateColumn({ type: 'timestamptz', name: 'updated_at' })
    updatedAt!: Date;
}
