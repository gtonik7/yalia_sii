import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

export type OperationRunStatus = 'queued' | 'running' | 'success' | 'error' | 'canceled';

/**
 * Historial + estado de una operación pesada ejecutada en background (borrado masivo,
 * count/stats/report exactos). El controller crea la fila `queued` y responde `{ runId }`
 * de inmediato; el `MaintenanceProcessor` la mueve a `running` y, al terminar, escribe
 * `result` (`success`) o `error`. El FE pollea esta fila en vez de bloquear en el trigger,
 * de modo que una operación que supere el techo de timeout del proxy del hub (30 s) ya no
 * deja al usuario sin saber el desenlace. Espejo de `BackupRun` para el dominio de tablas.
 */
@Entity('operation_runs')
export class OperationRun {
    @PrimaryGeneratedColumn('uuid')
    id!: string;

    /** Clave de la operación, p.ej. `table.bulkDelete`, `table.count`, `table.stats`, `table.report`. */
    @Index()
    @Column({ type: 'varchar', length: 64 })
    operation!: string;

    /** Tabla objetivo (para historial/filtrado); null si la operación no es por tabla. */
    @Index()
    @Column({ type: 'varchar', length: 128, name: 'table_key', nullable: true })
    tableKey!: string | null;

    @Index()
    @Column({ type: 'varchar', length: 16 })
    status!: OperationRunStatus;

    /** Parámetros del trigger (tableKey, connectionId, filters, groupBy, metrics, …). */
    @Column({ type: 'jsonb', default: {} })
    params!: Record<string, unknown>;

    /** Resultado de la operación al completar (deletedCount, count, stats, aggregate…). */
    @Column({ type: 'jsonb', nullable: true })
    result!: Record<string, unknown> | null;

    @Column({ type: 'text', nullable: true })
    error!: string | null;

    @Index()
    @Column({ type: 'timestamptz', name: 'created_at', default: () => 'now()' })
    createdAt!: Date;

    @Column({ type: 'timestamptz', name: 'started_at', nullable: true })
    startedAt!: Date | null;

    @Column({ type: 'timestamptz', name: 'finished_at', nullable: true })
    finishedAt!: Date | null;
}
