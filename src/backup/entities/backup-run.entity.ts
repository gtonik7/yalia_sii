import { Column, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';

export type BackupRunStatus = 'running' | 'success' | 'error';

/** Origen de un run: disparado a mano ("backup ahora") o por el schedule. */
export type BackupRunTrigger = 'manual' | 'schedule';

const bigintToNumber = {
    to: (v: number | null) => v,
    from: (v: string | null) => (v == null ? null : Number(v)),
};

/**
 * Historial de ejecuciones de backup y registro del artefacto producido. Un run
 * de un backup manual tiene `scheduleId` null. `filePath` apunta al `.siibak` en
 * disco mientras el artefacto siga disponible (destinos local/download); se pone
 * a null cuando el fichero se purga por retención o cuando el único destino era
 * email y ya se envió.
 */
@Entity('backup_runs')
export class BackupRun {
    @PrimaryGeneratedColumn('uuid')
    id!: string;

    @Index()
    @Column({ type: 'uuid', name: 'schedule_id', nullable: true })
    scheduleId!: string | null;

    @Column({ type: 'varchar', length: 16 })
    status!: BackupRunStatus;

    @Column({ type: 'varchar', length: 16, default: 'manual' })
    trigger!: BackupRunTrigger;

    /** Snapshot de las tablas incluidas en este backup. */
    @Column({ type: 'jsonb', default: [] })
    tables!: string[];

    @Column({ type: 'bigint', name: 'size_bytes', nullable: true, transformer: bigintToNumber })
    sizeBytes!: number | null;

    @Column({ type: 'varchar', length: 256, name: 'file_name', nullable: true })
    fileName!: string | null;

    @Column({ type: 'text', name: 'file_path', nullable: true })
    filePath!: string | null;

    @Column({ type: 'text', nullable: true })
    error!: string | null;

    /** Resultado por destino, p.ej. `{ email: 'sent', local: 'ok' }`. */
    @Column({ type: 'jsonb', name: 'destinations_result', nullable: true })
    destinationsResult!: Record<string, string> | null;

    @Index()
    @Column({ type: 'timestamptz', name: 'started_at' })
    startedAt!: Date;

    @Column({ type: 'timestamptz', name: 'finished_at', nullable: true })
    finishedAt!: Date | null;
}
