import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

/**
 * Destinos de un backup. Un backup puede escribirse a varios a la vez:
 * - `local`: el artefacto queda en `BACKUP_DIR` de forma persistente.
 * - `download`: el artefacto queda disponible para descarga bajo demanda por su
 *   `runId` (mismo fichero en disco; el flag solo documenta la intención).
 * - `email`: se envía como adjunto a `email.to` vía SMTP (MailService).
 */
export interface BackupDestinations {
    local?: boolean;
    download?: boolean;
    email?: { to: string[] } | null;
}

/**
 * Backup programado de la base de datos de yalia_sii. Opt-in: cada schedule elige
 * qué tablas, con qué frecuencia (cron) y a qué destinos. Ejecutado por
 * `BackupCron` (supervisor `setInterval`, mismo patrón que `TableRetentionCron`).
 * `tables` vacío = todas las tablas físicas backupeables.
 */
@Entity('backup_schedules')
export class BackupSchedule {
    @PrimaryGeneratedColumn('uuid')
    id!: string;

    @Column({ type: 'varchar', length: 256 })
    name!: string;

    /** Keys de tabla (nombres físicos en el schema `public`). Vacío = todas. */
    @Column({ type: 'jsonb', default: [] })
    tables!: string[];

    /** Expresión cron estándar (5 o 6 campos), evaluada por `BackupCron`. */
    @Column({ type: 'varchar', length: 128, name: 'cron_expression' })
    cronExpression!: string;

    @Column({ type: 'jsonb', default: {} })
    destinations!: BackupDestinations;

    /** Número de artefactos a conservar por schedule; los más antiguos se purgan. */
    @Column({ type: 'int', name: 'retention_count', default: 7 })
    retentionCount!: number;

    @Column({ type: 'boolean', default: true })
    enabled!: boolean;

    /** Instante de la última ejecución (para detectar slots cron pendientes). */
    @Column({ type: 'timestamptz', name: 'last_run_at', nullable: true })
    lastRunAt!: Date | null;

    @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
    createdAt!: Date;

    @UpdateDateColumn({ type: 'timestamptz', name: 'updated_at' })
    updatedAt!: Date;
}
