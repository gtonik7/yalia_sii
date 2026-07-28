import { createReadStream } from 'fs';
import { mkdir, stat, unlink } from 'fs/promises';
import { join } from 'path';
import { Readable } from 'stream';
import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { InjectQueue } from '@nestjs/bullmq';
import { DataSource, Repository } from 'typeorm';
import type { Queue } from 'bullmq';
import type { Env } from '../config/env';
import { DEFAULT_JOB_OPTS, QUEUES } from '../core/queues/queues.constants';
import { BackupSchedule, type BackupDestinations } from './entities/backup-schedule.entity';
import { BackupRun, type BackupRunTrigger } from './entities/backup-run.entity';
import { MailService } from './mail.service';
import { dumpTables, type PgConn } from './pg-tools';
import type { BackupJobData } from './backup-job.types';

export interface BackupTableInfo {
    name: string;
    sizeBytes: number;
}

export interface RunBackupOpts {
    scheduleId?: string | null;
    trigger: BackupRunTrigger;
    tables: string[];
    destinations: BackupDestinations;
    retentionCount?: number;
}

@Injectable()
export class BackupService {
    private readonly logger = new Logger(BackupService.name);

    constructor(
        @InjectDataSource() private readonly ds: DataSource,
        @InjectRepository(BackupSchedule) private readonly schedules: Repository<BackupSchedule>,
        @InjectRepository(BackupRun) private readonly runs: Repository<BackupRun>,
        private readonly config: ConfigService<Env, true>,
        private readonly mail: MailService,
        @InjectQueue(QUEUES.BACKUP) private readonly queue: Queue<BackupJobData>,
    ) {}

    private get backupDir(): string {
        return this.config.get('BACKUP_DIR', { infer: true });
    }

    private pgConn(): PgConn {
        return {
            host: this.config.get('DB_HOST', { infer: true }),
            port: this.config.get('DB_PORT', { infer: true }),
            user: this.config.get('DB_USER', { infer: true }),
            password: this.config.get('DB_PASSWORD', { infer: true }),
            database: this.config.get('DB_NAME', { infer: true }),
        };
    }

    // ── Tablas backupeables ───────────────────────────────────────────────────
    async listTables(): Promise<BackupTableInfo[]> {
        const rows = await this.ds.query<{ name: string; size: string }[]>(
            `SELECT tablename AS name, pg_total_relation_size(quote_ident(tablename)) AS size
               FROM pg_tables
              WHERE schemaname = 'public'
                AND tablename NOT IN ('migrations', 'typeorm_metadata')
              ORDER BY tablename`,
        );
        return rows.map((r) => ({ name: r.name, sizeBytes: Number(r.size) }));
    }

    // ── Schedules CRUD ─────────────────────────────────────────────────────────
    listSchedules(): Promise<BackupSchedule[]> {
        return this.schedules.find({ order: { createdAt: 'DESC' } });
    }

    async getSchedule(id: string): Promise<BackupSchedule> {
        const s = await this.schedules.findOne({ where: { id } });
        if (!s) throw new NotFoundException(`Schedule '${id}' no encontrado`);
        return s;
    }

    createSchedule(data: Partial<BackupSchedule>): Promise<BackupSchedule> {
        return this.schedules.save(this.schedules.create(data));
    }

    async updateSchedule(id: string, data: Partial<BackupSchedule>): Promise<BackupSchedule> {
        const existing = await this.schedules.findOne({ where: { id } });
        if (!existing) throw new NotFoundException(`Schedule '${id}' no encontrado`);
        Object.assign(existing, data);
        return this.schedules.save(existing);
    }

    async deleteSchedule(id: string): Promise<void> {
        await this.schedules.delete(id);
    }

    // ── Runs / artefactos ──────────────────────────────────────────────────────
    listRuns(limit = 100): Promise<BackupRun[]> {
        return this.runs.find({ order: { startedAt: 'DESC' }, take: limit });
    }

    async deleteRun(id: string): Promise<void> {
        const run = await this.runs.findOne({ where: { id } });
        if (!run) return;
        await this.safeUnlink(run.filePath);
        await this.runs.delete(id);
    }

    /** Stream del artefacto de un run para descarga. Lanza 404 si ya no existe. */
    async getArtifact(id: string): Promise<{ stream: Readable; fileName: string; sizeBytes: number }> {
        const run = await this.runs.findOne({ where: { id } });
        if (!run || !run.filePath || !run.fileName) throw new NotFoundException('El artefacto de backup ya no está disponible.');
        let sizeBytes = run.sizeBytes ?? 0;
        try {
            sizeBytes = (await stat(run.filePath)).size;
        } catch {
            throw new NotFoundException('El fichero de backup ya no existe en disco.');
        }
        return { stream: createReadStream(run.filePath), fileName: run.fileName, sizeBytes };
    }

    // ── Ejecución de un backup ─────────────────────────────────────────────────
    /**
     * Crea el `BackupRun` (`running`) y encola el pg_dump en la cola BACKUP, devolviendo
     * el run de inmediato. Usado por el endpoint HTTP para no bloquear el request hasta
     * que termine el dump (que puede superar de largo el techo de timeout del proxy del
     * hub). El FE pollea `/backups/runs`. El cron (`BackupCron`) usa `runBackup` inline.
     */
    async enqueueBackup(opts: RunBackupOpts): Promise<BackupRun> {
        const run = await this.createRun(opts);
        await this.queue.add('backup', { kind: 'backup', runId: run.id, opts }, { ...DEFAULT_JOB_OPTS, jobId: `backup-${run.id}`, attempts: 1 });
        return run;
    }

    /** Ejecuta por runId un backup ya creado — punto de entrada del processor. */
    async executeBackupById(runId: string, opts: RunBackupOpts): Promise<void> {
        const run = await this.runs.findOne({ where: { id: runId } });
        if (!run) {
            this.logger.warn(`executeBackupById: run ${runId} no encontrado`);
            return;
        }
        await this.executeBackup(run, opts);
    }

    /** Crea la fila `BackupRun` en estado `running` con el snapshot de tablas. */
    private async createRun(opts: RunBackupOpts): Promise<BackupRun> {
        const allTables = (await this.listTables()).map((t) => t.name);
        const tables = opts.tables?.length ? opts.tables.filter((t) => allTables.includes(t)) : allTables;
        return this.runs.save(
            this.runs.create({
                scheduleId: opts.scheduleId ?? null,
                trigger: opts.trigger,
                status: 'running',
                tables,
                startedAt: new Date(),
            }),
        );
    }

    /** Backup síncrono completo (crear + ejecutar). Lo usa el cron; el HTTP usa `enqueueBackup`. */
    async runBackup(opts: RunBackupOpts): Promise<BackupRun> {
        const run = await this.createRun(opts);
        return this.executeBackup(run, opts);
    }

    /** Ejecuta el pg_dump sobre un `BackupRun` ya creado y actualiza su desenlace. */
    private async executeBackup(run: BackupRun, opts: RunBackupOpts): Promise<BackupRun> {
        await mkdir(this.backupDir, { recursive: true });
        const tables = run.tables;

        const ts = new Date().toISOString().replace(/[:.]/g, '-').replace('T', '_').slice(0, 19);
        const fileName = `sii_${ts}.siibak`;
        const filePath = join(this.backupDir, fileName);
        const destResult: Record<string, string> = {};

        try {
            await dumpTables(this.pgConn(), tables, filePath);
            const sizeBytes = (await stat(filePath)).size;

            const dest = opts.destinations ?? {};
            if (dest.email?.to?.length) {
                try {
                    await this.mail.send({
                        to: dest.email.to,
                        subject: `Backup yalia_sii ${fileName}`,
                        text: `Backup de la base de datos de yalia_sii.\nTablas: ${tables.join(', ')}\nTamaño: ${sizeBytes} bytes`,
                        attachments: [{ filename: fileName, path: filePath }],
                    });
                    destResult.email = 'sent';
                } catch (err) {
                    destResult.email = `error: ${this.msg(err)}`;
                    this.logger.warn(`Backup email falló: ${this.msg(err)}`);
                }
            }
            if (dest.local) destResult.local = 'ok';
            if (dest.download) destResult.download = 'ok';

            // Si el único destino era email, no se conserva el fichero en disco.
            const keepFile = Boolean(dest.local || dest.download);
            if (!keepFile) await this.safeUnlink(filePath);

            run.status = 'success';
            run.sizeBytes = sizeBytes;
            run.fileName = keepFile ? fileName : null;
            run.filePath = keepFile ? filePath : null;
            run.destinationsResult = destResult;
            run.finishedAt = new Date();
            await this.runs.save(run);

            if (opts.scheduleId) {
                await this.schedules.update(opts.scheduleId, { lastRunAt: new Date() });
                await this.applyRetention(opts.scheduleId, opts.retentionCount ?? 7);
            }
            this.logger.log(`Backup ok run=${run.id} tables=${tables.length} size=${sizeBytes}`);
            return run;
        } catch (err) {
            await this.safeUnlink(filePath);
            run.status = 'error';
            run.error = this.msg(err);
            run.destinationsResult = destResult;
            run.finishedAt = new Date();
            await this.runs.save(run);
            this.logger.error(`Backup falló run=${run.id}: ${this.msg(err)}`);
            return run;
        }
    }

    /** Conserva los `retentionCount` runs con fichero más recientes; purga el resto. */
    private async applyRetention(scheduleId: string, retentionCount: number): Promise<void> {
        if (retentionCount <= 0) return;
        const withFile = await this.runs.find({
            where: { scheduleId, status: 'success' },
            order: { startedAt: 'DESC' },
        });
        const stale = withFile.filter((r) => r.filePath).slice(retentionCount);
        for (const r of stale) {
            await this.safeUnlink(r.filePath);
            r.filePath = null;
            r.fileName = null;
            await this.runs.save(r);
        }
    }

    private async safeUnlink(path: string | null): Promise<void> {
        if (!path) return;
        try {
            await unlink(path);
        } catch {
            /* ya no existe: no-op */
        }
    }

    private msg(err: unknown): string {
        return err instanceof Error ? err.message : String(err);
    }
}
