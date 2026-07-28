import { writeFile, mkdir, unlink } from 'fs/promises';
import { join } from 'path';
import { BadRequestException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { InjectQueue } from '@nestjs/bullmq';
import { DataSource, Repository } from 'typeorm';
import type { Queue } from 'bullmq';
import type { Env } from '../config/env';
import { DEFAULT_JOB_OPTS, QUEUES } from '../core/queues/queues.constants';
import { BackupRun } from './entities/backup-run.entity';
import { OperationRun } from '../tables/entities/operation-run.entity';
import { restoreTables, type PgConn } from './pg-tools';
import type { BackupJobData } from './backup-job.types';

export interface RestoreOpts {
    /** Restaurar desde un artefacto ya almacenado (por su runId)… */
    runId?: string;
    /** …o desde un fichero `.siibak` subido (base64). Uno de los dos. */
    uploadBase64?: string;
    /** Tablas a restaurar. Vacío = todas las del artefacto. */
    tables?: string[];
    /** 'replace' hace TRUNCATE previo; 'append' sólo inserta. */
    mode: 'replace' | 'append';
    /** Confirmación explícita — operación destructiva. */
    confirm: boolean;
}

/** Params persistidos en el `operation_run` de un restore (sin base64: el fichero ya está en disco). */
interface RestoreRunParams {
    tables: string[];
    mode: 'replace' | 'append';
    /** Fichero temporal del upload, a limpiar tras restaurar. */
    tempPath?: string;
    /** Artefacto de un backup almacenado (BackupRun.id). */
    artifactRunId?: string;
}

/**
 * Restauración (pg_restore) ejecutada en background. El request valida `confirm` y prepara
 * el fichero, crea un `operation_run` (`operation:'backup.restore'`) y encola el trabajo en
 * la cola BACKUP, devolviendo `{ runId }`. El FE pollea `GET /v1/operation-runs/:runId`.
 * Antes era síncrono con timeout de 300 s, que bloqueaba el request y era frágil.
 */
@Injectable()
export class RestoreService {
    private readonly logger = new Logger(RestoreService.name);

    constructor(
        @InjectDataSource() private readonly ds: DataSource,
        @InjectRepository(BackupRun) private readonly runs: Repository<BackupRun>,
        @InjectRepository(OperationRun) private readonly opRuns: Repository<OperationRun>,
        private readonly config: ConfigService<Env, true>,
        @InjectQueue(QUEUES.BACKUP) private readonly queue: Queue<BackupJobData>,
    ) {}

    private pgConn(): PgConn {
        return {
            host: this.config.get('DB_HOST', { infer: true }),
            port: this.config.get('DB_PORT', { infer: true }),
            user: this.config.get('DB_USER', { infer: true }),
            password: this.config.get('DB_PASSWORD', { infer: true }),
            database: this.config.get('DB_NAME', { infer: true }),
        };
    }

    /**
     * Valida, materializa el fichero a restaurar y encola el pg_restore. El base64 subido
     * se escribe a disco AQUÍ (no se guarda en la fila) para no inflar el `operation_run`
     * ni la memoria; sólo se difiere la parte pesada (pg_restore).
     */
    async enqueueRestore(opts: RestoreOpts): Promise<OperationRun> {
        if (!opts.confirm) throw new BadRequestException('La restauración requiere confirmación explícita (confirm=true).');

        const params: RestoreRunParams = { tables: opts.tables ?? [], mode: opts.mode };
        if (opts.runId) {
            const artifact = await this.runs.findOne({ where: { id: opts.runId } });
            if (!artifact || !artifact.filePath) throw new NotFoundException('El artefacto de backup ya no está disponible.');
            params.artifactRunId = opts.runId;
        } else if (opts.uploadBase64) {
            const dir = join(this.config.get('BACKUP_DIR', { infer: true }), 'restore-tmp');
            await mkdir(dir, { recursive: true });
            const tempPath = join(dir, `upload_${Date.now()}.siibak`);
            await writeFile(tempPath, Buffer.from(opts.uploadBase64, 'base64'));
            params.tempPath = tempPath;
        } else {
            throw new BadRequestException('Indica runId o uploadBase64.');
        }

        const run = await this.opRuns.save(
            this.opRuns.create({ operation: 'backup.restore', tableKey: null, status: 'queued', params: params as unknown as Record<string, unknown>, result: null, error: null }),
        );
        await this.queue.add('restore', { kind: 'restore', runId: run.id }, { ...DEFAULT_JOB_OPTS, jobId: `restore-${run.id}`, attempts: 1 });
        return run;
    }

    /** Ejecuta el pg_restore de un `operation_run` ya encolado — punto de entrada del processor. */
    async executeRestore(runId: string): Promise<void> {
        const run = await this.opRuns.findOne({ where: { id: runId } });
        if (!run || run.status === 'canceled') return;
        await this.opRuns.save({ id: runId, status: 'running', startedAt: new Date() });

        const params = run.params as unknown as RestoreRunParams;
        try {
            const result = await this.doRestore(params);
            await this.opRuns.save({ id: runId, status: 'success', result: result as unknown as Record<string, unknown>, finishedAt: new Date() });
            this.logger.log(`Restore ok run=${runId} tables=${result.tables.length ? result.tables.join(', ') : 'ALL'} mode=${result.mode}`);
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            await this.opRuns.save({ id: runId, status: 'error', error: message, finishedAt: new Date() });
            this.logger.error(`Restore falló run=${runId}: ${message}`);
        }
    }

    /** Resuelve el fichero, hace el TRUNCATE opcional + pg_restore, y limpia el temporal. */
    private async doRestore(params: RestoreRunParams): Promise<{ restored: boolean; tables: string[]; mode: string }> {
        const tables = params.tables ?? [];
        let filePath: string;
        if (params.artifactRunId) {
            const run = await this.runs.findOne({ where: { id: params.artifactRunId } });
            if (!run || !run.filePath) throw new NotFoundException('El artefacto de backup ya no está disponible.');
            filePath = run.filePath;
        } else if (params.tempPath) {
            filePath = params.tempPath;
        } else {
            throw new BadRequestException('Restore sin fichero de origen.');
        }

        try {
            if (params.mode === 'replace' && tables.length) {
                const idents = tables.map((t) => `"${t.replace(/"/g, '')}"`).join(', ');
                await this.ds.query(`TRUNCATE TABLE ${idents} RESTART IDENTITY CASCADE;`);
                this.logger.warn(`Restore: TRUNCATE ${tables.join(', ')}`);
            }
            await restoreTables(this.pgConn(), tables, filePath);
            return { restored: true, tables, mode: params.mode };
        } finally {
            if (params.tempPath) {
                try {
                    await unlink(params.tempPath);
                } catch {
                    /* no-op */
                }
            }
        }
    }
}
