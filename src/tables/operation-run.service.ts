import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { InjectQueue } from '@nestjs/bullmq';
import { In, Repository } from 'typeorm';
import type { Queue } from 'bullmq';
import { DEFAULT_JOB_OPTS, QUEUES } from '../core/queues/queues.constants';
import { OperationRun } from './entities/operation-run.entity';

export interface MaintenanceJobData {
    runId: string;
}

/**
 * Alta y transiciones de estado de una operación pesada async, más el encolado del
 * job de mantenimiento que la ejecuta. El request del trigger sólo llega hasta
 * `create()` (crea `queued` + encola) y responde `{ runId }`; el resto de transiciones
 * las hace el `MaintenanceProcessor`.
 */
@Injectable()
export class OperationRunService {
    constructor(
        @InjectRepository(OperationRun) private readonly repo: Repository<OperationRun>,
        @InjectQueue(QUEUES.MAINTENANCE) private readonly queue: Queue<MaintenanceJobData>,
    ) {}

    /** Crea el run `queued` y encola su ejecución. `jobId = runId` deduplica dobles clics. */
    async create(operation: string, tableKey: string | null, params: Record<string, unknown>): Promise<OperationRun> {
        const run = await this.repo.save(
            this.repo.create({ operation, tableKey, params, status: 'queued', result: null, error: null }),
        );
        // attempts: 1 — una operación pesada (borrado/scan) NO debe reintentarse
        // sola; su desenlace queda en la fila y el usuario puede relanzarla a mano.
        await this.queue.add('run', { runId: run.id }, { ...DEFAULT_JOB_OPTS, jobId: run.id, attempts: 1 });
        return run;
    }

    async get(runId: string): Promise<OperationRun> {
        const run = await this.repo.findOne({ where: { id: runId } });
        if (!run) throw new NotFoundException(`Operation run ${runId} not found`);
        return run;
    }

    /** Devuelve null en vez de lanzar — para el processor, que debe no-opear si el run desapareció. */
    async find(runId: string): Promise<OperationRun | null> {
        return this.repo.findOne({ where: { id: runId } });
    }

    /** Run activo (queued/running) de una operación+tabla, si lo hay — gate del scheduler. */
    async findActiveByOperation(operation: string, tableKey: string): Promise<OperationRun | null> {
        return this.repo.findOne({
            where: { operation, tableKey, status: In(['queued', 'running']) },
            order: { createdAt: 'DESC' },
        });
    }

    async markRunning(runId: string): Promise<void> {
        // save (no update) para no chocar con el tipado estricto de QueryDeepPartialEntity
        // sobre las columnas jsonb; el partial con id hace un UPDATE igualmente.
        await this.repo.save({ id: runId, status: 'running', startedAt: new Date() });
    }

    /** Fija el total y la fase al arrancar una operación con progreso (retención). */
    async startProgress(runId: string, total: number, phase = 'deleting'): Promise<void> {
        await this.repo.save({ id: runId, total, processed: 0, progress: 0, phase });
    }

    /** Empuja el avance de una operación larga (llamado cada lote). */
    async updateProgress(runId: string, processed: number, total: number): Promise<void> {
        const progress = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;
        await this.repo.save({ id: runId, processed, total, progress });
    }

    async markSuccess(runId: string, result: Record<string, unknown>): Promise<void> {
        await this.repo.save({ id: runId, status: 'success', result, progress: 100, phase: 'done', finishedAt: new Date() });
    }

    async markError(runId: string, error: string): Promise<void> {
        await this.repo.save({ id: runId, status: 'error', error, finishedAt: new Date() });
    }

    /** Marca cancelado y elimina el job pendiente si aún no empezó. */
    async cancel(runId: string): Promise<OperationRun> {
        const run = await this.get(runId);
        if (run.status === 'queued' || run.status === 'running') {
            await this.repo.save({ id: runId, status: 'canceled', finishedAt: new Date() });
            const job = await this.queue.getJob(runId);
            if (job) await job.remove().catch(() => undefined);
        }
        return this.get(runId);
    }
}
