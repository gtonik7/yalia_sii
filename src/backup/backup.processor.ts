import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { BackupService } from './backup.service';
import { RestoreService } from './restore.service';
import type { BackupJobData } from './backup-job.types';

/**
 * Ejecuta pg_dump/pg_restore en background para no bloquear el request HTTP (que antes
 * dependía de timeouts de 130 s/300 s en el proxy del hub y en el FE). El estado de un
 * backup vive en `backup_runs`; el de un restore, en `operation_runs`. Concurrency 1:
 * dump/restore de la misma BD no deben solaparse.
 */
@Processor(QUEUES.BACKUP, { concurrency: 1 })
export class BackupProcessor extends WorkerHost {
    private readonly logger = new Logger(BackupProcessor.name);

    constructor(
        private readonly backup: BackupService,
        private readonly restore: RestoreService,
    ) {
        super();
    }

    async process(job: Job<BackupJobData>): Promise<void> {
        const data = job.data;
        if (data.kind === 'backup') {
            await this.backup.executeBackupById(data.runId, data.opts);
        } else if (data.kind === 'restore') {
            await this.restore.executeRestore(data.runId);
        } else {
            this.logger.warn(`Unknown backup job kind: ${JSON.stringify(data)}`);
        }
    }
}
