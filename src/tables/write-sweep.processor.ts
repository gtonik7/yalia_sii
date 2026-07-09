import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';
import { WriteSweepJobData } from './write-sweep.types';
import { chunkRows, fetchQueuedRows } from './write-sweep-query.util';

/**
 * Fires once per debounced (tableKey, groupValues) job — see
 * TableRowsService.enqueueWriteSweep for why the job's own payload is never
 * trusted for anything beyond identifying the group: BullMQ collapses
 * concurrent enqueues sharing a `jobId`, so a burst of edits during the
 * debounce window (or one landing while a previous sweep for the same group
 * is already active — see the plan's open risk on same-jobId races) would be
 * silently lost if this re-queried anything less than current DB state.
 */
@Processor(QUEUES.WRITE_SWEEP, { concurrency: 1 })
export class WriteSweepProcessor extends WorkerHost {
  private readonly logger = new Logger(WriteSweepProcessor.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
  ) {
    super();
  }

  async process(job: Job<WriteSweepJobData>): Promise<void> {
    const { tableKey, groupValues, connectionId } = job.data;

    const template = await this.templates.findByKey(tableKey);
    if (!template?.write) {
      this.logger.warn(`Sweep skipped: table "${tableKey}" no longer has a write config`);
      return;
    }

    const queued = await fetchQueuedRows(this.dataSource, tableKey, groupValues, connectionId);
    if (!queued.length) return;

    const maxBatchSize = template.write.batch?.maxBatchSize;
    const chunks = maxBatchSize ? chunkRows(queued, maxBatchSize) : [queued];

    for (const rowsChunk of chunks) {
      const result = await this.rows.submitGroup(template, rowsChunk, { trigger: 'event', groupValues, connectionId });
      if (result?.status === 'error') {
        this.logger.warn(`Sweep table=${tableKey} batch=${result.batchId} failed: ${result.error}`);
      }
    }
  }
}
