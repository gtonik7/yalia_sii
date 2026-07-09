import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';
import { WriteEventJobData } from './write-event.types';
import { fetchRowsByIds } from './write-sweep-query.util';

/**
 * Immediate send of a single edited row (event mode): fired right after a form
 * save on a template whose `write.trigger==='event'`. Sends exactly that row as
 * an array of 1 — never the group's other queued rows (the per-connection
 * internal cron handles those). Re-checks the row is still `queued`/`error`
 * (via fetchRowsByIds) so a duplicate/stale job — or one racing the cron —
 * simply no-ops instead of re-sending an already accepted/pending row.
 */
@Processor(QUEUES.WRITE_EVENT, { concurrency: 5 })
export class WriteEventProcessor extends WorkerHost {
  private readonly logger = new Logger(WriteEventProcessor.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
  ) {
    super();
  }

  async process(job: Job<WriteEventJobData>): Promise<void> {
    const { tableKey, rowId, connectionId } = job.data;

    const template = await this.templates.findByKey(tableKey);
    if (!template?.write) {
      this.logger.warn(`Event send skipped: table "${tableKey}" no longer has a write config`);
      return;
    }

    const eligible = await fetchRowsByIds(this.dataSource, tableKey, [rowId], connectionId);
    if (!eligible.length) return; // already sent/pending, or deleted — nothing to do

    const row = eligible[0];
    const result = await this.rows.submitGroup(template, [{ id: row.id, data: row.data }], {
      trigger: 'event',
      connectionId,
    });
    if (result?.status === 'error') {
      this.logger.warn(`Event send table=${tableKey} row=${rowId} failed: ${result.error}`);
    }
  }
}
