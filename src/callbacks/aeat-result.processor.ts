import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { ParamList } from '../core/sql/sql-params.util';
import { mapVendorResult } from './aeat-result.mapper';
import type { AeatCallbackJobData } from './aeat-callback.types';

/**
 * Applies the vendor's AEAT-result callback to `table_rows`, correlated by
 * `external_ref`. Deliberately only ever touches `submission_status`/
 * `aeat_response` — `ingest()`'s `ON CONFLICT ... DO UPDATE SET data =
 * EXCLUDED.data` (a full jsonb replace) must never be reused here, or a
 * result callback would silently wipe the operator's edited data.
 */
@Processor(QUEUES.AEAT_INBOUND, { concurrency: 5 })
export class AeatResultProcessor extends WorkerHost {
  private readonly logger = new Logger(AeatResultProcessor.name);

  constructor(@InjectDataSource() private readonly dataSource: DataSource) {
    super();
  }

  async process(job: Job<AeatCallbackJobData>): Promise<void> {
    const items = mapVendorResult(job.data.payload);
    if (!items.length) {
      this.logger.warn('AEAT callback: no correlatable items in payload (missing invoiceId?)');
      return;
    }

    const p = new ParamList();
    const valuesSql = items
      .map(
        (item) =>
          `(${p.push(item.externalRef)}, ${p.push(item.submissionStatus)}, ${p.push(JSON.stringify(item.raw))}::jsonb)`,
      )
      .join(', ');

    // UPDATE ... RETURNING returns [rows, rowCount] via TypeORM's raw query() — see table-rows.service.ts's deleteRows().
    const [matched]: [{ external_ref: string }[], number] = await this.dataSource.query(
      `UPDATE table_rows AS t
         SET submission_status = v.submission_status, aeat_response = v.raw
         FROM (VALUES ${valuesSql}) AS v(external_ref, submission_status, raw)
         WHERE t.external_ref = v.external_ref
         RETURNING t.external_ref`,
      p.all,
    );

    const matchedRefs = new Set(matched.map((m) => m.external_ref));
    for (const item of items) {
      if (!matchedRefs.has(item.externalRef)) {
        this.logger.warn(`AEAT callback: no row found for external_ref="${item.externalRef}" — ignored`);
      }
    }
  }
}
