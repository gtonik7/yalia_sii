import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { ParamList } from '../core/sql/sql-params.util';
import { mapVendorResult } from './sii-result.mapper';
import type { SiiCallbackJobData } from './sii-callback.types';

/**
 * Applies the vendor's SII-result callback to `table_rows`, correlated by
 * `internal_ref` (our own row id, stamped on every outbound item and echoed
 * back verbatim by the vendor — see submitGroup in table-rows.service.ts).
 * Deliberately only ever touches `submission_status`/`sii_response` —
 * `ingest()`'s `ON CONFLICT ... DO UPDATE SET data = EXCLUDED.data` (a full
 * jsonb replace) must never be reused here, or a result callback would
 * silently wipe the operator's edited data.
 */
@Processor(QUEUES.SII_INBOUND, { concurrency: 5 })
export class SiiResultProcessor extends WorkerHost {
  private readonly logger = new Logger(SiiResultProcessor.name);

  constructor(@InjectDataSource() private readonly dataSource: DataSource) {
    super();
  }

  async process(job: Job<SiiCallbackJobData>): Promise<void> {
    const items = mapVendorResult(job.data.payload);
    if (!items.length) {
      this.logger.warn('SII callback: no correlatable items in payload (missing internal_ref?)');
      return;
    }

    const p = new ParamList();
    const valuesSql = items
      .map(
        (item) =>
          `(${p.push(item.internalRef)}::uuid, ${p.push(item.submissionStatus)}, ${p.push(JSON.stringify(item.raw))}::jsonb)`,
      )
      .join(', ');

    // UPDATE ... RETURNING returns [rows, rowCount] via TypeORM's raw query() — see table-rows.service.ts's deleteRows().
    const [matched]: [{ id: string }[], number] = await this.dataSource.query(
      `UPDATE table_rows AS t
         SET submission_status = v.submission_status, sii_response = v.raw
         FROM (VALUES ${valuesSql}) AS v(internal_ref, submission_status, raw)
         WHERE t.id = v.internal_ref
         RETURNING t.id`,
      p.all,
    );

    const matchedIds = new Set(matched.map((m) => m.id));
    for (const item of items) {
      if (!matchedIds.has(item.internalRef)) {
        this.logger.warn(`SII callback: no row found for internal_ref="${item.internalRef}" — ignored`);
      }
    }
  }
}
