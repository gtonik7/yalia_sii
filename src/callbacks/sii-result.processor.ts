import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { ParamList } from '../core/sql/sql-params.util';
import { mapVendorResult, VendorResultItem } from './sii-result.mapper';
import type { SiiCallbackJobData } from './sii-callback.types';
import { TableTemplatesService } from '../tables/table-templates.service';

/**
 * Applies the vendor's SII-result callback to `table_rows`, correlated by
 * `internal_ref` (our own row id, stamped on every outbound item and echoed
 * back verbatim by the vendor — see submitGroup in table-rows.service.ts).
 * The first pass only ever touches `submission_status`/`sii_response` —
 * `ingest()`'s `ON CONFLICT ... DO UPDATE SET data = EXCLUDED.data` (a full
 * jsonb replace) must never be reused here, or a result callback would
 * silently wipe the operator's edited data. `errorCode`/`errorMessage` are
 * merged into `data` as a second, narrower pass — and only for the
 * `error_code`/`error_message` keys the row's template actually declares.
 */
@Processor(QUEUES.SII_INBOUND, { concurrency: 5 })
export class SiiResultProcessor extends WorkerHost {
  private readonly logger = new Logger(SiiResultProcessor.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly templates: TableTemplatesService,
  ) {
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
    const [matched]: [{ id: string; table_key: string }[], number] = await this.dataSource.query(
      `UPDATE table_rows AS t
         SET submission_status = v.submission_status, sii_response = v.raw
         FROM (VALUES ${valuesSql}) AS v(internal_ref, submission_status, raw)
         WHERE t.id = v.internal_ref
         RETURNING t.id, t.table_key`,
      p.all,
    );

    const matchedIds = new Set(matched.map((m) => m.id));
    for (const item of items) {
      if (!matchedIds.has(item.internalRef)) {
        this.logger.warn(`SII callback: no row found for internal_ref="${item.internalRef}" — ignored`);
      }
    }

    await this.applyErrorFields(items, matched);
  }

  /**
   * Merges `errorCode`/`errorMessage` into `data.error_code`/`data.error_message`
   * — but only for rows whose template actually declares those columns; tables
   * that don't opt in are left untouched, same spirit as the readOnly/hidden
   * gating in table-rows.service.ts.
   */
  private async applyErrorFields(items: VendorResultItem[], matched: { id: string; table_key: string }[]): Promise<void> {
    const withError = items.filter((item) => item.errorCode != null || item.errorMessage != null);
    if (!withError.length) return;

    const tableKeyById = new Map(matched.map((m) => [m.id, m.table_key]));
    const itemsByTableKey = new Map<string, VendorResultItem[]>();
    for (const item of withError) {
      const tableKey = tableKeyById.get(item.internalRef);
      if (!tableKey) continue; // unmatched row — already warned above
      const bucket = itemsByTableKey.get(tableKey);
      if (bucket) bucket.push(item);
      else itemsByTableKey.set(tableKey, [item]);
    }

    for (const [tableKey, tableItems] of itemsByTableKey) {
      const template = await this.templates.findByKey(tableKey);
      if (!template) continue;
      const hasErrorCode = template.columns.some((c) => c.key === 'error_code');
      const hasErrorMessage = template.columns.some((c) => c.key === 'error_message');
      if (!hasErrorCode && !hasErrorMessage) continue;

      const p = new ParamList();
      const valuesSql = tableItems
        .map((item) => {
          const patch: Record<string, string> = {};
          if (hasErrorCode && item.errorCode != null) patch.error_code = item.errorCode;
          if (hasErrorMessage && item.errorMessage != null) patch.error_message = item.errorMessage;
          return `(${p.push(item.internalRef)}::uuid, ${p.push(JSON.stringify(patch))}::jsonb)`;
        })
        .join(', ');

      await this.dataSource.query(
        `UPDATE table_rows AS t
           SET data = t.data || v.patch
           FROM (VALUES ${valuesSql}) AS v(internal_ref, patch)
           WHERE t.id = v.internal_ref`,
        p.all,
      );
    }
  }
}
