import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { ParamList } from '../core/sql/sql-params.util';
import { mapVendorResult, VendorBatchResult, VendorResultItem } from './sii-result.mapper';
import type { SiiCallbackJobData } from './sii-callback.types';
import { TableTemplatesService } from '../tables/table-templates.service';

/**
 * Applies the vendor's SII-result callback to `table_rows`. Two correlation
 * paths, both landing on the same columns:
 *  - Line-level, by `internal_ref` (our own row id, stamped on every outbound
 *    line — flat or nested inside a collapsed submission's `rows` — and
 *    echoed back verbatim by the vendor; see submitGroup/buildCollapsedPayloadItem).
 *  - Batch-level fallback, by `batch_ref` (our own `batch_id`), only used
 *    when the vendor's result for a collapsed submission carries no per-line
 *    breakdown at all — the same status is then applied to every row of that
 *    batch.
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
    const { lineItems, batchItems } = mapVendorResult(job.data.payload);
    if (!lineItems.length && !batchItems.length) {
      this.logger.warn('SII callback: no correlatable items in payload (missing internal_ref/batch_ref?)');
      return;
    }

    const matched = await this.applyLineItems(lineItems);
    const batchMatched = await this.applyBatchItems(batchItems);

    await this.applyErrorFields([...matched, ...batchMatched]);
  }

  /** Bulk-updates rows correlated by `internal_ref` (table_rows.id). Returns each matched row paired with the error fields to merge into `data`. */
  private async applyLineItems(items: VendorResultItem[]): Promise<{ id: string; tableKey: string; errorCode?: string; errorMessage?: string }[]> {
    if (!items.length) return [];

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
           AND lower(coalesce(t.submission_status, '')) <> 'correcto'
         RETURNING t.id, t.table_key`,
      p.all,
    );

    const byRef = new Map(items.map((item) => [item.internalRef, item]));
    const matchedIds = new Set(matched.map((m) => m.id));
    for (const item of items) {
      if (!matchedIds.has(item.internalRef)) {
        this.logger.warn(`SII callback: no row found for internal_ref="${item.internalRef}" — ignored`);
      }
    }

    return matched.map((m) => ({
      id: m.id,
      tableKey: m.table_key,
      errorCode: byRef.get(m.id)?.errorCode,
      errorMessage: byRef.get(m.id)?.errorMessage,
    }));
  }

  /** Bulk-updates every row of each `batch_id` correlated by `batch_ref`. Returns each matched row paired with the error fields to merge into `data`. */
  private async applyBatchItems(items: VendorBatchResult[]): Promise<{ id: string; tableKey: string; errorCode?: string; errorMessage?: string }[]> {
    if (!items.length) return [];

    const p = new ParamList();
    const valuesSql = items
      .map(
        (item) =>
          `(${p.push(item.batchRef)}, ${p.push(item.submissionStatus)}, ${p.push(JSON.stringify(item.raw))}::jsonb)`,
      )
      .join(', ');

    const [matched]: [{ id: string; table_key: string; batch_id: string }[], number] = await this.dataSource.query(
      `UPDATE table_rows AS t
         SET submission_status = v.submission_status, sii_response = v.raw
         FROM (VALUES ${valuesSql}) AS v(batch_ref, submission_status, raw)
         WHERE t.batch_id = v.batch_ref
           AND lower(coalesce(t.submission_status, '')) <> 'correcto'
         RETURNING t.id, t.table_key, t.batch_id`,
      p.all,
    );

    const byRef = new Map(items.map((item) => [item.batchRef, item]));
    const matchedRefs = new Set(matched.map((m) => m.batch_id));
    for (const item of items) {
      if (!matchedRefs.has(item.batchRef)) {
        this.logger.warn(`SII callback: no rows found for batch_ref="${item.batchRef}" — ignored`);
      }
    }

    return matched.map((m) => ({
      id: m.id,
      tableKey: m.table_key,
      errorCode: byRef.get(m.batch_id)?.errorCode,
      errorMessage: byRef.get(m.batch_id)?.errorMessage,
    }));
  }

  /**
   * Merges `errorCode`/`errorMessage` into `data.error_code`/`data.error_message`
   * — but only for rows whose template actually declares those columns; tables
   * that don't opt in are left untouched, same spirit as the readOnly/hidden
   * gating in table-rows.service.ts.
   */
  private async applyErrorFields(matched: { id: string; tableKey: string; errorCode?: string; errorMessage?: string }[]): Promise<void> {
    const withError = matched.filter((m) => m.errorCode != null || m.errorMessage != null);
    if (!withError.length) return;

    const itemsByTableKey = new Map<string, typeof withError>();
    for (const m of withError) {
      const bucket = itemsByTableKey.get(m.tableKey);
      if (bucket) bucket.push(m);
      else itemsByTableKey.set(m.tableKey, [m]);
    }

    for (const [tableKey, tableItems] of itemsByTableKey) {
      const template = await this.templates.findByKey(tableKey);
      if (!template) continue;
      const hasErrorCode = template.columns.some((c) => c.key === 'error_code');
      const hasErrorMessage = template.columns.some((c) => c.key === 'error_message');
      if (!hasErrorCode && !hasErrorMessage) continue;

      const p = new ParamList();
      const valuesSql = tableItems
        .map((m) => {
          const patch: Record<string, string> = {};
          if (hasErrorCode && m.errorCode != null) patch.error_code = m.errorCode;
          if (hasErrorMessage && m.errorMessage != null) patch.error_message = m.errorMessage;
          return `(${p.push(m.id)}::uuid, ${p.push(JSON.stringify(patch))}::jsonb)`;
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
