import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { DataSource, Repository } from 'typeorm';
import { TableTemplatesService } from '../tables/table-templates.service';
import { TableRowsService } from '../tables/table-rows.service';
import type { AuditConfig, TableTemplate } from '../tables/entities/table-template.entity';
import { SourceConnectionsService } from '../connections/source-connections.service';
import { SourceHttpClient, type PageState, type SourceRequest } from '../connections/source-http.client';
import { SourcePollRunService } from './source-poll-run.service';
import { SourcePollState } from './entities/source-poll-state.entity';

export interface AuditPollOptions {
  tableKey: string;
  /** Overrides the connection bound in the template's audit config. */
  connectionId?: string;
  trigger?: 'manual' | 'scheduled';
}

/** Hard cap so a misconfigured stop-condition can never loop forever. */
const MAX_PAGES = 100_000;

@Injectable()
export class SourcePollService {
  private readonly logger = new Logger(SourcePollService.name);

  constructor(
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
    private readonly connections: SourceConnectionsService,
    private readonly client: SourceHttpClient,
    private readonly runs: SourcePollRunService,
    @InjectRepository(SourcePollState)
    private readonly stateRepo: Repository<SourcePollState>,
    @InjectDataSource()
    private readonly dataSource: DataSource,
  ) {}

  /** Validate + create the run, then page the source in the background. */
  async poll(opts: AuditPollOptions): Promise<string> {
    const template = await this.templates.getByKey(opts.tableKey);
    if (!template.audit) {
      throw new BadRequestException(`Table "${opts.tableKey}" has no audit config (not pull-enabled)`);
    }
    const connectionId = opts.connectionId ?? template.audit.connectionId;
    if (!connectionId) {
      throw new BadRequestException(`Table "${opts.tableKey}" audit has no connectionId`);
    }
    const conn = await this.connections.resolveById(connectionId);

    // Read watermark up-front so the run records the effective floor.
    const state = await this.stateRepo.findOne({ where: { tableKey: template.key, connectionId } });
    const since = template.audit.incremental ? state?.lastUpdatedAt ?? null : null;

    const run = await this.runs.create({
      tableKey: template.key,
      connectionId,
      connectionName: conn.name,
      trigger: opts.trigger ?? 'manual',
      since,
    });
    const runId = run.id;

    this.logger.log(`Audit run=${runId} table=${template.key} conn=${connectionId} since=${since ?? '—'} starting`);
    void this.execute(runId, template, template.audit, connectionId, since);
    return runId;
  }

  private async execute(
    runId: string,
    template: TableTemplate,
    audit: AuditConfig,
    connectionId: string,
    since: string | null,
  ): Promise<void> {
    let pages = 0;
    let fetched = 0;
    let inserted = 0;
    let upserted = 0;
    let maxUpdatedAt = since;

    try {
      const conn = await this.connections.resolveById(connectionId);
      const recordsPath = audit.recordsPath ?? conn.pagination.recordsPath;
      const req = this.buildRequest(audit, since);
      // Per-table recordsPath override is applied by shadowing the connection's.
      const effConn = { ...conn, pagination: { ...conn.pagination, recordsPath } };

      let state: PageState | null = this.client.initialState(effConn, req);
      while (state && pages < MAX_PAGES) {
        const page = await this.client.fetchPage(effConn, req, state);
        pages++;
        fetched += page.records.length;

        if (page.records.length) {
          const res = await this.rows.ingest(template, page.records, connectionId, runId);
          inserted += res.inserted;
          upserted += res.upserted;
        }

        if (audit.incremental) {
          maxUpdatedAt = advanceWatermark(maxUpdatedAt, page.records, audit.incremental.updatedAtField);
        }

        await this.runs.progress(runId, { pages, fetched, inserted, upserted });
        state = page.next;
      }

      if (audit.incremental) {
        // Plain unique index over real columns (table_key, connection_id) — no
        // literal-interpolation concern here, unlike table_rows' partial
        // indexes; everything is bound. TypeORM's repository API can't express
        // the atomic increment, hence the raw upsert.
        await this.dataSource.query(
          `INSERT INTO source_poll_states (table_key, connection_id, last_updated_at, total_seen, last_run_at)
           VALUES ($1, $2, $3, $4, now())
           ON CONFLICT (table_key, connection_id)
           DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at,
                          total_seen = source_poll_states.total_seen + EXCLUDED.total_seen,
                          last_run_at = EXCLUDED.last_run_at`,
          [template.key, connectionId, maxUpdatedAt, fetched],
        );
      }

      await this.runs.complete(runId, { pages, fetched, inserted, upserted });
      this.logger.log(
        `Audit run=${runId} done pages=${pages} fetched=${fetched} inserted=${inserted} upserted=${upserted}`,
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error(`Audit run=${runId} failed: ${message}`);
      await this.runs.fail(runId, message);
    }
  }

  /** Compose the SourceRequest, injecting the incremental `since` param. */
  private buildRequest(audit: AuditConfig, since: string | null): SourceRequest {
    const query: Record<string, string> = { ...(audit.query ?? {}) };
    let body = audit.body as unknown;

    if (audit.incremental && since) {
      const value = formatSince(since, audit.incremental.sinceFormat);
      if ((audit.incremental.sinceIn ?? 'query') === 'body') {
        body = { ...(audit.body ?? {}), [audit.incremental.sinceParam]: value };
      } else {
        query[audit.incremental.sinceParam] = value;
      }
    }

    return { path: audit.path, method: audit.method, query, body };
  }
}

/** Advance the watermark to the newest `updatedAtField` value seen on a page. */
function advanceWatermark(
  current: string | null,
  records: Record<string, unknown>[],
  field: string,
): string | null {
  let max = current;
  for (const r of records) {
    const v = r[field];
    if (v == null) continue;
    const s = String(v);
    if (max == null || s > max) max = s;
  }
  return max;
}

/** Encode the watermark for the wire format the source expects. */
function formatSince(since: string, format: 'iso' | 'epoch_ms' | 'epoch_s' | undefined): string {
  if (!format || format === 'iso') return since;
  const ms = Date.parse(since);
  if (Number.isNaN(ms)) return since;
  return format === 'epoch_s' ? String(Math.floor(ms / 1000)) : String(ms);
}
