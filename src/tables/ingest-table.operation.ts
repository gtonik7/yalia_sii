import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import {
  OperationContext,
  OperationHandler,
  OperationResult,
} from '../operations/operation.interface';
import { OperationRegistryService } from '../operations/operation-registry.service';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';
import { SourceConnectionsService } from '../connections/source-connections.service';

/**
 * Destination operation invoked by hub flows. Lands the delivered payload as
 * rows of a table template so they can be browsed in the explorer datatable.
 *
 * The target template is taken from the destination params (`params.tableKey`)
 * or, as a fallback, from the payload (`payload.tableKey`). Rows may be a single
 * object, an array, or `{ rows: [...] }`.
 */
@Injectable()
export class IngestTableOperation implements OperationHandler, OnModuleInit {
  readonly operationKey = 'table.ingest';
  readonly payloadExample: Record<string, unknown> = {
    tableKey: 'mi-tabla',
    rows: [{ id: '1', nombre: 'Ejemplo', estado: 'ok' }],
  };
  private readonly logger = new Logger(IngestTableOperation.name);

  constructor(
    private readonly registry: OperationRegistryService,
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
    private readonly connections: SourceConnectionsService,
  ) {}

  onModuleInit(): void {
    this.registry.register(this);
  }

  async execute(payload: unknown, ctx: OperationContext): Promise<OperationResult> {
    const body = (payload ?? {}) as Record<string, unknown>;
    const tableKey = asString(ctx.params?.tableKey) ?? asString(body.tableKey);
    if (!tableKey) {
      return err('MISSING_TABLE_KEY', 'tableKey is required (destination params or payload)');
    }

    const template = await this.templates.findByKey(tableKey);
    if (!template) {
      return err('UNKNOWN_TABLE', `No template registered for tableKey "${tableKey}"`);
    }

    const rows = extractRows(payload);
    if (!rows.length) {
      return err('EMPTY_PAYLOAD', 'No rows found in payload');
    }

    if (!ctx.connectionId) {
      return err('MISSING_CONNECTION', `No connectionId was provided for template "${tableKey}"`);
    }

    // The explorer's connection picker is scoped to registered
    // source_connections (see TableDatasetBridge); a connectionId that isn't
    // one of those would land rows nothing can ever select in the UI, so
    // reject it here instead of silently accepting an unreachable row.
    if (!(await this.connections.exists(ctx.connectionId))) {
      return err('UNKNOWN_CONNECTION', `"${ctx.connectionId}" is not a registered source connection for template "${tableKey}"`);
    }

    try {
      const res = await this.rows.ingest(template, rows, ctx.connectionId, ctx.traceId);
      this.logger.log(
        `Ingested table=${tableKey} inserted=${res.inserted} upserted=${res.upserted} trace=${ctx.traceId}`,
      );
      return { status: 'ok', data: { tableKey, ...res } };
    } catch (e) {
      const message = e instanceof Error ? e.message : 'ingest failed';
      this.logger.error(`Ingest failed table=${tableKey} trace=${ctx.traceId}: ${message}`);
      return err('INGEST_FAILED', message);
    }
  }
}

function extractRows(payload: unknown): Record<string, unknown>[] {
  if (Array.isArray(payload)) return payload as Record<string, unknown>[];
  if (payload && typeof payload === 'object') {
    const body = payload as Record<string, unknown>;
    if (Array.isArray(body.rows)) return body.rows as Record<string, unknown>[];
    // A single row object: strip the routing-only `tableKey` field.
    const { tableKey: _omit, ...rest } = body;
    return Object.keys(rest).length ? [rest] : [];
  }
  return [];
}

function asString(v: unknown): string | undefined {
  return v != null && v !== '' ? String(v) : undefined;
}

function err(code: string, message: string): OperationResult {
  return { status: 'error', error: { code, message } };
}
