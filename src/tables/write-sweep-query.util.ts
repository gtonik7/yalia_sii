import { DataSource } from 'typeorm';
import { ParamList } from '../core/sql/sql-params.util';

export interface QueuedRow {
  id: string;
  data: Record<string, unknown>;
}

/**
 * One distinct (connection, batch-group) tuple currently represented among a
 * template's `queued` rows. `connectionId` is null for non-perConnection
 * templates (submission then falls back to the template's fixed
 * `write.connectionId`); for perConnection templates it's the row's own
 * ingestion connection, so each company/tenant's queued rows submit through
 * their own connection instead of a single hardcoded one.
 */
export interface QueuedGroup {
  connectionId: string | null;
  groupValues: Record<string, string>;
}

/**
 * Rows currently `queued` for a specific batch group (or every `queued` row
 * of the template when `groupValues` is empty). Shared by WriteSweepProcessor
 * (one known group per debounced job) and TableWriteBatchService (schedule
 * mode, which must discover every group present before fetching each).
 * `connectionId` scopes to one connection's rows — pass it whenever the
 * group came from a perConnection template, so a batch never mixes rows
 * ingested under different connections.
 */
export async function fetchQueuedRows(
  dataSource: DataSource,
  tableKey: string,
  groupValues: Record<string, string>,
  connectionId?: string | null,
): Promise<QueuedRow[]> {
  const p = new ParamList();
  const where = [`table_key = ${p.push(tableKey)}`, `submission_status = 'queued'`];
  if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
  for (const [col, value] of Object.entries(groupValues)) {
    where.push(`(data ->> ${p.push(col)}) = ${p.push(value)}`);
  }
  return dataSource.query(`SELECT id, data FROM table_rows WHERE ${where.join(' AND ')} ORDER BY created_at`, p.all);
}

/**
 * Every distinct (connection, batch-group) tuple currently represented among
 * `queued` rows of a template — schedule mode has no single group handed to
 * it (unlike the event-mode debounce job), so it must find out which groups
 * exist before it can fetch/submit each one separately.
 *
 * When `opts.perConnection` is true, `connection_id` is folded into the
 * distinct tuple (and optionally pinned via `opts.connectionId`) so each
 * connection's queued rows are discovered — and later submitted — separately,
 * instead of being lumped into the template's single fixed write connection.
 * `[{ connectionId: null, groupValues: {} }]` (one global group) when neither
 * `groupBy` nor `perConnection` apply.
 */
export async function discoverQueuedGroups(
  dataSource: DataSource,
  tableKey: string,
  groupBy: string[],
  opts?: { perConnection?: boolean; connectionId?: string },
): Promise<QueuedGroup[]> {
  const perConnection = opts?.perConnection ?? false;
  if (!groupBy.length && !perConnection) return [{ connectionId: null, groupValues: {} }];

  const p = new ParamList();
  const where = [`table_key = ${p.push(tableKey)}`, `submission_status = 'queued'`];
  if (perConnection && opts?.connectionId) where.push(`connection_id = ${p.push(opts.connectionId)}`);

  const selectCols = groupBy.map((col, i) => `(data ->> ${p.push(col)}) AS g${i}`);
  if (perConnection) selectCols.unshift(`connection_id AS conn`);

  const rows: Record<string, string>[] = await dataSource.query(
    `SELECT DISTINCT ${selectCols.join(', ')} FROM table_rows WHERE ${where.join(' AND ')}`,
    p.all,
  );
  return rows.map((r) => {
    const groupValues: Record<string, string> = {};
    groupBy.forEach((col, i) => {
      groupValues[col] = r[`g${i}`];
    });
    return { connectionId: perConnection ? (r.conn ?? null) : null, groupValues };
  });
}

/** A row fetched by explicit id, carrying its ingestion connection for grouping. */
export interface RowWithConnection {
  id: string;
  data: Record<string, unknown>;
  connectionId: string | null;
}

/**
 * Fetch a specific set of rows by id for manual "force submit" of a selection
 * (distinct from the queued-only sweeps above). Only rows still eligible to
 * submit are returned — `submission_status IN ('queued','error')` — so already
 * accepted/pending rows in the selection are silently skipped (never
 * re-presented). Scoped to `table_key` and, when given, `connection_id`.
 * Returns each row's own `connection_id` so a perConnection selection can be
 * partitioned per connection before submitting.
 */
export async function fetchRowsByIds(
  dataSource: DataSource,
  tableKey: string,
  ids: string[],
  connectionId?: string | null,
): Promise<RowWithConnection[]> {
  if (!ids.length) return [];
  const p = new ParamList();
  const where = [
    `table_key = ${p.push(tableKey)}`,
    `id = ANY(${p.push(ids)}::uuid[])`,
    // Case-insensitive: transport failures keep 'queued', while an AEAT rejection
    // arrives from the callback as the vendor literal 'ERROR' (uppercase).
    `lower(submission_status) IN ('queued', 'error')`,
  ];
  if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
  const rows: { id: string; data: Record<string, unknown>; connection_id: string | null }[] =
    await dataSource.query(
      `SELECT id, data, connection_id FROM table_rows WHERE ${where.join(' AND ')} ORDER BY created_at`,
      p.all,
    );
  return rows.map((r) => ({ id: r.id, data: r.data, connectionId: r.connection_id }));
}

export function chunkRows<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}
