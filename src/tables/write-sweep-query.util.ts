import { DataSource } from 'typeorm';
import { ParamList } from '../core/sql/sql-params.util';

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
 * Returns each row's own `connection_id` so the selection can be partitioned
 * per connection before submitting.
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
    // Case-insensitive: transport failures keep 'queued', while an SII rejection
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

/**
 * Hasta `limit` filas `queued` de una tabla (todos los grupos a la vez),
 * cada una con su `connection_id` para poder particionar en memoria por
 * (conexión, groupBy) igual que `submitByIds`. `ORDER BY created_at` garantiza
 * que se envían primero las más antiguas; el resto (por encima del tope) espera
 * a la siguiente pasada del cron. Scoped a `table_key` y, si se pasa,
 * `connection_id`.
 */
export async function fetchQueuedRowsCapped(
  dataSource: DataSource,
  tableKey: string,
  limit: number,
  connectionId?: string | null,
): Promise<RowWithConnection[]> {
  const p = new ParamList();
  const where = [`table_key = ${p.push(tableKey)}`, `submission_status = 'queued'`];
  if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
  const rows: { id: string; data: Record<string, unknown>; connection_id: string | null }[] =
    await dataSource.query(
      `SELECT id, data, connection_id FROM table_rows WHERE ${where.join(' AND ')} ORDER BY created_at LIMIT ${p.push(limit)}`,
      p.all,
    );
  return rows.map((r) => ({ id: r.id, data: r.data, connectionId: r.connection_id }));
}

export function chunkRows<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}
