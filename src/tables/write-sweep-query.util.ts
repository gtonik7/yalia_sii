import { DataSource } from 'typeorm';
import { ParamList } from '../core/sql/sql-params.util';

/** A row fetched by explicit id, carrying its ingestion connection and materialized group id for grouping. */
export interface RowWithConnection {
  id: string;
  data: Record<string, unknown>;
  connectionId: string | null;
  /** `table_rows.group_id` — the materialized grouping key (NULL for legacy/ungrouped rows). */
  groupId: string | null;
  /** `table_rows.submission_status` — used by `partitionAndSubmit` to pick the create vs update send target. */
  submissionStatus: string | null;
}

/** Raw column shape shared by every fetch below. */
interface RowRecord {
  id: string;
  data: Record<string, unknown>;
  connection_id: string | null;
  group_id: string | null;
  submission_status: string | null;
}

function toRowWithConnection(r: RowRecord): RowWithConnection {
  return { id: r.id, data: r.data, connectionId: r.connection_id, groupId: r.group_id, submissionStatus: r.submission_status };
}

/**
 * Tope duro de filas por llamada HTTP saliente, independiente de
 * `write.batch.maxBatchSize` (que solo agrupa; el usuario podría no
 * configurarlo, o configurarlo por encima de esto). Compartido por todos los
 * caminos que trocean particiones (schedule, force-submit, event-mode sweep)
 * para que ninguno mande de más aunque `maxBatchSize` esté mal configurado.
 */
export const HARD_MAX_BATCH_SIZE = 1000;

/**
 * Fetch a specific set of rows by id for manual "force submit" of a selection
 * (distinct from the queued-only sweeps above). A row can be re-submitted
 * unless SII already accepted it (`submission_status = 'CORRECTO'`, terminal).
 * A row `'pending'` (transport-sent, no vendor result yet) is also eligible
 * here — the FE surfaces an explicit extra confirmation before letting the
 * user re-present rows still mid-flight, since a duplicate presentation is
 * possible if the original callback later lands. Rows that were never queued
 * (`submission_status IS NULL`) are excluded too. Scoped to `table_key` and,
 * when given, `connection_id`. Returns each row's own `connection_id` so the
 * selection can be partitioned per connection before submitting.
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
    `submission_status IS NOT NULL`,
    // Case-insensitive: internal statuses ('queued'/'pending'/'error') are
    // lowercase, vendor callback literals ('CORRECTO'/'ERROR') are uppercase.
    `lower(submission_status) <> 'correcto'`,
    // 'staged' rows belong to a load that isn't complete yet — never sendable,
    // not even by force-submit, so a group never goes out before its load seals.
    `submission_status <> 'staged'`,
  ];
  if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
  const rows: RowRecord[] = await dataSource.query(
    `SELECT id, data, connection_id, group_id, submission_status FROM table_rows WHERE ${where.join(' AND ')} ORDER BY created_at`,
    p.all,
  );
  return rows.map(toRowWithConnection);
}

/**
 * Hasta `limit` filas `queued`/`revisado` de una tabla (todos los grupos a la
 * vez), cada una con su `connection_id` para poder particionar en memoria por
 * (conexión, groupBy) igual que `submitByIds`. `'revisado'` (fijado por una
 * edición manual, ver updateAndWrite) se trata exactamente igual que `'queued'`
 * a efectos de envío. `ORDER BY created_at` garantiza que se envían primero
 * las más antiguas; el resto (por encima del tope) espera a la siguiente
 * pasada del cron. Scoped a `table_key` y, si se pasa, `connection_id`.
 */
export async function fetchQueuedRowsCapped(
  dataSource: DataSource,
  tableKey: string,
  limit: number,
  connectionId?: string | null,
): Promise<RowWithConnection[]> {
  const p = new ParamList();
  const where = [`table_key = ${p.push(tableKey)}`, `submission_status IN ('queued', 'revisado')`];
  if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
  const limitParam = p.push(limit);
  // Group-atomic cap: en vez de un `LIMIT` plano sobre filas (que podía cortar un
  // grupo a caballo del tope y enviarlo a medias), se eligen GRUPOS ENTEROS hasta
  // el tope. Los grupos se ordenan por su fila más antigua (`min(created_at)`,
  // igual que el orden previo) y se van sumando sus tamaños; se incluye un grupo
  // solo si el acumulado ≤ límite, y SIEMPRE al menos el primero (para no bloquear
  // un grupo único mayor que el tope — luego `maxBatchSize`/`HARD_MAX_BATCH_SIZE`
  // lo trocea en llamadas ≤1000). `grp = coalesce(group_id, id::text)`: las filas
  // sin grupo cuentan cada una como su propio grupo unitario. Así una carga sellada
  // nunca se parte por el tope del cron.
  const rows: RowRecord[] = await dataSource.query(
    `WITH candidate AS (
       SELECT id, data, connection_id, group_id, submission_status, created_at,
              coalesce(group_id, id::text) AS grp
       FROM table_rows
       WHERE ${where.join(' AND ')}
     ),
     grp AS (
       SELECT grp, min(created_at) AS grp_min, count(*) AS grp_count
       FROM candidate GROUP BY grp
     ),
     ranked AS (
       SELECT grp, grp_count,
              sum(grp_count) OVER (ORDER BY grp_min, grp) AS cum,
              row_number() OVER (ORDER BY grp_min, grp) AS rn
       FROM grp
     ),
     chosen AS (
       SELECT grp FROM ranked WHERE cum <= ${limitParam} OR rn = 1
     )
     SELECT c.id, c.data, c.connection_id, c.group_id, c.submission_status
     FROM candidate c JOIN chosen ch ON ch.grp = c.grp
     ORDER BY c.created_at`,
    p.all,
  );
  return rows.map(toRowWithConnection);
}

/**
 * Filas aún enviables (`submission_status` presente y distinto de `'correcto'`,
 * misma elegibilidad que `fetchRowsByIds`) que pertenecen a cualquiera de los
 * `groupIds` dados — usado por "Forzar envío" para expandir una selección a
 * TODAS las hermanas del mismo grupo, de modo que un grupo nunca se envíe a
 * medias. Scoped a `table_key` y, si se pasa, `connection_id`.
 */
export async function fetchRowsByGroupIds(
  dataSource: DataSource,
  tableKey: string,
  groupIds: string[],
  connectionId?: string | null,
): Promise<RowWithConnection[]> {
  if (!groupIds.length) return [];
  const p = new ParamList();
  const where = [
    `table_key = ${p.push(tableKey)}`,
    `group_id = ANY(${p.push(groupIds)})`,
    `submission_status IS NOT NULL`,
    `lower(submission_status) <> 'correcto'`,
    // Excluir 'staged': una carga incompleta nunca se expande a un force-submit.
    `submission_status <> 'staged'`,
  ];
  if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
  const rows: RowRecord[] = await dataSource.query(
    `SELECT id, data, connection_id, group_id, submission_status FROM table_rows WHERE ${where.join(' AND ')} ORDER BY created_at`,
    p.all,
  );
  return rows.map(toRowWithConnection);
}

/**
 * TODAS las filas de un grupo (cualquier `submission_status`, incluidas las ya
 * `correcto`/`pending`), ordenadas por `created_at`. Se usa para construir el
 * payload combinado (`collapse`) COMPLETO: las hermanas ya aceptadas viajan
 * como líneas de contexto para que el registro externo quede íntegro, aunque
 * solo se marquen las que realmente se envían (ver `submitGroup` `markRowIds`).
 * `group_id` ya codifica la conexión, pero se filtra también por `connection_id`
 * cuando se pasa para aprovechar el índice.
 */
export async function fetchGroupMembers(
  dataSource: DataSource,
  tableKey: string,
  connectionId: string | null,
  groupId: string,
): Promise<RowWithConnection[]> {
  const p = new ParamList();
  const where = [`table_key = ${p.push(tableKey)}`, `group_id = ${p.push(groupId)}`];
  if (connectionId) where.push(`connection_id = ${p.push(connectionId)}`);
  const rows: RowRecord[] = await dataSource.query(
    `SELECT id, data, connection_id, group_id, submission_status FROM table_rows WHERE ${where.join(' AND ')} ORDER BY created_at`,
    p.all,
  );
  return rows.map(toRowWithConnection);
}

export function chunkRows<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

/**
 * Ejecuta `tasks` con como mucho `limit` en vuelo a la vez. Con `limit <= 1` (o
 * una sola tarea) corre estrictamente secuencial y en orden — idéntico al bucle
 * `for await` clásico, de modo que el comportamiento por defecto no cambia.
 * Con `limit > 1` reparte las tareas entre `limit` workers. Un rechazo se
 * propaga (misma semántica que el bucle secuencial previo).
 */
export async function runWithConcurrency(tasks: Array<() => Promise<void>>, limit: number): Promise<void> {
  const n = Math.max(1, Math.floor(limit));
  if (n === 1 || tasks.length <= 1) {
    for (const task of tasks) await task();
    return;
  }
  let next = 0;
  const workers = Array.from({ length: Math.min(n, tasks.length) }, async () => {
    while (next < tasks.length) {
      const current = next++;
      await tasks[current]();
    }
  });
  await Promise.all(workers);
}
