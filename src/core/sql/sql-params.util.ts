export const TABLE_KEY_RE = /^[a-z0-9-]+$/;
export const COLUMN_KEY_RE = /^[A-Za-z0-9_.-]+$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function assertTableKey(k: string): void {
  if (!TABLE_KEY_RE.test(k)) throw new Error(`Unsafe table key "${k}"`);
}

export function assertColumnKey(k: string): void {
  if (!COLUMN_KEY_RE.test(k)) throw new Error(`Unsafe column key "${k}"`);
}

/**
 * Only for DDL (CREATE/DROP INDEX) and the ON CONFLICT arbiter predicate,
 * where Postgres has no bind parameters at all — every other query in this
 * codebase uses `$n` placeholders instead.
 */
export function sqlStringLiteral(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}

export function escapeLike(s: string): string {
  return s.replace(/[\\%_]/g, '\\$&');
}

export function isUuid(s: string): boolean {
  return UUID_RE.test(s);
}

/** Auto-incrementing `$n` placeholder bookkeeping for dynamic WHERE clauses. */
export class ParamList {
  private readonly values: unknown[] = [];

  push(v: unknown): string {
    this.values.push(v);
    return `$${this.values.length}`;
  }

  get all(): unknown[] {
    return this.values;
  }
}
