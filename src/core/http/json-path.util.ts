/**
 * Reads a nested value out of an arbitrary JSON object by a dot-path such as
 * `data.rows` or `result.page.items`. Used to teach the poller where, inside a
 * source system's response envelope, the records array and the paging metadata
 * live — so no concrete-system knowledge is hard-coded.
 *
 * Returns `undefined` when any segment is missing or the value is not an object.
 */
export function getByPath(obj: unknown, path: string | undefined): unknown {
  if (!path) return undefined;
  const segments = path.split('.').map((s) => s.trim()).filter(Boolean);
  let cur: unknown = obj;
  for (const seg of segments) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = (cur as Record<string, unknown>)[seg];
  }
  return cur;
}

/** Coerces a path value to a records array; throws when it is not an array. */
export function getArrayByPath(obj: unknown, path: string): Record<string, unknown>[] {
  const value = getByPath(obj, path);
  if (!Array.isArray(value)) {
    throw new Error(
      `recordsPath "${path}" did not resolve to an array (got ${value === undefined ? 'undefined' : typeof value}). ` +
        'Check the source response shape and the connection pagination.recordsPath.',
    );
  }
  return value as Record<string, unknown>[];
}

/** Truthy test tolerant to the string/number encodings APIs use for booleans. */
export function isTruthyFlag(value: unknown): boolean {
  return value === true || value === 'true' || value === 1 || value === '1';
}

/** Best-effort numeric read for paging metadata (total pages/results). */
export function getNumberByPath(obj: unknown, path: string | undefined): number | undefined {
  const value = getByPath(obj, path);
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value))) {
    return Number(value);
  }
  return undefined;
}
