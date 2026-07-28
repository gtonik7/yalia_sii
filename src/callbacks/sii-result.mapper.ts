export interface VendorResultItem {
  /** Correlates back to table_rows.id — the `internal_ref` we sent outbound, echoed back verbatim. */
  internalRef: string;
  /** Vendor's own state string, stored verbatim (see note below — no translation layer yet). */
  submissionStatus: string;
  /** Vendor's error code, when present — mapped into the row's `error_code` field if the template declares one. */
  errorCode?: string;
  /** Vendor's error message, when present — mapped into the row's `error_message` field if the template declares one. */
  errorMessage?: string;
  /** The whole per-item payload, stored verbatim into table_rows.sii_response. */
  raw: Record<string, unknown>;
}

export interface VendorBatchResult {
  /** Correlates back to every table_rows.batch_id that matches — the `batch_ref` we sent outbound (see buildCollapsedPayloadItem), echoed back verbatim. */
  batchRef: string;
  submissionStatus: string;
  errorCode?: string;
  errorMessage?: string;
  raw: Record<string, unknown>;
}

export interface MappedVendorResult {
  /** Correlated by `internal_ref` (table_rows.id) — one entry per row. */
  lineItems: VendorResultItem[];
  /**
   * Fallback for a collapsed submission whose vendor result carries no
   * per-line breakdown at all — just one status for the whole group,
   * correlated by `batch_ref` (table_rows.batch_id) instead.
   */
  batchItems: VendorBatchResult[];
}

/**
 * Normalizes the vendor's callback payload into correlatable results — a
 * single-row submission, a flat batch (array of N items), and a *collapsed*
 * batch (one item with a nested per-row array, see `buildCollapsedPayloadItem`)
 * all hit the exact same callback contract, so nothing downstream needs to
 * know which shape produced this payload.
 *
 * Line-level correlation is by `internal_ref` — the row id we stamped on
 * every outbound line (flat item or nested row) — which the vendor is
 * expected to echo back verbatim. For a collapsed submission, the nested
 * array can be under any key (whatever `collapse.rowsField` was configured
 * as), so it's detected generically: any array property of a top-level item
 * whose elements all carry their own `internal_ref` is treated as that
 * item's per-line breakdown, and a nested element missing its own
 * `state`/`errorCode`/`errorMessage` inherits the parent item's. A top-level
 * item with neither a nested per-line array nor its own `internal_ref`, but
 * with a `batch_ref`, is a batch-level fallback result instead.
 *
 * Isolated on purpose: the exact payload shape and the success literal
 * (assumed to be some form of `"CORRECTO"`) are both unconfirmed with the
 * vendor as of this writing. `submissionStatus` is stored exactly as the
 * vendor sent it — deliberately NOT lowercased/translated into our own
 * queued/pending vocabulary — so this function is the only place that needs
 * to change once the real contract (or a second vendor with a different
 * shape) is confirmed.
 */
export function mapVendorResult(payload: unknown): MappedVendorResult {
  const items = Array.isArray(payload) ? payload : [payload];
  const lineItems: VendorResultItem[] = [];
  const batchItems: VendorBatchResult[] = [];

  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    const obj = item as Record<string, unknown>;

    const parentState = typeof obj.state === 'string' && obj.state ? obj.state : undefined;
    const parentErrorCode = typeof obj.errorCode === 'string' && obj.errorCode ? obj.errorCode : undefined;
    const parentErrorMessage = typeof obj.errorMessage === 'string' && obj.errorMessage ? obj.errorMessage : undefined;

    const nestedRows = findNestedRows(obj);
    if (nestedRows) {
      for (const nested of nestedRows) {
        const internalRef = nested.internal_ref;
        if (internalRef == null || internalRef === '') continue;
        const state = typeof nested.state === 'string' && nested.state ? nested.state : parentState;
        lineItems.push({
          internalRef: String(internalRef),
          submissionStatus: state ?? 'unknown',
          errorCode: (typeof nested.errorCode === 'string' && nested.errorCode ? nested.errorCode : undefined) ?? parentErrorCode,
          errorMessage: (typeof nested.errorMessage === 'string' && nested.errorMessage ? nested.errorMessage : undefined) ?? parentErrorMessage,
          raw: nested,
        });
      }
      continue;
    }

    const internalRef = obj.internal_ref;
    if (internalRef != null && internalRef !== '') {
      lineItems.push({
        internalRef: String(internalRef),
        submissionStatus: parentState ?? 'unknown',
        errorCode: parentErrorCode,
        errorMessage: parentErrorMessage,
        raw: obj,
      });
      continue;
    }

    const batchRef = obj.batch_ref;
    if (batchRef != null && batchRef !== '') {
      batchItems.push({
        batchRef: String(batchRef),
        submissionStatus: parentState ?? 'unknown',
        errorCode: parentErrorCode,
        errorMessage: parentErrorMessage,
        raw: obj,
      });
    }
    // else: nothing to correlate this item to — silently dropped, same as before.
  }

  return { lineItems, batchItems };
}

/** Finds the first array property of `obj` whose elements are all objects carrying their own `internal_ref`. */
function findNestedRows(obj: Record<string, unknown>): Record<string, unknown>[] | null {
  for (const value of Object.values(obj)) {
    if (!Array.isArray(value) || !value.length) continue;
    if (value.every((el) => el && typeof el === 'object' && 'internal_ref' in (el as Record<string, unknown>))) {
      return value as Record<string, unknown>[];
    }
  }
  return null;
}
