export interface VendorResultItem {
  /** Correlates back to table_rows.id — the `internal_ref` we sent outbound, echoed back verbatim. */
  internalRef: string;
  /** Vendor's own state string, stored verbatim (see note below — no translation layer yet). */
  submissionStatus: string;
  /** The whole per-item payload, stored verbatim into table_rows.sii_response. */
  raw: Record<string, unknown>;
}

/**
 * Normalizes the vendor's callback payload into a flat array of correlatable
 * results — a single-row submission and a batch submission hit the exact
 * same callback contract, so nothing downstream needs to know which one
 * produced this payload (see the plan: "un solo camino de código, sin
 * bifurcar síncrono/asíncrono").
 *
 * Correlation is by `internal_ref` — the row id we stamped on every outbound
 * item (see table-rows.service.ts's submitGroup) — which the vendor is
 * expected to echo back verbatim, rather than by any id the vendor assigns
 * itself.
 *
 * Isolated on purpose: the exact payload shape (`{state,errorCode,internal_ref,
 * timestamp,siiResponse}` per row) and the success literal (assumed to be
 * some form of `"CORRECTO"`) are both unconfirmed with the vendor as of this
 * writing. `submissionStatus` is stored exactly as the vendor sent it —
 * deliberately NOT lowercased/translated into our own queued/pending
 * vocabulary — so this function is the only place that needs to change once
 * the real contract (or a second vendor with a different shape) is confirmed.
 */
export function mapVendorResult(payload: unknown): VendorResultItem[] {
  const items = Array.isArray(payload) ? payload : [payload];
  const out: VendorResultItem[] = [];

  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    const obj = item as Record<string, unknown>;

    const internalRef = obj.internal_ref;
    if (internalRef == null || internalRef === '') continue; // nothing to correlate this row to

    out.push({
      internalRef: String(internalRef),
      submissionStatus: typeof obj.state === 'string' && obj.state ? obj.state : 'unknown',
      raw: obj,
    });
  }

  return out;
}
