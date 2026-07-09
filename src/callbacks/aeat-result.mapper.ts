export interface VendorResultItem {
  /** Correlates back to table_rows.external_ref — the vendor's `invoiceId`. */
  externalRef: string;
  /** Vendor's own state string, stored verbatim (see note below — no translation layer yet). */
  submissionStatus: string;
  /** The whole per-item payload, stored verbatim into table_rows.aeat_response. */
  raw: Record<string, unknown>;
}

/**
 * Normalizes the vendor's callback payload into a flat array of correlatable
 * results — a single-row submission and a batch submission hit the exact
 * same callback contract, so nothing downstream needs to know which one
 * produced this payload (see the plan: "un solo camino de código, sin
 * bifurcar síncrono/asíncrono").
 *
 * Isolated on purpose: the exact payload shape (`{state,errorCode,invoiceId,
 * timestamp,aeatResponse}` per row) and the success literal (assumed to be
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

    const externalRef = obj.invoiceId;
    if (externalRef == null || externalRef === '') continue; // nothing to correlate this row to

    out.push({
      externalRef: String(externalRef),
      submissionStatus: typeof obj.state === 'string' && obj.state ? obj.state : 'unknown',
      raw: obj,
    });
  }

  return out;
}
