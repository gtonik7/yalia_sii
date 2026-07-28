/**
 * Collapses several rows of one outbound chunk into a single payload item:
 * the `write.batch.groupBy` columns (already partitioned to share the same
 * value across every row in the chunk — see `partitionAndSubmit`) are lifted
 * to the top level ("cabecera"); every other field stays nested per-row
 * under `rowsField`, regardless of whether its value happens to coincide
 * across rows. Each line still carries its own `internal_ref` so the
 * vendor's result callback can correlate back to the individual row (see
 * `mapVendorResult`). `batch_ref` is the group's own `batch_id`, for when the
 * vendor can only report a single status for the whole group.
 */
export function buildCollapsedPayloadItem(
    rows: { id: string; data: Record<string, unknown> }[],
    groupByKeys: string[],
    rowsField: string,
    batchId: string
): Record<string, unknown> {
    const common: Record<string, unknown> = {};
    for (const key of groupByKeys) {
        const row = rows.find((r) => key in r.data);
        if (row) common[key] = row.data[key];
    }

    const lines = rows.map((r) => {
        const line: Record<string, unknown> = { internal_ref: r.id };
        for (const [key, value] of Object.entries(r.data)) {
            if (!groupByKeys.includes(key)) line[key] = value;
        }
        return line;
    });

    return { batch_ref: batchId, ...common, [rowsField]: lines };
}
