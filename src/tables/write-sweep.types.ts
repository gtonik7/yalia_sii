/**
 * Payload of a debounced write-sweep job. Deliberately NOT trusted at
 * execution time beyond identifying which group to re-scan — BullMQ collapses
 * concurrent `add()` calls sharing a `jobId`, silently keeping only the first
 * payload (see WriteSweepProcessor), so the processor always re-queries
 * `submission_status='queued'` for this (tableKey, groupValues) instead of
 * acting on whatever rows triggered the enqueue.
 */
export interface WriteSweepJobData {
  tableKey: string;
  /** Column key -> value for every `write.batch.groupBy` column; empty when the template has no groupBy. */
  groupValues: Record<string, string>;
  /** Source connection the affected rows were ingested under; null for non-perConnection templates. Determines which connection the sweep submits through. */
  connectionId: string | null;
}
