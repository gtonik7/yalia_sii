/**
 * Payload of an immediate, row-targeted event send. Unlike the old debounced
 * sweep (which re-queried a whole batch group), this carries exactly one row:
 * an edit in `event` mode is submitted on its own, as an array of 1, the moment
 * the form save persists — never batched with the group's other queued rows
 * (those wait for the per-connection internal cron). The processor still
 * re-checks the row is `queued` before sending, so a duplicate/stale job no-ops.
 */
export interface WriteEventJobData {
  tableKey: string;
  /** The single row to submit. */
  rowId: string;
  /** Source connection the row was ingested under. */
  connectionId: string | null;
}
