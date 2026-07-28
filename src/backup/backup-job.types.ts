import type { RunBackupOpts } from './backup.service';

/**
 * Jobs de la cola BACKUP. `backup` lleva sus opts (destinos/retención/schedule) porque
 * `BackupRun` no los guarda; `restore` sólo lleva el runId y lee sus params del
 * `operation_run` correspondiente (destructivo → confirm ya validado antes de encolar).
 */
export type BackupJobData =
    | { kind: 'backup'; runId: string; opts: RunBackupOpts }
    | { kind: 'restore'; runId: string };
