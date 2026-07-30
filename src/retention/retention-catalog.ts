/**
 * Catálogo de tablas purgables por retención del satélite. Fuente única de verdad de
 * los targets que expone el RetentionController y renderiza la pestaña "Retención".
 *
 * Distinción clave (la que pidió el usuario): sólo las tablas de trazas/ledger se
 * purgan periódicamente; `table_rows` es dato de negocio PERSISTENTE (facturas SII) y
 * su retención se gestiona por plantilla (TableTemplate.retentionDays) en la pestaña
 * Tablas — aquí sólo aparece como fila informativa (readonly), sin purga agresiva.
 *
 * La config mutable de los targets físicos vive en `retention_settings` (hot-editable).
 */

export type RetentionExecutor =
  // Borrado genérico por lotes (progreso real), acotado por ctid.
  | { kind: 'batched'; table: string; dateColumn: string; extraWhere?: string }
  // Hypertable Timescale: drop_chunks por antigüedad (barato, progreso grueso).
  | { kind: 'drop_chunks'; hypertable: string; dateColumn: string };

export interface RetentionTargetDef {
  key: string;
  label: string;
  description: string;
  /** Tabla física para reportar tamaño con pg_total_relation_size. */
  sizeTable: string;
  defaultRetentionDays: number;
  defaultIntervalDays: number;
  minRetentionDays: number;
  defaultEnabled: boolean;
  /** table_rows: sólo informativo (persistente, retención por plantilla). Sin purga/config. */
  readonly?: boolean;
  executor?: RetentionExecutor;
}

export const RETENTION_CATALOG: RetentionTargetDef[] = [
  {
    key: 'table_delete_events',
    label: 'Ledger de borrados',
    description:
      'Registro de cada borrado masivo, con el array de IDs afectados (puede pesar MB por evento). Es traza auditable, no dato de negocio: purgable pasado un tiempo.',
    sizeTable: 'table_delete_events',
    defaultRetentionDays: 30,
    defaultIntervalDays: 1,
    minRetentionDays: 1,
    defaultEnabled: true,
    executor: { kind: 'batched', table: 'table_delete_events', dateColumn: 'created_at' },
  },
  {
    key: 'domain_event_outbox',
    label: 'Outbox de eventos de dominio',
    description:
      'Cola de salida de eventos hacia hub-events. Sólo se purgan los ya drenados (drained_at); los pendientes y dead-letter nunca se tocan.',
    sizeTable: 'domain_event_outbox',
    defaultRetentionDays: 14,
    defaultIntervalDays: 1,
    minRetentionDays: 1,
    defaultEnabled: true,
    executor: { kind: 'batched', table: 'domain_event_outbox', dateColumn: 'drained_at' },
  },
  {
    key: 'table_write_runs',
    label: 'Runs de escritura (telemetría)',
    description:
      'Hypertable Timescale con el historial de intentos de escritura a SII. Telemetría operativa: se purga por chunks enteros (drop_chunks).',
    sizeTable: 'table_write_runs',
    defaultRetentionDays: 30,
    defaultIntervalDays: 1,
    minRetentionDays: 1,
    defaultEnabled: true,
    executor: { kind: 'drop_chunks', hypertable: 'table_write_runs', dateColumn: 'created_at' },
  },
  {
    key: 'table_rows',
    label: 'Registros de tablas (persistente)',
    description:
      'Datos de negocio (facturas SII). NO se purgan como trazas: su retención se configura por plantilla en la pestaña Tablas. Aquí sólo se muestra el volumen.',
    sizeTable: 'table_rows',
    defaultRetentionDays: 0,
    defaultIntervalDays: 0,
    minRetentionDays: 0,
    defaultEnabled: false,
    readonly: true,
  },
];

export function findRetentionTarget(key: string): RetentionTargetDef | undefined {
  return RETENTION_CATALOG.find((t) => t.key === key);
}
