/**
 * Metadatos de la "carga" (unidad de ingesta) a la que pertenece un evento,
 * propagados por el envelope del hub (transform-independientes, ver
 * SatelliteJobEnvelope). Los emite el satélite de origen cuando fragmenta una
 * unidad en varios eventos (SFTP: un fichero >= 1MB → N lotes) para que el
 * destino pueda retener las filas hasta que la carga esté completa y así no
 * enviar un lote a medias. Ausente en eventos únicos (webhook / fichero pequeño).
 */
export interface LoadMeta {
  /** Identidad estable de la carga, idéntica en todos sus lotes y su sello. */
  loadId: string;
  /** Índice de este lote dentro de la carga (presente en eventos de datos). */
  batchIndex?: number;
  /** Total de lotes de la carga — solo lo lleva el evento de sello. */
  totalBatches?: number;
  /** true en el evento de sello (fin de carga); su payload no lleva filas. */
  loadComplete?: boolean;
}

export interface OperationContext {
  traceId: string;
  hopIndex: number;
  connectionId: string;
  idempotencyKey: string;
  flowId?: string;
  defaultMethod?: string;
  batchId?: string;
  params?: Record<string, unknown>;
  loadMeta?: LoadMeta;
}

export interface OperationResult {
  status: 'ok' | 'error';
  externalRef?: string;
  data?: unknown;
  error?: {
    code: string;
    message: string;
  };
}

export interface OperationHandler {
  readonly operationKey: string;
  readonly payloadExample?: Record<string, unknown>;
  execute(payload: unknown, ctx: OperationContext): Promise<OperationResult>;
}
