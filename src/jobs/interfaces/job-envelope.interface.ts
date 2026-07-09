// Must stay in sync with yalia_hub/src/core/egress/adapters/satellite-egress.adapter.ts
export interface SatelliteJobEnvelope {
  traceId: string;
  hopIndex: number;
  flowId?: string;
  operation?: string;
  defaultMethod?: string;
  connectionId?: string;
  idempotencyKey: string;
  batchId?: string;
  params?: Record<string, unknown>;
  callback?: {
    traceId: string;
    destinationKey: string;
    callbackKey: string;
  };
  payload: unknown;
}

export interface SatelliteQueueJob {
  envelope: SatelliteJobEnvelope;
}
