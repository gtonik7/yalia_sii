export interface OperationContext {
  traceId: string;
  hopIndex: number;
  connectionId: string;
  idempotencyKey: string;
  flowId?: string;
  defaultMethod?: string;
  batchId?: string;
  params?: Record<string, unknown>;
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
