import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job, UnrecoverableError } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { OperationRegistryService } from '../operations/operation-registry.service';
import { CallbackService } from './callback.service';
import { SatelliteQueueJob } from './interfaces/job-envelope.interface';
import { OperationResult } from '../operations/operation.interface';

@Processor(QUEUES.JOBS, { concurrency: Number(process.env.WORKER_CONCURRENCY) || 10 })
export class JobsProcessor extends WorkerHost {
  private readonly logger = new Logger(JobsProcessor.name);

  constructor(
    private readonly operations: OperationRegistryService,
    private readonly callback: CallbackService,
  ) {
    super();
  }

  async process(job: Job<SatelliteQueueJob>): Promise<void> {
    const { envelope } = job.data;

    this.logger.debug(
      `Processing job trace=${envelope.traceId} hop=${envelope.hopIndex} op=${envelope.operation ?? 'none'}`,
    );

    let result: OperationResult;

    if (!envelope.operation) {
      result = { status: 'ok', data: envelope.payload };
    } else if (!this.operations.has(envelope.operation)) {
      result = {
        status: 'error',
        error: {
          code: 'UNKNOWN_OPERATION',
          message: `No handler registered for operation "${envelope.operation}"`,
        },
      };
    } else {
      try {
        const handler = this.operations.get(envelope.operation);
        result = await handler.execute(envelope.payload, {
          traceId: envelope.traceId,
          hopIndex: envelope.hopIndex,
          connectionId: envelope.connectionId ?? '',
          idempotencyKey: envelope.idempotencyKey,
          flowId: envelope.flowId,
          defaultMethod: envelope.defaultMethod,
          batchId: envelope.batchId,
          params: envelope.params,
        });
      } catch (err) {
        const e = err as Error;
        this.logger.error(`Operation "${envelope.operation}" failed trace=${envelope.traceId}: ${e.message}`);
        result = { status: 'error', error: { code: 'OPERATION_FAILED', message: e.message } };
      }
    }

    if (envelope.callback) {
      await this.callback.send(envelope.callback, result);
    } else if (result.status === 'error') {
      const msg = result.error?.message ?? 'operation returned error';
      this.logger.error(
        `Operation "${envelope.operation ?? 'none'}" failed (no callback) trace=${envelope.traceId}: ${msg}`,
      );
      throw new UnrecoverableError(msg);
    }
  }
}
