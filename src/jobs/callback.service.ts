import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import type { Queue } from 'bullmq';
import { QUEUES, DEFAULT_JOB_OPTS } from '../core/queues/queues.constants';
import type { OperationResult } from '../operations/operation.interface';

export interface CallbackCoordinates {
  traceId: string;
  destinationKey: string;
  callbackKey: string;
}

export interface HubCallbackJob {
  traceId: string;
  callbackKey: string;
  destinationKey: string;
  result: OperationResult;
}

@Injectable()
export class CallbackService {
  private readonly logger = new Logger(CallbackService.name);

  constructor(@InjectQueue(QUEUES.HUB_CALLBACKS) private readonly hubCallbacks: Queue) {}

  async send(coords: CallbackCoordinates, result: OperationResult): Promise<void> {
    const jobData: HubCallbackJob = {
      traceId: coords.traceId,
      callbackKey: coords.callbackKey,
      destinationKey: coords.destinationKey,
      result,
    };
    await this.hubCallbacks.add('hub.callback', jobData, {
      ...DEFAULT_JOB_OPTS,
      jobId: `${coords.traceId}-${coords.callbackKey}`,
    });
    this.logger.log(
      `Callback queued → hub:callbacks traceId=${coords.traceId} callbackKey=${coords.callbackKey}`,
    );
  }
}
