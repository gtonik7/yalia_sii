import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { JobsProcessor } from './jobs.processor';
import { CallbackService } from './callback.service';
import { OperationRegistryModule } from '../operations/operation-registry.module';

@Module({
  imports: [
    BullModule.registerQueue({ name: QUEUES.JOBS }),
    BullModule.registerQueue({ name: QUEUES.HUB_CALLBACKS }),
    OperationRegistryModule,
  ],
  providers: [JobsProcessor, CallbackService],
  exports: [CallbackService],
})
export class JobsModule {}
