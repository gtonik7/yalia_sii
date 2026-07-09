import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { AeatCallbackController } from './aeat-callback.controller';
import { AeatResultProcessor } from './aeat-result.processor';
import { QUEUES } from '../core/queues/queues.constants';

@Module({
  imports: [BullModule.registerQueue({ name: QUEUES.AEAT_INBOUND })],
  controllers: [AeatCallbackController],
  providers: [AeatResultProcessor],
})
export class CallbacksModule {}
