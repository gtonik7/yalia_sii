import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { SiiCallbackController } from './sii-callback.controller';
import { SiiResultProcessor } from './sii-result.processor';
import { QUEUES } from '../core/queues/queues.constants';
import { ConnectionsModule } from '../connections/connections.module';

@Module({
  imports: [BullModule.registerQueue({ name: QUEUES.SII_INBOUND }), ConnectionsModule],
  controllers: [SiiCallbackController],
  providers: [SiiResultProcessor],
})
export class CallbacksModule {}
