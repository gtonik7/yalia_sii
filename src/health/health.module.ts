import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { BullModule } from '@nestjs/bullmq';
import { HealthController } from './health.controller';
import { QUEUES } from '../core/queues/queues.constants';

@Module({
  imports: [TerminusModule, BullModule.registerQueue({ name: QUEUES.JOBS })],
  controllers: [HealthController],
})
export class HealthModule {}
