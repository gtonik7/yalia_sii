import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ScheduleModule } from '@nestjs/schedule';
import { QUEUES } from '../core/queues/queues.constants';
import { SatelliteAnnounceService } from './satellite-announce.service';
import { OperationRegistryModule } from '../operations/operation-registry.module';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    BullModule.registerQueue({ name: QUEUES.HUB_ANNOUNCE }),
    OperationRegistryModule,
  ],
  providers: [SatelliteAnnounceService],
})
export class AnnounceModule {}
