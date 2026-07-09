import { Module } from '@nestjs/common';
import { CapabilitiesController } from './capabilities.controller';
import { SatelliteStatusController } from './satellite-status.controller';

@Module({
  controllers: [CapabilitiesController, SatelliteStatusController],
})
export class CapabilitiesModule {}
