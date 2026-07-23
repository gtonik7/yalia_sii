import { Module } from '@nestjs/common';
import { CapabilitiesController } from './capabilities.controller';
import { SatelliteStatusController } from './satellite-status.controller';
import { SatelliteMetricsController } from './satellite-metrics.controller';

@Module({
  controllers: [CapabilitiesController, SatelliteStatusController, SatelliteMetricsController],
})
export class CapabilitiesModule {}
