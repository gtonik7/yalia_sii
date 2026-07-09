import { Controller, Get } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OperationRegistryService } from '../operations/operation-registry.service';
import type { Env } from '../config/env';

/**
 * Unauthenticated status probe consumed by the hub (`SatellitesController.probe`
 * hits this without a mgmt token). Reports the operations this satellite can run
 * so the hub flow editor can offer them in the destination's `operation` selector.
 */
@Controller('v1/satellite')
export class SatelliteStatusController {
  constructor(
    private readonly operations: OperationRegistryService,
    private readonly config: ConfigService<Env, true>,
  ) {}

  @Get('status')
  getStatus() {
    const keys = this.operations.list();
    const operationSchemas = Object.fromEntries(
      keys.map((key) => [key, this.operations.get(key).payloadExample ?? null]),
    );
    return {
      satelliteKey: this.config.get('SATELLITE_KEY', { infer: true }),
      operations: keys,
      operationSchemas,
      // Webhook is destination-only: no cron/poll-triggerable operations.
      cronOperations: [],
      uptime: process.uptime(),
    };
  }
}
