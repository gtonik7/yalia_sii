import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService, TypeOrmHealthIndicator } from '@nestjs/terminus';
import { InjectQueue } from '@nestjs/bullmq';
import type { Queue } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly db: TypeOrmHealthIndicator,
    @InjectQueue(QUEUES.JOBS) private readonly jobsQueue: Queue,
  ) {}

  @Get('live')
  liveness() {
    return { status: 'ok' };
  }

  @Get('ready')
  @HealthCheck()
  readiness() {
    return this.health.check([
      () => this.db.pingCheck('database'),
      async () => {
        const client = (await this.jobsQueue.client) as unknown as { ping(): Promise<string> };
        const pong = await client.ping();
        return { redis: { status: pong === 'PONG' ? 'up' : 'down' } };
      },
    ]);
  }
}
