import { Controller, Get, Query, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { OutboxStatsService, type OutboxEventStatus } from './outbox-stats.service';

const VALID_STATUSES: OutboxEventStatus[] = ['pending', 'drained', 'dead-letter'];

function parseStatus(status?: string): OutboxEventStatus | undefined {
  return VALID_STATUSES.includes(status as OutboxEventStatus) ? (status as OutboxEventStatus) : undefined;
}

function parseLimit(limit?: string): number | undefined {
  const n = Number(limit);
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : undefined;
}

/** Observabilidad de solo lectura del outbox de eventos de dominio (ver src/outbox/). */
@Controller('v1/satellite/outbox')
@UseGuards(MgmtTokenGuard)
export class OutboxStatsController {
  constructor(private readonly stats: OutboxStatsService) {}

  @Get('stats')
  getStats() {
    return this.stats.getStats();
  }

  @Get('events')
  listEvents(@Query('status') status?: string, @Query('limit') limit?: string) {
    return this.stats.listEvents({ status: parseStatus(status), limit: parseLimit(limit) });
  }
}
