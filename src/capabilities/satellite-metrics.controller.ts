import { Controller, Get, UseGuards } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource } from '@nestjs/typeorm';
import type { DataSource } from 'typeorm';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { collectHostMetrics } from './runtime-metrics';
import type { Env } from '../config/env';

/**
 * Métricas de recursos del satélite (host + proceso + BD), consumidas por el panel
 * Sistema del hub. Protegido con MgmtTokenGuard: expone datos de infraestructura y
 * no debe ser público como `/v1/satellite/status`. Este satélite usa Postgres.
 */
@Controller('v1/satellite')
@UseGuards(MgmtTokenGuard)
export class SatelliteMetricsController {
  constructor(
    private readonly config: ConfigService<Env, true>,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  @Get('metrics')
  async getMetrics() {
    const host = await collectHostMetrics();
    return {
      satelliteKey: this.config.get('SATELLITE_KEY', { infer: true }),
      uptimeSeconds: Math.round(process.uptime()),
      process: {
        rssBytes: host.processRssBytes,
        heapUsedBytes: host.processHeapUsedBytes,
        heapTotalBytes: host.processHeapTotalBytes,
      },
      host,
      db: await this.dbMetrics(),
    };
  }

  private async dbMetrics(): Promise<Record<string, unknown> | null> {
    try {
      const rows = await this.dataSource.query<{ size: string }[]>(
        'SELECT pg_database_size(current_database()) AS size',
      );
      return { kind: 'postgres', sizeBytes: Number(rows[0]?.size ?? 0) };
    } catch {
      return null;
    }
  }
}
