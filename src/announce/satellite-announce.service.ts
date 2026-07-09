import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import type { Queue } from 'bullmq';
import { QUEUES, DEFAULT_JOB_OPTS } from '../core/queues/queues.constants';
import { OperationRegistryService } from '../operations/operation-registry.service';
import type { Env } from '../config/env';

export interface AnnounceEnvelope {
  key: string;
  name?: string;
  managementUrl?: string;
  capabilities: string[];
  version?: string;
  ts: number;
}

@Injectable()
export class SatelliteAnnounceService implements OnModuleInit {
  private readonly logger = new Logger(SatelliteAnnounceService.name);

  constructor(
    @InjectQueue(QUEUES.HUB_ANNOUNCE) private readonly hubAnnounce: Queue,
    private readonly config: ConfigService<Env, true>,
    private readonly operations: OperationRegistryService,
  ) {}

  private buildEnvelope(): AnnounceEnvelope {
    const explicitUrl = this.config.get('SATELLITE_MGMT_URL', { infer: true });
    const host = this.config.get('SATELLITE_HOST', { infer: true }) ?? 'localhost';
    const port = this.config.get('PORT', { infer: true });
    const managementUrl = explicitUrl ?? `http://${host}:${port}`;
    return {
      key: this.config.get('SATELLITE_KEY', { infer: true }),
      name: this.config.get('SATELLITE_NAME', { infer: true }),
      managementUrl,
      capabilities: this.operations.list(),
      version: process.env.npm_package_version,
      ts: Date.now(),
    };
  }

  private async announce(): Promise<void> {
    const envelope = this.buildEnvelope();
    await this.hubAnnounce.add('hub.announce', envelope, {
      ...DEFAULT_JOB_OPTS,
      jobId: `announce-${envelope.key}`,
    });
  }

  async onModuleInit(): Promise<void> {
    try {
      await this.announce();
      this.logger.log(
        `Announce inicial enviado → hub-announce key=${this.config.get('SATELLITE_KEY', { infer: true })}`,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.warn(`Announce inicial falló (reintentará por heartbeat): ${msg}`);
    }
  }

  @Cron('*/30 * * * * *')
  async heartbeat(): Promise<void> {
    try {
      await this.announce();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.warn(`Heartbeat de announce falló: ${msg}`);
    }
  }
}
