import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { RETENTION_CATALOG } from '../retention/retention-catalog';
import { RetentionService } from '../retention/retention.service';
import { OperationRunService } from './operation-run.service';

const DAY_MS = 86_400_000;

/**
 * Barrido periódico que encola una purga `retention.purge` por cada target físico
 * activo cuya cadencia venció (last_run_at + intervalDays). Vive en TablesModule
 * porque necesita OperationRunService (encolar el run); reutiliza RetentionService del
 * RetentionModule para la config/cuenta atrás. Mismo patrón in-proceso que
 * WriteCronService / TableRetentionCron (setInterval, sin coordinación cross-nodo).
 */
@Injectable()
export class RetentionSchedulerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RetentionSchedulerService.name);
  private readonly tickMs = Number(process.env.RETENTION_TICK_MS) || 3600 * 1000; // cada hora
  private timer: NodeJS.Timeout | null = null;
  private tickInProgress = false;

  constructor(
    private readonly retention: RetentionService,
    private readonly runs: OperationRunService,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.tick(), this.tickMs);
    this.timer.unref?.();
    this.logger.log(`Retention scheduler started (tick ${this.tickMs}ms)`);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private async tick(): Promise<void> {
    if (this.tickInProgress) return;
    this.tickInProgress = true;
    try {
      for (const def of RETENTION_CATALOG) {
        if (def.readonly || !def.executor) continue;
        try {
          const cfg = await this.retention.getConfig(def);
          if (!cfg.enabled) continue;
          const dueAt = cfg.lastRunAt ? cfg.lastRunAt.getTime() + cfg.intervalDays * DAY_MS : 0;
          if (Date.now() < dueAt) continue;
          if (await this.runs.findActiveByOperation('retention.purge', def.key)) continue;

          await this.runs.create('retention.purge', def.key, { targetKey: def.key });
          this.logger.log(`Retention scheduled purge target=${def.key} days=${cfg.retentionDays}`);
        } catch (err) {
          this.logger.warn(`Retention schedule failed target=${def.key}: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
    } finally {
      this.tickInProgress = false;
    }
  }
}
