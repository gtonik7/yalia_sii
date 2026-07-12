import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';

/**
 * Daily sweep that purges rows older than `retentionDays` for templates that
 * opt in (see TableTemplate.retentionDays). Reuses `TableRowsService.deleteRows`
 * — the same code path already exposed on-demand via the datasets API — so
 * there is only one deletion implementation to keep correct.
 *
 * In-process `setInterval` (same pattern as WriteCronService): no Redis, no
 * cross-satellite coordination needed since retention is purely local data
 * hygiene. `lastRun` is in-memory — after a restart the next tick just runs
 * immediately, which is harmless (deleteRows is idempotent: nothing left to
 * delete twice).
 */
@Injectable()
export class TableRetentionCron implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TableRetentionCron.name);
  private readonly tickMs = Number(process.env.TABLE_RETENTION_TICK_MS) || 24 * 3600 * 1000;
  private timer: NodeJS.Timeout | null = null;
  private tickInProgress = false;

  constructor(
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.tick(), this.tickMs);
    this.timer.unref?.();
    this.logger.log(`Table retention cron started (tick ${this.tickMs}ms)`);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private async tick(): Promise<void> {
    if (this.tickInProgress) return;
    this.tickInProgress = true;
    try {
      const templates = (await this.templates.findAll()).filter((t) => t.retentionDays != null && t.retentionDays > 0);
      for (const template of templates) {
        try {
          const { affected } = await this.rows.deleteRows(template, { olderThanDays: template.retentionDays! });
          if (affected > 0) {
            this.logger.log(`Retention purge table=${template.key} olderThanDays=${template.retentionDays} affected=${affected}`);
          }
        } catch (err) {
          this.logger.warn(`Retention purge failed table=${template.key}: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
    } catch (err) {
      this.logger.error(`Table retention cron tick failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      this.tickInProgress = false;
    }
  }
}
