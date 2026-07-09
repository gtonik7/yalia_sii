import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { SourceConnectionsService } from '../connections/source-connections.service';
import { TableTemplatesService } from './table-templates.service';
import { TableWriteBatchService } from './table-write-batch.service';

/**
 * yalia_sii's own internal write cron — per connection, in-process, and
 * deliberately NOT backed by a Redis queue: once rows land in the tables they
 * sit in `submission_status='queued'`, and this service is what collects them,
 * groups them (by the template's `write.batch.groupBy`), submits each batch and
 * flips the rows to `sent`/`pending`. It reverses (for this satellite only) the
 * "hub = único scheduler" migration: the send cadence lives on the connection
 * (`writeCronIntervalSec`), not on a hub flow origin.
 *
 * Solo barre las tablas *seleccionadas* para cron: las plantillas con
 * `write.trigger==='schedule'`. Las de modo `'event'` se envían por debounce en
 * la propia edición y quedan fuera de este barrido. Cada tabla saca como mucho
 * `write.batch.maxRecordsPerPoll` filas por pasada (default 10.000); el resto
 * espera a la siguiente.
 *
 * A single supervisor `setInterval` ticks at a fixed granularity and, each tick,
 * re-reads the connections from Postgres and runs any whose interval has
 * elapsed. Re-reading every tick means connection add/edit/disable is picked up
 * on its own, with no cross-module notification (which would create a
 * ConnectionsModule↔TablesModule cycle). `lastRun` is in-memory: after a restart
 * a connection simply sweeps on the next due check.
 */
@Injectable()
export class WriteCronService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WriteCronService.name);
  /** Supervisor granularity; a connection's interval is effectively rounded up to this. */
  private readonly tickMs = Number(process.env.WRITE_CRON_TICK_MS) || 15_000;
  private timer: NodeJS.Timeout | null = null;
  private tickInProgress = false;
  private readonly lastRun = new Map<string, number>();
  private readonly running = new Set<string>();

  constructor(
    private readonly connections: SourceConnectionsService,
    private readonly templates: TableTemplatesService,
    private readonly writeBatch: TableWriteBatchService,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.supervisorTick(), this.tickMs);
    // Don't keep the event loop alive just for the cron (matters in tests/CLI).
    this.timer.unref?.();
    this.logger.log(`Internal write cron supervisor started (tick ${this.tickMs}ms)`);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /** One supervisor pass: dispatch a sweep for every connection whose interval elapsed. */
  private async supervisorTick(): Promise<void> {
    if (this.tickInProgress) return; // never let a slow DB read overlap ticks
    this.tickInProgress = true;
    try {
      const conns = await this.connections.listWriteCronConnections();
      const now = Date.now();
      for (const { id, intervalSec } of conns) {
        if (this.running.has(id)) continue; // previous sweep still in flight
        const last = this.lastRun.get(id) ?? 0;
        if (now - last < intervalSec * 1000) continue; // not due yet
        this.lastRun.set(id, now);
        this.running.add(id);
        void this.sweepConnection(id).finally(() => this.running.delete(id));
      }
    } catch (err) {
      this.logger.error(`Write cron supervisor tick failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      this.tickInProgress = false;
    }
  }

  /** Sweep every schedule-mode table for one connection's queued rows. */
  private async sweepConnection(connectionId: string): Promise<void> {
    // Solo las tablas seleccionadas para cron (write.trigger==='schedule'). Las
    // 'event' se envían por debounce en la propia edición (enqueueEventSend /
    // write-event.processor.ts), no por este barrido.
    const templates = (await this.templates.findAll()).filter((t) => t.write && t.write.trigger === 'schedule');
    for (const template of templates) {
      try {
        await this.writeBatch.submitAllQueued(template, 'schedule', connectionId);
      } catch (err) {
        this.logger.warn(
          `Write cron sweep table=${template.key} conn=${connectionId} failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
  }
}
