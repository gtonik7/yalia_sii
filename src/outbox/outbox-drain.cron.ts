import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { InjectQueue } from '@nestjs/bullmq';
import type { Queue } from 'bullmq';
import { DEFAULT_JOB_OPTS, QUEUES } from '../core/queues/queues.constants';
import type { HubEventJob, OutboxRecord } from './domain-event.types';
import { TypeOrmOutboxStore } from './typeorm-outbox.store';

/** Identidad de este satélite; se copia a `sourceKey` del envelope de `hub-events`. */
const SOURCE_KEY = 'sii';
const MAX_ATTEMPTS = 5;

/**
 * Drena el outbox hacia la cola `hub-events` cada 5s. Mismo patrón que
 * `yalia_netsuite/src/outbox/sat-outbox-drain.cron.ts`, pero publicando a una cola
 * BullMQ en lugar de invocar un callback. Ante fallo de publicación reintenta
 * (contando intentos); superado el máximo, mueve a dead-letter.
 */
@Injectable()
export class OutboxDrainCron {
  private readonly logger = new Logger(OutboxDrainCron.name);

  constructor(
    private readonly store: TypeOrmOutboxStore,
    @InjectQueue(QUEUES.HUB_EVENTS) private readonly queue: Queue<HubEventJob>,
  ) {}

  @Cron('*/5 * * * * *')
  async drain(): Promise<void> {
    const pending = await this.store.getPending(50);
    if (pending.length === 0) return;

    for (const item of pending) {
      if (item.attempts >= MAX_ATTEMPTS) {
        this.logger.error(
          `Event ${item.id} exceeded max attempts (${MAX_ATTEMPTS}), moving to dead letter`,
        );
        await this.store.markDeadLetter(item.id);
        continue;
      }

      try {
        await this.queue.add('event', this.toEnvelope(item), DEFAULT_JOB_OPTS);
        await this.store.markDrained(item.id);
        this.logger.log(
          `Drained event ${item.id} operation=${item.event.operation} → hub-events`,
        );
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await this.store.recordAttempt(item.id, msg);
        this.logger.warn(
          `Failed to drain event ${item.id} (attempt ${item.attempts + 1}/${MAX_ATTEMPTS}): ${msg}`,
        );
      }
    }
  }

  private toEnvelope(item: OutboxRecord): HubEventJob {
    const e = item.event;
    return {
      sourceKey: SOURCE_KEY,
      operation: e.operation,
      payload: e.payload,
      connectionId: e.connectionId,
      idempotencyKey: e.idempotencyKey,
      traceId: e.traceId,
      flowId: e.flowId,
      routeKey: e.routeKey,
    };
  }
}
