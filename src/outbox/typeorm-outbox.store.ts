import { Injectable } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource, EntityManager, IsNull } from 'typeorm';
import type { DomainEvent, OutboxRecord, OutboxStore } from './domain-event.types';
import { DomainEventOutbox } from './domain-event-outbox.entity';

/**
 * Adapter Postgres/TypeORM del puerto `OutboxStore`.
 *
 * `save()` acepta un `EntityManager` opcional (el handle de transacción del
 * llamante, pasado como `tx`) para spoolear el evento en la MISMA transacción que
 * la escritura de dominio; sin él, escribe en autocommit. El resto de operaciones
 * son del drenado, siempre fuera de transacción de dominio.
 */
@Injectable()
export class TypeOrmOutboxStore implements OutboxStore {
  constructor(@InjectDataSource() private readonly ds: DataSource) {}

  async save(event: DomainEvent, tx?: unknown): Promise<void> {
    const manager = (tx as EntityManager | undefined) ?? this.ds.manager;
    const repo = manager.getRepository(DomainEventOutbox);
    await repo.save(
      repo.create({
        operation: event.operation,
        payload: event.payload,
        connectionId: event.connectionId,
        traceId: event.traceId ?? null,
        idempotencyKey: event.idempotencyKey ?? null,
      }),
    );
  }

  async getPending(limit: number): Promise<OutboxRecord[]> {
    const rows = await this.ds.getRepository(DomainEventOutbox).find({
      where: { drainedAt: IsNull(), deadLetterAt: IsNull() },
      order: { spooledAt: 'ASC' },
      take: limit,
    });
    return rows.map((r) => ({
      id: r.id,
      attempts: r.attempts,
      event: {
        operation: r.operation,
        payload: r.payload,
        connectionId: r.connectionId,
        traceId: r.traceId ?? undefined,
        idempotencyKey: r.idempotencyKey ?? undefined,
      },
    }));
  }

  async markDrained(id: string): Promise<void> {
    await this.ds.getRepository(DomainEventOutbox).update(id, { drainedAt: new Date() });
  }

  async recordAttempt(id: string, error: string): Promise<void> {
    const repo = this.ds.getRepository(DomainEventOutbox);
    await repo.increment({ id }, 'attempts', 1);
    await repo.update(id, { lastError: error.slice(0, 500) });
  }

  async markDeadLetter(id: string): Promise<void> {
    await this.ds
      .getRepository(DomainEventOutbox)
      .update(id, { deadLetterAt: new Date(), drainedAt: new Date() });
  }
}
