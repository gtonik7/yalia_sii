import { Injectable } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource, IsNull, MoreThanOrEqual, Not } from 'typeorm';
import { DomainEventOutbox } from './domain-event-outbox.entity';

export type OutboxEventStatus = 'pending' | 'drained' | 'dead-letter';

export interface OutboxStats {
  pending: number;
  deadLettered: number;
  drainedRecently: number;
  lastError: { message: string; operation: string; connectionId: string; occurredAt: string } | null;
}

export interface OutboxEventRow {
  id: string;
  operation: string;
  connectionId: string;
  attempts: number;
  lastError: string | null;
  spooledAt: string;
  drainedAt: string | null;
  deadLetterAt: string | null;
  status: OutboxEventStatus;
}

const RECENT_WINDOW_MS = 15 * 60 * 1000;
const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

function toStatus(r: Pick<DomainEventOutbox, 'drainedAt' | 'deadLetterAt'>): OutboxEventStatus {
  if (r.deadLetterAt) return 'dead-letter';
  if (r.drainedAt) return 'drained';
  return 'pending';
}

/**
 * Lectura de solo observabilidad sobre `domain_event_outbox`, separada del
 * puerto `OutboxStore` (ese es del camino de escritura/drenado). `markDeadLetter`
 * pone `deadLetterAt` Y `drainedAt` a la vez (ver `typeorm-outbox.store.ts`), así
 * que "drenado con éxito" se cuenta excluyendo `deadLetterAt`.
 */
@Injectable()
export class OutboxStatsService {
  constructor(@InjectDataSource() private readonly ds: DataSource) {}

  async getStats(): Promise<OutboxStats> {
    const repo = this.ds.getRepository(DomainEventOutbox);
    const recentSince = new Date(Date.now() - RECENT_WINDOW_MS);

    const [pending, deadLettered, drainedRecently, lastErrorRow] = await Promise.all([
      repo.count({ where: { drainedAt: IsNull(), deadLetterAt: IsNull() } }),
      repo.count({ where: { deadLetterAt: Not(IsNull()) } }),
      repo.count({ where: { drainedAt: MoreThanOrEqual(recentSince), deadLetterAt: IsNull() } }),
      repo.findOne({ where: { lastError: Not(IsNull()) }, order: { spooledAt: 'DESC' } }),
    ]);

    return {
      pending,
      deadLettered,
      drainedRecently,
      lastError: lastErrorRow
        ? {
            message: lastErrorRow.lastError!,
            operation: lastErrorRow.operation,
            connectionId: lastErrorRow.connectionId,
            occurredAt: lastErrorRow.spooledAt.toISOString(),
          }
        : null,
    };
  }

  async listEvents(params: { status?: OutboxEventStatus; limit?: number }): Promise<OutboxEventRow[]> {
    const limit = Math.min(Math.max(params.limit ?? DEFAULT_LIMIT, 1), MAX_LIMIT);
    const repo = this.ds.getRepository(DomainEventOutbox);

    const where =
      params.status === 'pending'
        ? { drainedAt: IsNull(), deadLetterAt: IsNull() }
        : params.status === 'drained'
          ? { drainedAt: Not(IsNull()), deadLetterAt: IsNull() }
          : params.status === 'dead-letter'
            ? { deadLetterAt: Not(IsNull()) }
            : {};

    const rows = await repo.find({ where, order: { spooledAt: 'DESC' }, take: limit });

    return rows.map((r) => ({
      id: r.id,
      operation: r.operation,
      connectionId: r.connectionId,
      attempts: r.attempts,
      lastError: r.lastError,
      spooledAt: r.spooledAt.toISOString(),
      drainedAt: r.drainedAt?.toISOString() ?? null,
      deadLetterAt: r.deadLetterAt?.toISOString() ?? null,
      status: toStatus(r),
    }));
  }
}
