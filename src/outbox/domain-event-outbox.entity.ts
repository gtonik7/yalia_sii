import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn } from 'typeorm';

/**
 * Outbox transaccional de eventos de dominio salientes. Una fila = un
 * {@link DomainEvent} spooleado por `DomainEmitterService.emit()` dentro de la
 * misma transacción que la escritura de dominio que lo originó (ver
 * `TableRowsService.submitGroup` → `commitOutcome`). El `OutboxDrainCron` local
 * (`src/outbox/`) lo publica luego en la cola `hub-events`.
 *
 * El índice parcial de drenado (`spooled_at` WHERE no drenado ni dead-letter) lo
 * crea la migración `CreateDomainEventOutbox`; no se declara aquí porque el
 * esquema es migrations-only (`synchronize:false`).
 */
@Entity('domain_event_outbox')
export class DomainEventOutbox {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  /** Discriminador del hecho: 'emitida.sent' | 'emitida.error' | … */
  @Column({ type: 'varchar', length: 64 })
  operation!: string;

  @Column({ type: 'jsonb' })
  payload!: unknown;

  @Column({ type: 'varchar', length: 64, name: 'connection_id' })
  connectionId!: string;

  @Column({ type: 'varchar', length: 64, name: 'trace_id', nullable: true })
  traceId!: string | null;

  @Column({ type: 'varchar', length: 128, name: 'idempotency_key', nullable: true })
  idempotencyKey!: string | null;

  /** Intentos de publicación a `hub-events` (para el corte a dead-letter). */
  @Column({ type: 'int', default: 0 })
  attempts!: number;

  @Column({ type: 'text', name: 'last_error', nullable: true })
  lastError!: string | null;

  @CreateDateColumn({ type: 'timestamptz', name: 'spooled_at' })
  spooledAt!: Date;

  @Column({ type: 'timestamptz', name: 'drained_at', nullable: true })
  drainedAt!: Date | null;

  @Column({ type: 'timestamptz', name: 'dead_letter_at', nullable: true })
  deadLetterAt!: Date | null;
}
