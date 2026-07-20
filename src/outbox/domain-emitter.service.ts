import { Injectable } from '@nestjs/common';
import type { DomainEvent } from './domain-event.types';
import { TypeOrmOutboxStore } from './typeorm-outbox.store';

/**
 * Punto de entrada para emitir un evento de dominio hacia el hub.
 *
 * Outbox puro: `emit()` SOLO escribe en el outbox; nunca hace enqueue inline. Así
 * el evento se spoolea dentro de la transacción del llamante (pasando `tx`) y se
 * publica en `hub-events` únicamente tras el commit, de forma diferida por
 * `OutboxDrainCron`. Esto garantiza que no se pierda ni se publique un evento cuya
 * transacción de dominio acabó revirtiéndose.
 */
@Injectable()
export class DomainEmitterService {
  constructor(private readonly store: TypeOrmOutboxStore) {}

  emit(event: DomainEvent, tx?: unknown): Promise<void> {
    return this.store.save(event, tx);
  }
}
