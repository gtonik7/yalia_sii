import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { TypeOrmModule } from '@nestjs/typeorm';
import { QUEUES } from '../core/queues/queues.constants';
import { DomainEventOutbox } from './domain-event-outbox.entity';
import { TypeOrmOutboxStore } from './typeorm-outbox.store';
import { DomainEmitterService } from './domain-emitter.service';
import { OutboxDrainCron } from './outbox-drain.cron';
import { OutboxStatsService } from './outbox-stats.service';
import { OutboxStatsController } from './outbox-stats.controller';

/**
 * Emisión outbound de eventos de dominio de sii hacia el hub.
 *
 * Registra la entidad del outbox (para que TypeORM la conozca vía
 * `autoLoadEntities`), la cola `hub-events` como productor, el adapter Postgres y
 * el cron de drenado. Exporta `DomainEmitterService` para que quede inyectable en
 * quien importe este módulo (p.ej. `TablesModule`).
 *
 * Patrón local y autocontenido: cualquier otro satélite copia `src/outbox/` y
 * adapta el store a su persistencia. No hay paquete compartido ni registry.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([DomainEventOutbox]),
    BullModule.registerQueue({ name: QUEUES.HUB_EVENTS }),
  ],
  controllers: [OutboxStatsController],
  providers: [TypeOrmOutboxStore, DomainEmitterService, OutboxDrainCron, OutboxStatsService],
  exports: [DomainEmitterService],
})
export class OutboxModule {}
