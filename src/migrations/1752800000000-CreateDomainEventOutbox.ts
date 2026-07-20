import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Crea `domain_event_outbox`: el outbox transaccional de eventos de dominio
 * salientes (ver `src/outbox/`). Cada fila se spoolea en la misma
 * transacción que la escritura de dominio que la origina y el `OutboxDrainCron`
 * la publica luego en `hub-events`.
 *
 * El índice parcial cubre exactamente la query de drenado (`getPending`): filas
 * ni drenadas ni descartadas, en orden de llegada.
 *
 * PREREQUISITO: `gen_random_uuid()` (pg16), ya usado por el resto del esquema.
 */
export class CreateDomainEventOutbox1752800000000 implements MigrationInterface {
  name = 'CreateDomainEventOutbox1752800000000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      CREATE TABLE IF NOT EXISTS "domain_event_outbox" (
        "id" uuid NOT NULL DEFAULT gen_random_uuid(),
        "operation" varchar(64) NOT NULL,
        "payload" jsonb NOT NULL,
        "connection_id" varchar(64) NOT NULL,
        "trace_id" varchar(64),
        "idempotency_key" varchar(128),
        "attempts" int NOT NULL DEFAULT 0,
        "last_error" text,
        "spooled_at" timestamptz NOT NULL DEFAULT now(),
        "drained_at" timestamptz,
        "dead_letter_at" timestamptz,
        CONSTRAINT "pk_domain_event_outbox" PRIMARY KEY ("id")
      );
    `);

    await q.query(`
      CREATE INDEX IF NOT EXISTS "ix_domain_event_outbox_pending"
      ON "domain_event_outbox" ("spooled_at")
      WHERE "drained_at" IS NULL AND "dead_letter_at" IS NULL;
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP TABLE IF EXISTS "domain_event_outbox";`);
  }
}
