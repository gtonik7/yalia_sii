import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Crea `table_write_runs`: el histórico de lotes salientes de escritura
 * (presentación), análogo write del pull `source_poll_runs`. Una fila por
 * llamada `submitGroup()` (un batch = una petición HTTP al sistema externo).
 *
 * Tabla plana (no hypertable de Timescale, a diferencia de source_poll_runs):
 * el volumen es un run por lote y un btree sobre `created_at` basta. Si hiciera
 * falta retention, es una migración posterior.
 *
 * PREREQUISITO: ninguno más allá de la extensión pgcrypto/uuid usada por el
 * resto del esquema (gen_random_uuid ya disponible en pg16).
 */
export class TableWriteRuns1751000004000 implements MigrationInterface {
  name = 'TableWriteRuns1751000004000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      CREATE TABLE IF NOT EXISTS "table_write_runs" (
        "id" uuid NOT NULL DEFAULT gen_random_uuid(),
        "table_key" varchar(128) NOT NULL,
        "connection_id" varchar(64),
        "connection_name" varchar(256),
        "trigger" varchar(16) NOT NULL,
        "status" varchar(16) NOT NULL,
        "batch_id" varchar(64),
        "group_values" jsonb,
        "row_count" int NOT NULL DEFAULT 0,
        "http_status" int,
        "error_message" text,
        "completed_at" timestamptz,
        "created_at" timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT "pk_table_write_runs" PRIMARY KEY ("id")
      );
    `);

    await q.query(`CREATE INDEX IF NOT EXISTS "ix_table_write_runs_table_key" ON "table_write_runs" ("table_key");`);
    await q.query(`CREATE INDEX IF NOT EXISTS "ix_table_write_runs_created_at" ON "table_write_runs" ("created_at");`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP TABLE IF EXISTS "table_write_runs";`);
  }
}
