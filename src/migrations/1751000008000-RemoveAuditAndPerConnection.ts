import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Retira la auditoría automática (pull) y `perConnection` como opción
 * configurable de `table_templates`:
 *  - El módulo `audit/` (SourcePollService/Controller, `source_poll_runs`,
 *    `source_poll_states`, operación `table.audit.poll`) se elimina — no se
 *    usaba.
 *  - `perConnection` deja de ser un toggle: todas las filas de toda tabla se
 *    clasifican siempre por `connection_id` (`table_rows.connection_id`, que
 *    ya existía y se sigue usando incondicionalmente).
 *
 * Sin backfill: confirmado que no hay filas de producción/desarrollo que
 * dependieran de `perConnection = false` en el momento de esta migración.
 */
export class RemoveAuditAndPerConnection1751000008000 implements MigrationInterface {
  name = 'RemoveAuditAndPerConnection1751000008000';

  public async up(q: QueryRunner): Promise<void> {
    // `source_poll_runs` es una hypertable de Timescale con retention policy
    // (1751000002000-SourcePollRunsTimescale.ts) — hay que quitar la policy
    // antes de dropear la tabla, mismo patrón que el down() de esa migración.
    await q.query(`
      DO $$ BEGIN
        PERFORM remove_retention_policy('source_poll_runs', if_exists => true);
      EXCEPTION WHEN undefined_function THEN NULL;
      END $$;
    `);

    await q.query(`DROP TABLE IF EXISTS "source_poll_runs"`);
    await q.query(`DROP TABLE IF EXISTS "source_poll_states"`);

    await q.query(`ALTER TABLE "table_templates" DROP COLUMN "audit"`);
    await q.query(`ALTER TABLE "table_templates" DROP COLUMN "per_connection"`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_templates" ADD COLUMN "per_connection" boolean NOT NULL DEFAULT false`);
    await q.query(`ALTER TABLE "table_templates" ADD COLUMN "audit" jsonb`);

    // Limitación aceptada del rollback: recrea las tablas simples, no la
    // hypertable/retention policy de source_poll_runs (igual que el down() de
    // 1751000002000-SourcePollRunsTimescale.ts no revierte create_hypertable).
    await q.query(`
      CREATE TABLE "source_poll_states" (
        "id"                uuid          NOT NULL DEFAULT gen_random_uuid(),
        "table_key"         varchar(128)  NOT NULL,
        "connection_id"     varchar(64)   NOT NULL,
        "last_updated_at"   text,
        "total_seen"        integer       NOT NULL DEFAULT 0,
        "last_run_at"       timestamptz,
        "created_at"        timestamptz   NOT NULL DEFAULT now(),
        "updated_at"        timestamptz   NOT NULL DEFAULT now(),
        CONSTRAINT "PK_source_poll_states" PRIMARY KEY ("id")
      )
    `);
    await q.query(`
      CREATE UNIQUE INDEX "UQ_source_poll_states_key_conn"
        ON "source_poll_states" ("table_key", "connection_id")
    `);

    await q.query(`
      CREATE TABLE "source_poll_runs" (
        "id"                uuid          NOT NULL DEFAULT gen_random_uuid(),
        "table_key"         varchar(128)  NOT NULL,
        "connection_id"     varchar(64)   NOT NULL,
        "connection_name"   varchar(256),
        "trigger"           varchar(16)   NOT NULL,
        "status"            varchar(16)   NOT NULL DEFAULT 'running',
        "since"             text,
        "pages"             integer       NOT NULL DEFAULT 0,
        "fetched"           integer       NOT NULL DEFAULT 0,
        "inserted"          integer       NOT NULL DEFAULT 0,
        "upserted"          integer       NOT NULL DEFAULT 0,
        "error_message"     text,
        "completed_at"      timestamptz,
        "created_at"        timestamptz   NOT NULL DEFAULT now(),
        CONSTRAINT "PK_source_poll_runs" PRIMARY KEY ("id")
      )
    `);
    await q.query(`CREATE INDEX "IDX_source_poll_runs_table_key" ON "source_poll_runs" ("table_key")`);
    await q.query(`CREATE INDEX "IDX_source_poll_runs_connection_id" ON "source_poll_runs" ("connection_id")`);
  }
}
