import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Baseline: crea todas las tablas de yalia_sii desde cero.
 *
 * Escrita a mano (igual que la de yalia_hub) porque `migration:generate`
 * requiere una BD viva. Aplicar ANTES que 1751000001000-TableRowsTimescale.ts
 * y 1751000002000-SourcePollRunsTimescale.ts.
 */
export class Baseline1751000000000 implements MigrationInterface {
  name = 'Baseline1751000000000';

  public async up(q: QueryRunner): Promise<void> {
    // ── source_connections ───────────────────────────────────────────────────
    await q.query(`
      CREATE TABLE "source_connections" (
        "id"                     uuid          NOT NULL DEFAULT gen_random_uuid(),
        "name"                   varchar(256)  NOT NULL,
        "base_url"               varchar(512)  NOT NULL,
        "auth_type"              varchar(16)   NOT NULL DEFAULT 'bearer',
        "credentials_encrypted"  text          NOT NULL DEFAULT '',
        "default_headers"        jsonb         NOT NULL DEFAULT '{}',
        "pagination"             jsonb         NOT NULL,
        "handshake"               jsonb,
        "active"                 boolean       NOT NULL DEFAULT true,
        "created_at"             timestamptz   NOT NULL DEFAULT now(),
        "updated_at"             timestamptz   NOT NULL DEFAULT now(),
        CONSTRAINT "PK_source_connections" PRIMARY KEY ("id")
      )
    `);
    await q.query(`CREATE UNIQUE INDEX "UQ_source_connections_name" ON "source_connections" ("name")`);

    // ── table_templates ───────────────────────────────────────────────────────
    await q.query(`
      CREATE TABLE "table_templates" (
        "id"              uuid          NOT NULL DEFAULT gen_random_uuid(),
        "key"             varchar(128)  NOT NULL,
        "label"           varchar(256)  NOT NULL,
        "description"     text,
        "per_connection"  boolean       NOT NULL DEFAULT false,
        "connection_ids"  jsonb,
        "id_field"        varchar(128)  NOT NULL DEFAULT '',
        "columns"         jsonb         NOT NULL DEFAULT '[]',
        "default_sort"    jsonb,
        "audit"           jsonb,
        "write"           jsonb,
        "created_at"      timestamptz   NOT NULL DEFAULT now(),
        "updated_at"      timestamptz   NOT NULL DEFAULT now(),
        CONSTRAINT "PK_table_templates" PRIMARY KEY ("id")
      )
    `);
    await q.query(`CREATE UNIQUE INDEX "UQ_table_templates_key" ON "table_templates" ("key")`);

    // ── table_rows ────────────────────────────────────────────────────────────
    // La PK (id) será reemplazada por (id, created_at) en
    // 1751000001000-TableRowsTimescale.ts para cumplir el requisito de
    // TimescaleDB de incluir la columna de partición en la clave primaria.
    await q.query(`
      CREATE TABLE "table_rows" (
        "id"               uuid          NOT NULL DEFAULT gen_random_uuid(),
        "table_key"        varchar(128)  NOT NULL,
        "connection_id"    varchar(64)   NOT NULL DEFAULT '',
        "data"             jsonb         NOT NULL DEFAULT '{}',
        "trace_id"         varchar(64),
        "write_status"     varchar(8),
        "write_error"      text,
        "last_written_at"  timestamptz,
        "external_ref"     text,
        "created_at"       timestamptz   NOT NULL DEFAULT now(),
        CONSTRAINT "PK_table_rows" PRIMARY KEY ("id")
      )
    `);
    await q.query(`
      CREATE INDEX "IDX_table_rows_key_conn_created"
        ON "table_rows" ("table_key", "connection_id", "created_at")
    `);

    // ── source_poll_states ────────────────────────────────────────────────────
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

    // ── source_poll_runs ──────────────────────────────────────────────────────
    // La PK (id) será reemplazada por (id, created_at) en
    // 1751000002000-SourcePollRunsTimescale.ts (mismo motivo que table_rows).
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

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP TABLE IF EXISTS "source_poll_runs"`);
    await q.query(`DROP TABLE IF EXISTS "source_poll_states"`);
    await q.query(`DROP TABLE IF EXISTS "table_rows"`);
    await q.query(`DROP TABLE IF EXISTS "table_templates"`);
    await q.query(`DROP TABLE IF EXISTS "source_connections"`);
  }
}
