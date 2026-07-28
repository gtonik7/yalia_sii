import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Sistema de backup programado: `backup_schedules` (config: qué tablas, cron,
 * destinos, retención) + `backup_runs` (historial + registro del artefacto).
 * Ejecutado por `BackupCron`; el artefacto se produce con `pg_dump -Fc`.
 */
export class BackupSchedules1752900000000 implements MigrationInterface {
    name = 'BackupSchedules1752900000000';

    public async up(q: QueryRunner): Promise<void> {
        await q.query(`
            CREATE TABLE IF NOT EXISTS "backup_schedules" (
                "id" uuid NOT NULL DEFAULT gen_random_uuid(),
                "name" varchar(256) NOT NULL,
                "tables" jsonb NOT NULL DEFAULT '[]',
                "cron_expression" varchar(128) NOT NULL,
                "destinations" jsonb NOT NULL DEFAULT '{}',
                "retention_count" int NOT NULL DEFAULT 7,
                "enabled" boolean NOT NULL DEFAULT true,
                "last_run_at" timestamptz,
                "created_at" timestamptz NOT NULL DEFAULT now(),
                "updated_at" timestamptz NOT NULL DEFAULT now(),
                CONSTRAINT "PK_backup_schedules" PRIMARY KEY ("id")
            );
        `);

        await q.query(`
            CREATE TABLE IF NOT EXISTS "backup_runs" (
                "id" uuid NOT NULL DEFAULT gen_random_uuid(),
                "schedule_id" uuid,
                "status" varchar(16) NOT NULL,
                "trigger" varchar(16) NOT NULL DEFAULT 'manual',
                "tables" jsonb NOT NULL DEFAULT '[]',
                "size_bytes" bigint,
                "file_name" varchar(256),
                "file_path" text,
                "error" text,
                "destinations_result" jsonb,
                "started_at" timestamptz NOT NULL,
                "finished_at" timestamptz,
                CONSTRAINT "PK_backup_runs" PRIMARY KEY ("id")
            );
        `);

        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_backup_runs_schedule_id" ON "backup_runs" ("schedule_id");`);
        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_backup_runs_started_at" ON "backup_runs" ("started_at");`);
    }

    public async down(q: QueryRunner): Promise<void> {
        await q.query(`DROP TABLE IF EXISTS "backup_runs";`);
        await q.query(`DROP TABLE IF EXISTS "backup_schedules";`);
    }
}
