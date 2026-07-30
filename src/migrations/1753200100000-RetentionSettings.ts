import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * `retention_settings`: config de retención (hot-editable) por target físico del
 * satélite — table_delete_events, domain_event_outbox, table_write_runs. `table_rows`
 * mantiene su retención por plantilla (TableTemplate.retentionDays) y no entra aquí.
 * Sin fila = defaults del catálogo.
 */
export class RetentionSettings1753200100000 implements MigrationInterface {
    name = 'RetentionSettings1753200100000';

    public async up(q: QueryRunner): Promise<void> {
        await q.query(`
            CREATE TABLE IF NOT EXISTS "retention_settings" (
                "target_key" varchar(64) NOT NULL,
                "retention_days" int NOT NULL,
                "interval_days" int NOT NULL DEFAULT 1,
                "enabled" boolean NOT NULL DEFAULT true,
                "last_run_at" timestamptz,
                "updated_at" timestamptz NOT NULL DEFAULT now(),
                CONSTRAINT "PK_retention_settings" PRIMARY KEY ("target_key")
            );
        `);
    }

    public async down(q: QueryRunner): Promise<void> {
        await q.query(`DROP TABLE IF EXISTS "retention_settings";`);
    }
}
