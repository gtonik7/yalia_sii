import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Append-only ledger of app-initiated deletions (see TableDeleteEvent entity),
 * so the reconciliation page can tell voluntary deletions (bulk delete / retention
 * / delete-by-ids) apart from uncontrolled loss instead of counting every physical
 * delete as a loss.
 *
 * The `'baseline'` seed row snapshots the current `n_tup_del` for `table_rows`,
 * folding all pre-feature deletions into "voluntary" so `uncontrolled =
 * max(0, n_tup_del - SUM(affected))` starts at 0 on deploy (e.g. the ~16.752 test
 * deletions no longer trip the warning). `gen_random_uuid()` is available (pg16),
 * same as the rest of the schema.
 */
export class TableDeleteEvents1752600000000 implements MigrationInterface {
    name = 'TableDeleteEvents1752600000000';

    public async up(q: QueryRunner): Promise<void> {
        await q.query(`
            CREATE TABLE IF NOT EXISTS "table_delete_events" (
                "id"            uuid          NOT NULL DEFAULT gen_random_uuid(),
                "table_key"     varchar(128)  NOT NULL,
                "connection_id" varchar(64),
                "affected"      integer       NOT NULL,
                "reason"        varchar(16)   NOT NULL,
                "created_at"    timestamptz   NOT NULL DEFAULT now(),
                CONSTRAINT "PK_table_delete_events" PRIMARY KEY ("id")
            );
        `);
        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_table_delete_events_table_key" ON "table_delete_events" ("table_key");`);
        await q.query(`
            INSERT INTO "table_delete_events" ("table_key", "connection_id", "affected", "reason")
            SELECT '*', NULL, COALESCE(n_tup_del, 0)::int, 'baseline'
            FROM pg_stat_user_tables WHERE relname = 'table_rows';
        `);
    }

    public async down(q: QueryRunner): Promise<void> {
        await q.query(`DROP TABLE IF EXISTS "table_delete_events";`);
    }
}
