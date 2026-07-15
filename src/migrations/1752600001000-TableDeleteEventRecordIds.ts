import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * `table_delete_events` only ever stored aggregate counts (`affected`), so a
 * missing-records diff on the hub side could never say WHICH ids a voluntary
 * delete removed — only that some delete happened, at some point, for that
 * connection. `record_ids` captures the actual business-key ids returned by
 * `deleteRows`'s `RETURNING` clause (see TableRowsService), letting
 * `findMissingIds` answer "this id was deleted on `<date>`" with certainty
 * instead of a timing guess. Nullable + no backfill: events recorded before this
 * migration (including the `'baseline'` seed) simply have no id-level data —
 * same "no explanation on file" fallback as any other unattributed gap.
 */
export class TableDeleteEventRecordIds1752600001000 implements MigrationInterface {
    name = 'TableDeleteEventRecordIds1752600001000';

    public async up(q: QueryRunner): Promise<void> {
        await q.query(`ALTER TABLE "table_delete_events" ADD COLUMN IF NOT EXISTS "record_ids" text[];`);
        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_table_delete_events_record_ids" ON "table_delete_events" USING GIN ("record_ids");`);
    }

    public async down(q: QueryRunner): Promise<void> {
        await q.query(`DROP INDEX IF EXISTS "IDX_table_delete_events_record_ids";`);
        await q.query(`ALTER TABLE "table_delete_events" DROP COLUMN IF EXISTS "record_ids";`);
    }
}
