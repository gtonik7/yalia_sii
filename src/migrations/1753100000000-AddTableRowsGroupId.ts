import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Adds `table_rows.group_id`: the grouping key materialized whenever a row is
 * (re)queued (see `TableRowsService.markQueued` / `buildGroupIdSql`) — an md5 of
 * `connection_id` + the values of the template's `write.batch.groupBy` columns.
 * Rows of the same group share it, so forcing the send of one row can pull in
 * its siblings and ship the group complete.
 *
 * The backfill re-derives `group_id` for every existing row of every grouping
 * template with the SAME formula the service uses at write time, so already
 * loaded data groups immediately (no re-ingest needed). Templates without a
 * `write.batch.groupBy` are left untouched (group_id stays NULL).
 */
export class AddTableRowsGroupId1753100000000 implements MigrationInterface {
  name = 'AddTableRowsGroupId1753100000000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_rows" ADD COLUMN IF NOT EXISTS "group_id" varchar(64);`);
    await q.query(
      `CREATE INDEX IF NOT EXISTS "idx_table_rows_group"
         ON "table_rows" (table_key, connection_id, group_id) WHERE group_id IS NOT NULL;`,
    );
    // Backfill: build, per grouping template, the exact md5 expression used by
    // `buildGroupIdSql` and stamp every one of its rows. `%L` yields a safe
    // string literal for the column name and the empty-string coalesce default.
    await q.query(`
      DO $$
      DECLARE
        t RECORD;
        col text;
        expr text;
      BEGIN
        FOR t IN
          SELECT key, write->'batch'->'groupBy' AS gb
          FROM table_templates
          WHERE jsonb_typeof(write->'batch'->'groupBy') = 'array'
            AND jsonb_array_length(write->'batch'->'groupBy') > 0
        LOOP
          expr := format('coalesce(connection_id, %L)', '');
          FOR col IN SELECT jsonb_array_elements_text(t.gb) LOOP
            expr := expr || ' || chr(31) || ' || format('coalesce(data ->> %L, %L)', col, '');
          END LOOP;
          EXECUTE format('UPDATE table_rows SET group_id = md5(%s) WHERE table_key = %L', expr, t.key);
        END LOOP;
      END $$;
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP INDEX IF EXISTS "idx_table_rows_group";`);
    await q.query(`ALTER TABLE "table_rows" DROP COLUMN IF EXISTS "group_id";`);
  }
}
