import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Recrea el índice sobre `external_ref` para incluir `connection_id`.
 * El anterior (1751000007000) solo indexaba `(table_key, external_ref)`,
 * pero los callbacks siempre buscan por los tres: `table_key`, `connection_id`,
 * `external_ref`.
 */
export class FixTableRowsExternalRefIndex1751000010000 implements MigrationInterface {
  name = 'FixTableRowsExternalRefIndex1751000010000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`DROP INDEX IF EXISTS "idx_table_rows_external_ref"`);
    await q.query(
      `CREATE INDEX "idx_table_rows_external_ref" ON "table_rows" ("table_key", "connection_id", "external_ref") WHERE "external_ref" IS NOT NULL;`,
    );
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP INDEX IF EXISTS "idx_table_rows_external_ref"`);
    await q.query(
      `CREATE INDEX "idx_table_rows_external_ref" ON "table_rows" ("table_key", "external_ref") WHERE "external_ref" IS NOT NULL;`,
    );
  }
}
