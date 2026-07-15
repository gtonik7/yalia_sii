import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * "Newest wins" deduplication per template: `table_templates.recency_field`.
 * When set (alongside `id_field`), ingest keeps the row with the greatest
 * numeric value of this field for a given id — both within a single call and
 * across the ON CONFLICT upsert — instead of the last row processed. Empty by
 * default (last-write-wins, the historical behavior). For the SII `emitidas`
 * table this is `source_modify_at` (the SFTP file's modifyTime stamped by the
 * transform), so a reprocess of an older extract can never overwrite a newer
 * version of the same invoice. See `TableRowsService.ingest`.
 */
export class TableTemplateRecencyField1751000023000 implements MigrationInterface {
  name = 'TableTemplateRecencyField1751000023000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_templates" ADD COLUMN IF NOT EXISTS "recency_field" varchar(128) NOT NULL DEFAULT '';`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_templates" DROP COLUMN IF EXISTS "recency_field";`);
  }
}
