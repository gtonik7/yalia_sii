import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Adds `table_templates.id_fields` (jsonb, nullable) — a composite upsert key
 * (2+ column keys) used instead of `id_field` when set. Purely additive: no
 * backfill, existing templates keep using their single `id_field` untouched.
 */
export class TableTemplateAddIdFields1753200300000 implements MigrationInterface {
  name = 'TableTemplateAddIdFields1753200300000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE table_templates ADD COLUMN IF NOT EXISTS id_fields jsonb`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE table_templates DROP COLUMN IF EXISTS id_fields`);
  }
}
