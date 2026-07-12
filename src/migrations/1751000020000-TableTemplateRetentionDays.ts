import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Opt-in automatic retention per template (Sprint 5.3): `table_templates.retention_days`.
 * Null by default — no template purges automatically unless explicitly configured.
 * Swept daily by `TableRetentionCron`, which reuses `TableRowsService.deleteRows`
 * (the same code path as the manual/on-demand purge already exposed via the
 * datasets API).
 */
export class TableTemplateRetentionDays1751000020000 implements MigrationInterface {
  name = 'TableTemplateRetentionDays1751000020000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_templates" ADD COLUMN IF NOT EXISTS "retention_days" int;`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_templates" DROP COLUMN IF EXISTS "retention_days";`);
  }
}
