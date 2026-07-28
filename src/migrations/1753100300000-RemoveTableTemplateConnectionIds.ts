import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Drops `table_templates.connection_ids` ("Conexiones donde se expone"): it
 * only ever narrowed the connection picker in Explorer/Registros/Informe —
 * nothing server-side enforced it. That UI slot is replaced by the
 * write-connections list, which now supports a generic/fallback rule
 * (`WriteConnectionRule.connectionId` optional) alongside per-connection
 * overrides — see `WriteConfigAddEditable` and `resolveWriteRule`.
 *
 * No backfill on down(): the column's data isn't recoverable, same as
 * `RemoveAuditAndPerConnection`'s treatment of `audit`/`per_connection`.
 */
export class RemoveTableTemplateConnectionIds1753100300000 implements MigrationInterface {
  name = 'RemoveTableTemplateConnectionIds1753100300000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_templates" DROP COLUMN "connection_ids"`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "table_templates" ADD COLUMN "connection_ids" jsonb`);
  }
}
