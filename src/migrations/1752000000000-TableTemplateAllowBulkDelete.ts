import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Per-template gate for the mass delete-by-filter operation (`table.bulkDelete`).
 * Off by default — an operator must explicitly opt a table in before the mass
 * delete button/endpoint becomes usable for it.
 */
export class TableTemplateAllowBulkDelete1752000000000 implements MigrationInterface {
    name = 'TableTemplateAllowBulkDelete1752000000000';

    public async up(q: QueryRunner): Promise<void> {
        await q.query(`ALTER TABLE "table_templates" ADD COLUMN IF NOT EXISTS "allow_bulk_delete" boolean NOT NULL DEFAULT false;`);
    }

    public async down(q: QueryRunner): Promise<void> {
        await q.query(`ALTER TABLE "table_templates" DROP COLUMN IF EXISTS "allow_bulk_delete";`);
    }
}
