import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Handles renaming the response column from aeat_response to sii_response
 * for environments where the old migration already ran.
 * Safe no-op if the column is already named correctly or doesn't exist yet.
 */
export class RenameSiiResponseColumn1751000014000 implements MigrationInterface {
  name = 'RenameSiiResponseColumn1751000014000';

  public async up(q: QueryRunner): Promise<void> {
    // Use PostgreSQL's anonymous block to silently ignore if column doesn't exist or already has correct name.
    await q.query(`
      DO $$
      BEGIN
        ALTER TABLE "table_rows" RENAME COLUMN "aeat_response" TO "sii_response";
      EXCEPTION WHEN others THEN
        NULL;
      END $$;
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      DO $$
      BEGIN
        ALTER TABLE "table_rows" RENAME COLUMN "sii_response" TO "aeat_response";
      EXCEPTION WHEN others THEN
        NULL;
      END $$;
    `);
  }
}
