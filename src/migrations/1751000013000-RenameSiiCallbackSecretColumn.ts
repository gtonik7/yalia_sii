import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Handles renaming the callback secret column from aeat_callback_secret_encrypted
 * to sii_callback_secret_encrypted for environments where the old migration already ran.
 * Safe no-op if the column is already named correctly or doesn't exist yet.
 */
export class RenameSiiCallbackSecretColumn1751000013000 implements MigrationInterface {
  name = 'RenameSiiCallbackSecretColumn1751000013000';

  public async up(q: QueryRunner): Promise<void> {
    // Use PostgreSQL's anonymous block to silently ignore if column doesn't exist or already has correct name.
    await q.query(`
      DO $$
      BEGIN
        ALTER TABLE "source_connections" RENAME COLUMN "aeat_callback_secret_encrypted" TO "sii_callback_secret_encrypted";
      EXCEPTION WHEN others THEN
        NULL;
      END $$;
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      DO $$
      BEGIN
        ALTER TABLE "source_connections" RENAME COLUMN "sii_callback_secret_encrypted" TO "aeat_callback_secret_encrypted";
      EXCEPTION WHEN others THEN
        NULL;
      END $$;
    `);
  }
}
