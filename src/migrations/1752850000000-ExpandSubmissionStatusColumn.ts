import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Expands the `submission_status` column in `table_rows` from varchar(16) to
 * varchar(32) to accommodate vendor states like `PARCIALMENTE_CORRECTO` (23
 * chars). The column mixes internal pipeline states (queued, pending, revisado)
 * with vendor literals (CORRECTO, ERROR, INCORRECTO, PARCIALMENTE_CORRECTO),
 * so it must be large enough for the longest vendor state.
 */
export class ExpandSubmissionStatusColumn1752850000000 implements MigrationInterface {
  name = 'ExpandSubmissionStatusColumn1752850000000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      ALTER TABLE "table_rows"
      ALTER COLUMN "submission_status" TYPE varchar(32);
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      ALTER TABLE "table_rows"
      ALTER COLUMN "submission_status" TYPE varchar(16);
    `);
  }
}
