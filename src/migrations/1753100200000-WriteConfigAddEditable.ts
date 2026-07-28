import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Adds `write.editable` — decoupled from whether the table has connections
 * configured: a table can send its ingested rows out (connections/scheduled/
 * batch) while keeping the explorer's row detail read-only. Every template
 * that already had `write` configured is backfilled to `editable: true`,
 * preserving today's behavior (write presence used to imply fully editable).
 */
export class WriteConfigAddEditable1753100200000 implements MigrationInterface {
  name = 'WriteConfigAddEditable1753100200000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      UPDATE table_templates
      SET write = jsonb_set(write, '{editable}', 'true'::jsonb)
      WHERE write IS NOT NULL AND NOT (write ? 'editable');
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      UPDATE table_templates
      SET write = write - 'editable'
      WHERE write IS NOT NULL AND write ? 'editable';
    `);
  }
}
