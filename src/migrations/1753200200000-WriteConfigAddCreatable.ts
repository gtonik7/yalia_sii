import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Adds `write.creatable` — decoupled from `editable`: a table can allow manual
 * row creation ("Nuevo registro") without allowing edits to existing rows, or
 * vice versa. Before this migration, creation rode the `editable` gate, so
 * every writable template is backfilled to `creatable = editable`, preserving
 * today's behavior.
 */
export class WriteConfigAddCreatable1753200200000 implements MigrationInterface {
  name = 'WriteConfigAddCreatable1753200200000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      UPDATE table_templates
      SET write = jsonb_set(write, '{creatable}', COALESCE(write->'editable', 'false'::jsonb))
      WHERE write IS NOT NULL AND NOT (write ? 'creatable');
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      UPDATE table_templates
      SET write = write - 'creatable'
      WHERE write IS NOT NULL AND write ? 'creatable';
    `);
  }
}
