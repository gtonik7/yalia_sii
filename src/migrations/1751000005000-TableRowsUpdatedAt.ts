import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Añade `updated_at` a `table_rows`: marca de "última modificación" de la fila,
 * distinta de `created_at` (ingesta) y de `last_written_at` (último envío
 * externo). La expone el explorador/pestaña de registros como columna fija.
 *
 * Todas las escrituras del módulo de tablas son SQL crudo (ingest-upsert,
 * edición, cambios de estado de presentación), así que un `@UpdateDateColumn`
 * de TypeORM NO se dispararía. Un trigger `BEFORE UPDATE` es la fuente de
 * verdad y cubre cualquier UPDATE de forma uniforme.
 *
 * PREREQUISITO: table_rows debe existir ya (Baseline).
 */
export class TableRowsUpdatedAt1751000005000 implements MigrationInterface {
  name = 'TableRowsUpdatedAt1751000005000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      ALTER TABLE "table_rows"
        ADD COLUMN "updated_at" timestamptz NOT NULL DEFAULT now();
    `);

    await q.query(`
      CREATE OR REPLACE FUNCTION "table_rows_set_updated_at"()
      RETURNS trigger AS $$
      BEGIN
        NEW."updated_at" = now();
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await q.query(`
      CREATE TRIGGER "trg_table_rows_set_updated_at"
        BEFORE UPDATE ON "table_rows"
        FOR EACH ROW
        EXECUTE FUNCTION "table_rows_set_updated_at"();
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP TRIGGER IF EXISTS "trg_table_rows_set_updated_at" ON "table_rows";`);
    await q.query(`DROP FUNCTION IF EXISTS "table_rows_set_updated_at"();`);
    await q.query(`ALTER TABLE "table_rows" DROP COLUMN IF EXISTS "updated_at";`);
  }
}
