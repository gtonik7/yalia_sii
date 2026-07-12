import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Elimina la columna generada STORED `search_vector` (+ su índice GIN) de
 * `table_rows`. Revierte 1751000001000-TableRowsSearchIndex.
 *
 * Motivo (compresión): el `to_tsvector('simple', data::text)` STORED
 * materializaba de nuevo el JSON completo de cada fila y el índice GIN añadía
 * otra copia — juntos ~duplicaban el tamaño de la tabla. Para datos fiscales
 * estructurados de SII la búsqueda útil es por columna (ya soportada por los
 * filtros de `TableRowsService.query`), no de texto libre sobre todo el JSON,
 * así que se retira por completo. `TableRowsService.query` deja de referenciar
 * `search_vector` (la rama `params.search` se ignora).
 */
export class DropTableRowsSearchVector1751000015000 implements MigrationInterface {
  name = 'DropTableRowsSearchVector1751000015000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`DROP INDEX IF EXISTS "ix_table_rows_search_vector";`);
    await q.query(`ALTER TABLE "table_rows" DROP COLUMN IF EXISTS "search_vector";`);
  }

  public async down(q: QueryRunner): Promise<void> {
    // Recrea la columna generada + índice GIN idénticos a TableRowsSearchIndex.
    await q.query(`
      ALTER TABLE "table_rows"
        ADD COLUMN "search_vector" tsvector
        GENERATED ALWAYS AS (to_tsvector('simple', "data"::text)) STORED;
    `);
    await q.query(`
      CREATE INDEX IF NOT EXISTS "ix_table_rows_search_vector"
        ON "table_rows" USING gin ("search_vector");
    `);
  }
}
