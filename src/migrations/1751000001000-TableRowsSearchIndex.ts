import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Adds a generated `search_vector` column (GIN-indexed) to `table_rows` for
 * free-text search — replaces the wildcard text index (`$**: 'text'`) the
 * equivalent Mongo collection had.
 *
 * PREREQUISITO: `table_rows` debe existir ya (1751000000000-Baseline.ts).
 *
 * `table_rows` NO es una hypertable de TimescaleDB (decisión tomada tras
 * descubrir, durante la implementación, dos restricciones duras e
 * inseparables de Timescale):
 *   1. Toda hypertable con `compression_enabled = true` rechaza
 *      `CREATE UNIQUE INDEX` incondicionalmente (verificado en vivo, incluso
 *      con 0 chunks comprimidos).
 *   2. Toda unique index en una hypertable (comprimida o no) debe incluir la
 *      columna de partición — en este caso `created_at` — lo cual arruinaría
 *      por completo el propósito del índice único parcial por `idField`
 *      (`connection_id`, `data->>'idField'` `WHERE table_key=...`): dos
 *      ingestas de la misma fila lógica en momentos distintos dejarían de
 *      colisionar, y el `ON CONFLICT` de `ingest()` nunca dispararía —
 *      duplicando filas en vez de actualizarlas.
 * `TableIndexManagerService` (Parte 5 del plan) necesita ese índice único
 * dinámico por template para el upsert por `idField`, así que hacer de
 * `table_rows` una hypertable es incompatible con la capacidad central de
 * edición/ingest de este satélite. Se prioriza esa capacidad: `table_rows`
 * queda como tabla Postgres normal, con los mismos índices GIN/expresión que
 * ya tenía planeados (`IDX_table_rows_key_conn_created` en Baseline, más los
 * índices dinámicos de `TableIndexManagerService`) — el mismo patrón que usa
 * el resto de tablas no-`audit_events` en `yalia_hub`. `source_poll_runs`
 * (sin necesidad de índices únicos dinámicos) sí es una hypertable — ver
 * `SourcePollRunsTimescale`.
 */
export class TableRowsSearchIndex1751000001000 implements MigrationInterface {
  name = 'TableRowsSearchIndex1751000001000';

  public async up(q: QueryRunner): Promise<void> {
    // Generada y mantenida por Postgres en cada escritura — sin upkeep desde la
    // app. TableRow (entity) deliberadamente NO declara esta columna: solo se
    // toca desde SQL crudo en TableRowsService.query()'s free-text search.
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

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP INDEX IF EXISTS "ix_table_rows_search_vector";`);
    await q.query(`ALTER TABLE "table_rows" DROP COLUMN IF EXISTS "search_vector";`);
  }
}
