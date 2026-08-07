import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Sellado de cargas para evitar envíos parciales de lotes.
 *
 * Cuando una unidad de ingesta se fragmenta en varios eventos independientes
 * (SFTP trocea los ficheros >= 1MB en lotes de 2000 filas, cada uno un evento
 * hub/saga separado), las filas de un mismo grupo (`write.batch.groupBy`) pueden
 * quedar repartidas entre eventos que se ingieren en transacciones distintas. El
 * write-cron interno podía barrer y enviar un grupo a medias antes de que llegaran
 * el resto de lotes.
 *
 * Solución: las filas de una carga fragmentada aterrizan en `submission_status =
 * 'staged'` (NO barrible; el cron y "forzar envío" solo miran 'queued'/'revisado')
 * estampadas con `load_id`. Cuando han llegado los N lotes de la carga (contados en
 * `table_load_batches`) y el evento de sello fija `expected_batches`, TODA la carga
 * pasa `staged → queued` de forma atómica. Un evento único (webhook / fichero < 1MB
 * / alta manual) no lleva `load_id` y sigue aterrizando `queued` directamente.
 */
export class TableLoadStaging1753300000000 implements MigrationInterface {
  name = 'TableLoadStaging1753300000000';

  public async up(q: QueryRunner): Promise<void> {
    // Carga a la que pertenece una fila mientras está 'staged' (NULL para filas
    // que no llegaron por una carga fragmentada). El índice parcial acelera el
    // flip `staged → queued` por load_id y el barrido del reaper.
    await q.query(`ALTER TABLE "table_rows" ADD COLUMN IF NOT EXISTS "load_id" varchar(255);`);
    await q.query(
      `CREATE INDEX IF NOT EXISTS "idx_table_rows_load"
         ON "table_rows" (table_key, load_id) WHERE load_id IS NOT NULL;`,
    );

    // Una fila por carga fragmentada. `expected_batches` lo fija el evento de
    // sello (NULL hasta entonces); `sealed_at` marca que la carga ya se completó
    // y sus filas pasaron a 'queued'. `stale_at` lo pone el reaper cuando una
    // carga lleva demasiado tiempo sin sellar (lote perdido) — nunca se auto-envía
    // parcial: el lote que falta se reintenta desde la DLQ y completa la carga.
    await q.query(`
      CREATE TABLE IF NOT EXISTS "table_loads" (
        "load_id" varchar(255) PRIMARY KEY,
        "table_key" varchar(128) NOT NULL,
        "connection_id" varchar(64),
        "expected_batches" int,
        "created_at" timestamptz NOT NULL DEFAULT now(),
        "sealed_at" timestamptz,
        "stale_at" timestamptz
      );
    `);
    // Barrido del reaper: cargas sin sellar por antigüedad.
    await q.query(
      `CREATE INDEX IF NOT EXISTS "idx_table_loads_unsealed"
         ON "table_loads" (created_at) WHERE sealed_at IS NULL;`,
    );

    // Un lote recibido de una carga. La PK (load_id, batch_index) hace el conteo
    // idempotente frente a reintentos/re-entregas del mismo lote (INSERT ON
    // CONFLICT DO NOTHING). `count(*)` por load_id = lotes recibidos.
    await q.query(`
      CREATE TABLE IF NOT EXISTS "table_load_batches" (
        "load_id" varchar(255) NOT NULL,
        "batch_index" int NOT NULL,
        "created_at" timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY ("load_id", "batch_index")
      );
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP TABLE IF EXISTS "table_load_batches";`);
    await q.query(`DROP INDEX IF EXISTS "idx_table_loads_unsealed";`);
    await q.query(`DROP TABLE IF EXISTS "table_loads";`);
    await q.query(`DROP INDEX IF EXISTS "idx_table_rows_load";`);
    await q.query(`ALTER TABLE "table_rows" DROP COLUMN IF EXISTS "load_id";`);
  }
}
