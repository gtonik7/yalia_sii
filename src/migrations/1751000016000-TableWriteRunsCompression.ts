import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Convierte `table_write_runs` en hypertable de TimescaleDB con compresión
 * columnar + retención. Es el follow-up que anticipaba TableWriteRuns
 * ("si hiciera falta retention, es una migración posterior").
 *
 * Motivo (compresión): `payload_preview` guarda el payload saliente COMPLETO por
 * lote (decisión de auditoría deliberada — ver memoria yalia_sii_full_payload_audit),
 * lo que hace crecer la tabla. En vez de recortar el payload (perdería fidelidad
 * de auditoría), se comprime columnarmente (10-20x sobre jsonb) y se pone un tope
 * de retención. Mismo patrón que `audit_events` y `source_poll_runs`.
 *
 * PREREQUISITO: `table_write_runs` ya existe (1751000004000) con sus columnas
 * jsonb (`payload_preview`/`response_body`) añadidas por migraciones posteriores.
 */
export class TableWriteRunsCompression1751000016000 implements MigrationInterface {
  name = 'TableWriteRunsCompression1751000016000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`CREATE EXTENSION IF NOT EXISTS timescaledb;`);

    // create_hypertable exige que toda PK incluya la columna de partición.
    // Ampliamos la PK a (id, created_at); el nombre del constraint se descubre
    // dinámicamente por robustez.
    await q.query(`
      DO $$
      DECLARE pk_name text;
      BEGIN
        SELECT conname INTO pk_name FROM pg_constraint
          WHERE conrelid = 'table_write_runs'::regclass AND contype = 'p';
        IF pk_name IS NOT NULL THEN
          EXECUTE format('ALTER TABLE table_write_runs DROP CONSTRAINT %I', pk_name);
        END IF;
      END $$;
    `);
    await q.query(`ALTER TABLE "table_write_runs" ADD PRIMARY KEY ("id", "created_at");`);

    // Volumen: un run por lote saliente; chunks semanales.
    await q.query(`
      SELECT create_hypertable(
        'table_write_runs', 'created_at',
        chunk_time_interval => INTERVAL '7 days',
        migrate_data => true,
        if_not_exists => true
      );
    `);

    await q.query(`
      ALTER TABLE "table_write_runs" SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = '"table_key", "status"',
        timescaledb.compress_orderby = '"created_at" DESC'
      );
    `);
    await q.query(`SELECT add_compression_policy('table_write_runs', INTERVAL '7 days');`);

    // Histórico operativo ("trace, not truth"): 365 días.
    await q.query(`SELECT add_retention_policy('table_write_runs', INTERVAL '365 days');`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      DO $$ BEGIN
        PERFORM remove_retention_policy('table_write_runs', if_exists => true);
      EXCEPTION WHEN undefined_function THEN NULL;
      END $$;
    `);
    await q.query(`
      DO $$ BEGIN
        PERFORM remove_compression_policy('table_write_runs', if_exists => true);
      EXCEPTION WHEN undefined_function THEN NULL;
      END $$;
    `);
    await q.query(`ALTER TABLE "table_write_runs" SET (timescaledb.compress = false);`);
    // Revertir hypertable → tabla normal no es trivial en Timescale; recrear
    // desde cero si se necesita. La PK compuesta se conserva.
  }
}
