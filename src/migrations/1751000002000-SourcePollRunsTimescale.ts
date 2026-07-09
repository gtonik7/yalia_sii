import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Convierte `source_poll_runs` en una hypertable de TimescaleDB con una
 * retention policy fija de 365 días — es el análogo directo del índice TTL
 * (`expireAfterSeconds`, gobernado por `POLL_RUN_RETENTION_SECONDS`) que tenía
 * la colección Mongo equivalente.
 *
 * Cambio de comportamiento consciente: el retention deja de ser configurable
 * por env var en runtime; cambiarlo en el futuro requiere una nueva migración
 * que llame `add_retention_policy` con otro intervalo.
 *
 * PREREQUISITO: `source_poll_runs` debe existir ya (1751000000000-Baseline.ts).
 */
export class SourcePollRunsTimescale1751000002000 implements MigrationInterface {
  name = 'SourcePollRunsTimescale1751000002000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`CREATE EXTENSION IF NOT EXISTS timescaledb;`);

    await q.query(`
      DO $$
      DECLARE pk_name text;
      BEGIN
        SELECT conname INTO pk_name FROM pg_constraint
          WHERE conrelid = 'source_poll_runs'::regclass AND contype = 'p';
        IF pk_name IS NOT NULL THEN
          EXECUTE format('ALTER TABLE source_poll_runs DROP CONSTRAINT %I', pk_name);
        END IF;
      END $$;
    `);
    await q.query(`ALTER TABLE "source_poll_runs" ADD PRIMARY KEY ("id", "created_at");`);

    // Volumen de escritura mucho menor que table_rows (un run por poll, no por
    // fila) — chunks semanales.
    await q.query(`
      SELECT create_hypertable(
        'source_poll_runs', 'created_at',
        chunk_time_interval => INTERVAL '7 days',
        migrate_data => true,
        if_not_exists => true
      );
    `);

    await q.query(`SELECT add_retention_policy('source_poll_runs', INTERVAL '365 days');`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      DO $$ BEGIN
        PERFORM remove_retention_policy('source_poll_runs', if_exists => true);
      EXCEPTION WHEN undefined_function THEN NULL;
      END $$;
    `);
  }
}
