import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Añade compresión columnar a la hypertable `source_poll_runs`. Ya tenía
 * retención (1751000002000-SourcePollRunsTimescale) pero le faltaba la política
 * de compresión que `audit_events` sí tiene — de ahí este follow-up. Reduce
 * ~10-20x los chunks fríos (>7 días) sin afectar a los recientes.
 *
 * PREREQUISITO: `source_poll_runs` ya es hypertable (1751000002000).
 */
export class SourcePollRunsCompression1751000017000 implements MigrationInterface {
  name = 'SourcePollRunsCompression1751000017000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      ALTER TABLE "source_poll_runs" SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = '"table_key", "status"',
        timescaledb.compress_orderby = '"created_at" DESC'
      );
    `);
    await q.query(`SELECT add_compression_policy('source_poll_runs', INTERVAL '7 days');`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`
      DO $$ BEGIN
        PERFORM remove_compression_policy('source_poll_runs', if_exists => true);
      EXCEPTION WHEN undefined_function THEN NULL;
      END $$;
    `);
    await q.query(`ALTER TABLE "source_poll_runs" SET (timescaledb.compress = false);`);
  }
}
