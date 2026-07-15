import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Ajusta las políticas de `table_write_runs` para acotar el espacio en disco:
 * la tabla guarda el payload saliente COMPLETO por lote (`payload_preview`,
 * jsonb ~70-100KB/fila — decisión de auditoría deliberada), así que crece rápido.
 *
 *  - Retención: 365 → 90 días. Es historial operativo de envíos ("trace, not
 *    truth"): los registros fiscales reales viven en `table_rows`, no aquí, así
 *    que 90 días bastan para investigar incidencias recientes.
 *  - Compresión: 7 → 2 días. Comprime columnarmente casi todo el histórico
 *    (gran ahorro sobre los jsonb) dejando solo un par de días "calientes" sin
 *    comprimir para lecturas/escrituras rápidas.
 *
 * Las políticas no se "actualizan" in situ: hay que quitar la anterior y volver
 * a añadirla con el nuevo intervalo. PREREQUISITO: TableWriteRunsCompression
 * (1751000016000) ya convirtió la tabla en hypertable con compresión activada.
 */
export class TableWriteRunsRetentionTune1751000022000 implements MigrationInterface {
  name = 'TableWriteRunsRetentionTune1751000022000';

  public async up(q: QueryRunner): Promise<void> {
    // Retención 365 → 90 días.
    await q.query(`SELECT remove_retention_policy('table_write_runs', if_exists => true);`);
    await q.query(`SELECT add_retention_policy('table_write_runs', INTERVAL '90 days');`);

    // Compresión 7 → 2 días.
    await q.query(`SELECT remove_compression_policy('table_write_runs', if_exists => true);`);
    await q.query(`SELECT add_compression_policy('table_write_runs', INTERVAL '2 days');`);
  }

  public async down(q: QueryRunner): Promise<void> {
    // Vuelve a los intervalos de TableWriteRunsCompression (7 días / 365 días).
    await q.query(`
      DO $$ BEGIN
        PERFORM remove_retention_policy('table_write_runs', if_exists => true);
        PERFORM add_retention_policy('table_write_runs', INTERVAL '365 days');
        PERFORM remove_compression_policy('table_write_runs', if_exists => true);
        PERFORM add_compression_policy('table_write_runs', INTERVAL '7 days');
      EXCEPTION WHEN undefined_function THEN NULL;
      END $$;
    `);
  }
}
