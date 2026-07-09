import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Soporte del cron interno de envío por conexión y del enriquecimiento de las
 * ejecuciones de escritura:
 *  - `source_connections.write_cron_interval_sec`: cadencia (segundos) del cron
 *    interno de yalia_sii que barre las filas `en cola` de esa conexión.
 *  - índice sobre `table_rows (table_key, external_ref)`: correlación rápida del
 *    callback SII (externalid) que hasta ahora hacía un seq-scan.
 *  - `table_write_runs.payload_preview` / `response_body`: muestra reducida del
 *    payload enviado y cuerpo de la respuesta del sistema externo en errores.
 */
export class SiiInternalWriteCron1751000007000 implements MigrationInterface {
  name = 'SiiInternalWriteCron1751000007000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "source_connections" ADD COLUMN "write_cron_interval_sec" int NULL;`);
    await q.query(
      `CREATE INDEX IF NOT EXISTS "idx_table_rows_external_ref" ON "table_rows" ("table_key", "external_ref") WHERE "external_ref" IS NOT NULL;`,
    );
    await q.query(
      `ALTER TABLE "table_write_runs" ADD COLUMN "payload_preview" jsonb NULL, ADD COLUMN "response_body" jsonb NULL;`,
    );
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(
      `ALTER TABLE "table_write_runs" DROP COLUMN IF EXISTS "response_body", DROP COLUMN IF EXISTS "payload_preview";`,
    );
    await q.query(`DROP INDEX IF EXISTS "idx_table_rows_external_ref";`);
    await q.query(`ALTER TABLE "source_connections" DROP COLUMN IF EXISTS "write_cron_interval_sec";`);
  }
}
