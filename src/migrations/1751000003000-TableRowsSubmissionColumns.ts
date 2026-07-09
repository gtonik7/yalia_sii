import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Añade el estado de presentación SII a `table_rows` (unificado en la misma
 * fila que el resto del ciclo de vida de escritura — ver el plan del pipeline
 * SII: no existe una tabla "sii-azure" separada).
 *
 * `submission_status` es distinto de `write_status`: `write_status` es el ack
 * de transporte del envío saliente (llegó o no al sistema externo);
 * `submission_status` es el resultado real de SII, que llega después vía
 * callback y se correlaciona por `external_ref`. `batch_id` es solo para
 * trazabilidad/detección de lotes atascados, nunca para decidir el resultado
 * por fila. `sii_response` guarda el último payload crudo del callback,
 * sobreescrito entero en cada respuesta (no hace falta histórico).
 *
 * También rellena `write.trigger = 'event'` en las templates existentes que ya
 * tengan `write` configurado pero no `trigger` — el comportamiento más
 * parecido al actual (llamada inline en la misma petición de edición).
 *
 * PREREQUISITO: table_rows y table_templates deben existir ya (Baseline).
 */
export class TableRowsSubmissionColumns1751000003000 implements MigrationInterface {
  name = 'TableRowsSubmissionColumns1751000003000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      ALTER TABLE "table_rows"
        ADD COLUMN "submission_status" varchar(16),
        ADD COLUMN "batch_id" varchar(64),
        ADD COLUMN "sii_response" jsonb;
    `);

    // El callback correlaciona por external_ref; hoy no hay ningún índice sobre esa columna.
    await q.query(`
      CREATE INDEX IF NOT EXISTS "ix_table_rows_external_ref"
        ON "table_rows" ("external_ref")
        WHERE "external_ref" IS NOT NULL;
    `);

    // Sostiene el barrido completo en modo schedule (WHERE table_key=$1 AND submission_status='queued').
    await q.query(`
      CREATE INDEX IF NOT EXISTS "ix_table_rows_key_submission_status"
        ON "table_rows" ("table_key", "submission_status");
    `);

    await q.query(`
      UPDATE "table_templates"
        SET "write" = jsonb_set("write", '{trigger}', '"event"')
        WHERE "write" IS NOT NULL AND NOT ("write" ? 'trigger');
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP INDEX IF EXISTS "ix_table_rows_key_submission_status";`);
    await q.query(`DROP INDEX IF EXISTS "ix_table_rows_external_ref";`);
    await q.query(`
      ALTER TABLE "table_rows"
        DROP COLUMN IF EXISTS "sii_response",
        DROP COLUMN IF EXISTS "batch_id",
        DROP COLUMN IF EXISTS "submission_status";
    `);
    // No revertimos el backfill de write.trigger: quitarlo dejaría templates
    // existentes con `write` pero sin `trigger`, un estado que la nueva
    // validación/entidad ya no espera encontrar.
  }
}
