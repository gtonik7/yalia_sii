import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Repara filas huérfanas: `table_rows.submission_status IS NULL` en templates
 * que sí tienen `write` configurado. Ocurre cuando la ingesta corrió antes de
 * que la template tuviera `write` (ver `TableRowsService.markQueued`, que hace
 * no-op sin `template.write`) o en filas previas a la migración
 * `TableRowsSubmissionColumns` (nunca tuvo backfill). Esas filas quedaban
 * invisibles tanto para el cron (`submission_status = 'queued'`) como para el
 * force-submit manual (`submission_status IN ('queued','error')`), mostrando
 * guión en "Estado envío"/"Estado SII" sin forma de reenviarlas.
 *
 * Mismas columnas que toca `markQueued()` (submission_status/batch_id/sii_response)
 * para que la próxima pasada del cron las recoja como cualquier fila recién puesta en cola.
 */
export class BackfillMissingSubmissionStatus1751000021000 implements MigrationInterface {
  name = 'BackfillMissingSubmissionStatus1751000021000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      UPDATE "table_rows" tr
        SET "submission_status" = 'queued', "batch_id" = NULL, "sii_response" = NULL
        FROM "table_templates" tt
        WHERE tr."table_key" = tt."key"
          AND tt."write" IS NOT NULL
          AND tr."submission_status" IS NULL;
    `);
  }

  public async down(): Promise<void> {
    // No revertimos: no hay forma de distinguir las filas que este backfill
    // puso en 'queued' de filas que ya estaban legítimamente en 'queued'.
  }
}
