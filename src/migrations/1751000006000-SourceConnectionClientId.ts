import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Añade `client_id` a `source_connections`: identificador de cliente enviado en el
 * body de las peticiones de escritura salientes (`{clientId, payload}`). Opcional —
 * cuando está vacío, `resolveClientId()` deriva un slug de `name`.
 */
export class SourceConnectionClientId1751000006000 implements MigrationInterface {
  name = 'SourceConnectionClientId1751000006000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      ALTER TABLE "source_connections"
        ADD COLUMN "client_id" varchar(256) NULL;
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "source_connections" DROP COLUMN IF EXISTS "client_id";`);
  }
}
