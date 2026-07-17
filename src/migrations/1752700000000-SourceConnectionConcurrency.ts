import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Añade `concurrency` a `source_connections`: nº de lotes que "Forzar envío" y
 * el barrido de escritura de esta conexión envían en paralelo al sistema
 * externo. Opcional — null/1 = envío secuencial (comportamiento previo).
 */
export class SourceConnectionConcurrency1752700000000 implements MigrationInterface {
  name = 'SourceConnectionConcurrency1752700000000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      ALTER TABLE "source_connections"
        ADD COLUMN "concurrency" integer NULL;
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "source_connections" DROP COLUMN IF EXISTS "concurrency";`);
  }
}
