import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Renombra `source_connections.client_id` a `clave`: el mismo identificador de
 * cliente (explícito, o slug derivado de `name`), ahora enviado como
 * `customerId` en el body saliente (`{customerId, payload}`) en vez de
 * `clientId`.
 */
export class RenameClientIdToClave1751000011000 implements MigrationInterface {
  name = 'RenameClientIdToClave1751000011000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "source_connections" RENAME COLUMN "client_id" TO "clave";`);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "source_connections" RENAME COLUMN "clave" TO "client_id";`);
  }
}
