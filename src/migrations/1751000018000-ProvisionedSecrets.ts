import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Crea `provisioned_secrets`: el token de gestión que el hub auto-provisiona a
 * este satélite (entregado por la cola `sat-<key>-control`, persistido para
 * sobrevivir reinicios). Contrapartida Postgres de la colección Mongo homónima
 * en los demás satélites. Una única fila key='mgmt-token'.
 */
export class ProvisionedSecrets1751000018000 implements MigrationInterface {
  name = 'ProvisionedSecrets1751000018000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      CREATE TABLE IF NOT EXISTS "provisioned_secrets" (
        "key" varchar(64) NOT NULL,
        "token" text NOT NULL,
        "issued_at" bigint NOT NULL,
        "previous_token" text,
        "previous_until" bigint,
        CONSTRAINT "pk_provisioned_secrets" PRIMARY KEY ("key")
      );
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP TABLE IF EXISTS "provisioned_secrets";`);
  }
}
