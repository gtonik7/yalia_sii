import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Quita `pagination` de `source_connections`: era exclusivamente consumida por
 * el poller de auditoría (`SourceHttpClient.initialState`/`fetchPage`/`probe`,
 * eliminados en 1751000008000-RemoveAuditAndPerConnection.ts junto con el
 * módulo `audit/`). Sin ese consumidor, el campo queda huérfano — las
 * conexiones ahora solo se usan para `write` (envío saliente) y el handshake
 * de "Probar conexión", ninguno de los cuales pagina.
 */
export class RemoveSourceConnectionPagination1751000009000 implements MigrationInterface {
  name = 'RemoveSourceConnectionPagination1751000009000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "source_connections" DROP COLUMN "pagination"`);
  }

  public async down(q: QueryRunner): Promise<void> {
    // No se puede restaurar NOT NULL sin backfill real por conexión — se
    // recrea nullable como limitación aceptada del rollback.
    await q.query(`ALTER TABLE "source_connections" ADD COLUMN "pagination" jsonb`);
  }
}
