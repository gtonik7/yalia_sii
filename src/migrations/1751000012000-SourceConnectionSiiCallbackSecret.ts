import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Per-connection callback secret: each source connection gets its own AES-256-GCM
 * encrypted HMAC secret for the SII-result callback, replacing the single global
 * SII_CALLBACK_HMAC_SECRET env var. Lets the callback URL become per-connection
 * (`/v1/callbacks/sii/:connectionId`) and the secret be auto-generated/rotated
 * from the UI, mirroring the hub's webhook secret pattern.
 */
export class SourceConnectionSiiCallbackSecret1751000012000 implements MigrationInterface {
  name = 'SourceConnectionSiiCallbackSecret1751000012000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(
      `ALTER TABLE "source_connections" ADD COLUMN "sii_callback_secret_encrypted" text NULL;`,
    );
  }

  public async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE "source_connections" DROP COLUMN "sii_callback_secret_encrypted";`);
  }
}
