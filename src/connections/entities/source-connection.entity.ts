import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

export type SourceAuthType = 'bearer';

export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'HEAD';

/**
 * Optional "handshake" request used by the connection test. Generic systems
 * (like the emitter's targets) often expose a cheap health/ping endpoint; this
 * lets the user point the test at it so "Probar conexión" exercises auth + a
 * real round-trip instead of blindly hitting the base URL. The outbound write
 * request for each table is configured separately on the table's `write`
 * (method/path/query).
 */
export interface HandshakeConfig {
  /** HTTP method for the test request. */
  method: HttpMethod;
  /** Path appended to baseUrl (empty / `/` = hit baseUrl as-is). May be absolute. */
  path?: string;
  /** Static query params merged into the test request. */
  query?: Record<string, string>;
  /** Request body for non-GET handshakes. */
  body?: unknown;
}

/** Decrypted credentials for a source connection. yalia_sii is bearer-only. */
export interface SourceCredentials {
  token?: string;
}

@Entity('source_connections')
export class SourceConnection {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Index({ unique: true })
  @Column({ type: 'varchar', length: 256 })
  name!: string;

  /**
   * Identificador de cliente enviado como `clientId` en el body saliente
   * (`{clientId, payload}`). Alfanumérico en minúsculas + guiones. Si está
   * vacío, se deriva de `name` con ese mismo formato (ver `resolveClave()`).
   */
  @Column({ type: 'varchar', length: 256, name: 'clave', nullable: true })
  clave!: string | null;

  /**
   * Cadencia (segundos) del cron interno de yalia_sii que barre las filas `en cola`
   * de esta conexión y las envía. null/0 = sin cron interno (el envío queda a cargo
   * de la edición por evento o de un disparo manual). Ver WriteCronService.
   */
  @Column({ type: 'int', name: 'write_cron_interval_sec', nullable: true })
  writeCronIntervalSec!: number | null;

  /** Base URL of the external system, e.g. `https://acme.restlets.api.netsuite.com`. */
  @Column({ type: 'varchar', length: 512, name: 'base_url' })
  baseUrl!: string;

  @Column({ type: 'varchar', length: 16, name: 'auth_type', default: 'bearer' })
  authType!: SourceAuthType;

  /** AES-256-GCM of {@link SourceCredentials}. */
  @Column({ type: 'text', name: 'credentials_encrypted', default: '' })
  credentialsEncrypted!: string;

  /** Static headers sent on every request (e.g. `Accept: application/json`). */
  @Column({ type: 'jsonb', name: 'default_headers', default: {} })
  defaultHeaders!: Record<string, string>;

  /** Optional request used by the connection "test" endpoint. */
  @Column({ type: 'jsonb', nullable: true })
  handshake!: HandshakeConfig | null;

  @Column({ type: 'boolean', default: true })
  active!: boolean;

  /**
   * AES-256-GCM of the plaintext HMAC secret used to verify the
   * SII-result callback signature (`x-sii-signature`) for this connection.
   * Auto-generated on create; rotatable via `SourceConnectionsService.rotateCallbackSecret()`.
   */
  @Column({ type: 'text', name: 'sii_callback_secret_encrypted', nullable: true })
  siiCallbackSecretEncrypted!: string | null;

  @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
  createdAt!: Date;

  @UpdateDateColumn({ type: 'timestamptz', name: 'updated_at' })
  updatedAt!: Date;
}
