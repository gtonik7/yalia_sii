import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

export type SourceAuthType = 'bearer';

export type PaginationType = 'none' | 'page' | 'offset' | 'cursor' | 'link' | 'nextUrl';

export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'HEAD';

/**
 * Optional "handshake" request used by the connection test. Generic systems
 * (like the emitter's targets) often expose a cheap health/ping endpoint; this
 * lets the user point the test at it so "Probar conexión" exercises auth + a
 * real round-trip instead of blindly hitting the base URL. The data fetch for
 * each table is configured separately on the table's `audit` (method/path/query).
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

/**
 * Describes how the source system paginates so the poller can drive it without
 * any system-specific code. Paths are dot-paths into the JSON response envelope
 * (e.g. NetSuite saved-search restlet returns `{ data: { rows, is_last, ... } }`,
 * so recordsPath=`data.rows`, isLastPath=`data.is_last`).
 */
export interface PaginationConfig {
  type: PaginationType;
  /** Dot-path to the records array inside the response (e.g. `data.rows`). */
  recordsPath: string;

  // --- page / offset request params ---
  /** Query param carrying the page number (type=page). */
  pageParam?: string;
  /** Query param carrying the page size. */
  pageSizeParam?: string;
  /** Records per page asked from the source. */
  pageSize?: number;
  /** First page number the source expects (0 or 1). Default 1. */
  startPage?: number;
  /** Query param carrying the offset (type=offset). */
  offsetParam?: string;
  /** Query param carrying the limit (type=offset). */
  limitParam?: string;

  // --- stop conditions (any present is honored) ---
  /** Dot-path to a boolean "this is the last page" flag (e.g. `data.is_last`). */
  isLastPath?: string;
  /** Dot-path to the total page count (e.g. `data.pages`). */
  totalPagesPath?: string;
  /** Dot-path to the total record count (e.g. `data.total_results`). */
  totalResultsPath?: string;

  // --- cursor / next-url ---
  /** Query param carrying the cursor (type=cursor). */
  cursorParam?: string;
  /** Dot-path to the next cursor in the response (type=cursor). */
  nextCursorPath?: string;
  /** Dot-path to an absolute "next page" URL in the response (type=nextUrl). */
  nextUrlPath?: string;
}

@Entity('source_connections')
export class SourceConnection {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Index({ unique: true })
  @Column({ type: 'varchar', length: 256 })
  name!: string;

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

  @Column({ type: 'jsonb' })
  pagination!: PaginationConfig;

  /** Optional request used by the connection "test" endpoint. */
  @Column({ type: 'jsonb', nullable: true })
  handshake!: HandshakeConfig | null;

  @Column({ type: 'boolean', default: true })
  active!: boolean;

  @CreateDateColumn({ type: 'timestamptz', name: 'created_at' })
  createdAt!: Date;

  @UpdateDateColumn({ type: 'timestamptz', name: 'updated_at' })
  updatedAt!: Date;
}
