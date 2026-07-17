import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { ConfigService } from '@nestjs/config';
import { Repository } from 'typeorm';
import { randomBytes } from 'node:crypto';
import {
  HandshakeConfig,
  SourceAuthType,
  SourceConnection,
  SourceCredentials,
} from './entities/source-connection.entity';
import { UpsertSourceConnectionDto } from './dto/upsert-source-connection.dto';
import { decryptJson, encryptJson } from '../core/crypto/crypto.util';
import { isUuid } from '../core/sql/sql-params.util';
import type { Env } from '../config/env';

/** The connection's SII-callback URL plus its one-time-visible plaintext secret. */
export interface SiiCallbackInfo {
  url: string;
  secret: string;
}

/** A connection with credentials decrypted and ready for the HTTP client. */
export interface ResolvedSourceConnection {
  id: string;
  name: string;
  clave: string | null;
  baseUrl: string;
  authType: SourceAuthType;
  credentials: SourceCredentials;
  defaultHeaders: Record<string, string>;
  handshake?: HandshakeConfig;
  active: boolean;
}

@Injectable()
export class SourceConnectionsService {
  constructor(
    @InjectRepository(SourceConnection)
    private readonly repo: Repository<SourceConnection>,
    private readonly config: ConfigService<Env, true>,
  ) {}

  /** List connections without leaking the encrypted credentials blob. */
  async list(): Promise<Record<string, unknown>[]> {
    const items = await this.repo.find({ order: { name: 'ASC' } });
    return items.map((item) => this.serialize(item));
  }

  /**
   * Active connections that have an internal write cron configured
   * (`writeCronIntervalSec > 0`). Lean shape for WriteCronService — no
   * credentials, no serialization. Re-read every supervisor tick so config
   * changes (add/edit/disable) are picked up without cross-module wiring.
   */
  async listWriteCronConnections(): Promise<{ id: string; intervalSec: number }[]> {
    const items = await this.repo.find({
      where: { active: true },
      select: { id: true, writeCronIntervalSec: true },
    });
    return items
      .filter((c) => (c.writeCronIntervalSec ?? 0) > 0)
      .map((c) => ({ id: c.id, intervalSec: c.writeCronIntervalSec as number }));
  }

  async findById(id: string): Promise<SourceConnection> {
    const conn = isUuid(id) ? await this.repo.findOne({ where: { id } }) : null;
    if (!conn) throw new NotFoundException(`Source connection ${id} not found`);
    return conn;
  }

  /**
   * Existence check for callers (e.g. push ingest) that only need a boolean and
   * must not throw on a malformed id — arbitrary caller-supplied connectionIds
   * routinely aren't syntactically valid uuids.
   */
  async exists(id: string): Promise<boolean> {
    if (!isUuid(id)) return false;
    return this.repo.exists({ where: { id } });
  }

  async create(dto: UpsertSourceConnectionDto): Promise<Record<string, unknown>> {
    this.validateCredentials(dto.authType, dto.credentials);
    const saved = await this.repo.save(
      this.repo.create({
        name: dto.name,
        clave: dto.clave?.trim() || null,
        writeCronIntervalSec: dto.writeCronIntervalSec ?? null,
        concurrency: dto.concurrency ?? null,
        baseUrl: dto.baseUrl.trim().replace(/\/+$/, ''),
        authType: dto.authType,
        credentialsEncrypted: dto.credentials ? encryptJson(dto.credentials) : '',
        defaultHeaders: dto.defaultHeaders ?? {},
        handshake: dto.handshake ?? null,
        active: dto.active ?? true,
        siiCallbackSecretEncrypted: encryptJson(this.generateCallbackSecret()),
      }),
    );
    return this.serialize(saved);
  }

  async update(id: string, dto: UpsertSourceConnectionDto): Promise<Record<string, unknown>> {
    const existing = await this.findById(id);
    const patch: Partial<SourceConnection> = {
      name: dto.name,
      clave: dto.clave?.trim() || null,
      writeCronIntervalSec: dto.writeCronIntervalSec ?? null,
      concurrency: dto.concurrency ?? null,
      baseUrl: dto.baseUrl.trim().replace(/\/+$/, ''),
      authType: dto.authType,
      defaultHeaders: dto.defaultHeaders ?? {},
      active: dto.active ?? true,
      // Absent handshake on update means "remove it" (the form can clear it).
      handshake: dto.handshake ?? null,
    };
    if (dto.credentials && Object.keys(dto.credentials).length > 0) {
      this.validateCredentials(dto.authType, dto.credentials);
      patch.credentialsEncrypted = encryptJson(dto.credentials);
    } else if (!existing.credentialsEncrypted) {
      throw new BadRequestException(`authType "${dto.authType}" requires credentials`);
    }
    // `handshake.body` is deliberately `unknown` (opaque passthrough JSON),
    // which TypeORM's QueryDeepPartialEntity mapped type can't recurse into —
    // cast at this single, tightly-scoped call site.
    await this.repo.update({ id }, patch as Parameters<typeof this.repo.update>[1]);
    const updated = await this.findById(id);
    return this.serialize(updated);
  }

  async remove(id: string): Promise<{ ok: true }> {
    const result = await this.repo.delete({ id });
    if (result.affected === 0) throw new NotFoundException(`Source connection ${id} not found`);
    return { ok: true };
  }

  /** Decrypt credentials and return the connection ready to use in the client. */
  async resolveById(id: string): Promise<ResolvedSourceConnection> {
    const doc = await this.findById(id);
    const credentials = doc.credentialsEncrypted ? decryptJson<SourceCredentials>(doc.credentialsEncrypted) : {};
    return {
      id: doc.id,
      name: doc.name,
      clave: doc.clave,
      baseUrl: doc.baseUrl,
      authType: doc.authType,
      credentials,
      defaultHeaders: doc.defaultHeaders ?? {},
      handshake: doc.handshake ?? undefined,
      active: doc.active,
    };
  }

  /**
   * The connection's SII-result callback URL and its plaintext secret — shown
   * in the UI so the user can hand both to the external system. Every response
   * decrypts the secret fresh; there's no separate "reveal" step.
   */
  async getCallbackInfo(id: string): Promise<SiiCallbackInfo> {
    const conn = await this.findById(id);
    if (!conn.siiCallbackSecretEncrypted) {
      // Connections created before this feature shipped have none yet — backfill lazily.
      return this.rotateCallbackSecret(id);
    }
    return {
      url: this.buildCallbackUrl(conn.id),
      secret: decryptJson<string>(conn.siiCallbackSecretEncrypted),
    };
  }

  /** Generate a brand-new callback secret, replacing the old one, and return it in plaintext. */
  async rotateCallbackSecret(id: string): Promise<SiiCallbackInfo> {
    const conn = await this.findById(id);
    const secret = this.generateCallbackSecret();
    await this.repo.update({ id }, { siiCallbackSecretEncrypted: encryptJson(secret) });
    return { url: this.buildCallbackUrl(conn.id), secret };
  }

  /** Lean lookup for the callback controller — decrypts only the secret it needs to verify a signature. */
  async findCallbackSecret(id: string): Promise<string | null> {
    if (!isUuid(id)) return null;
    const conn = await this.repo.findOne({ where: { id }, select: { id: true, siiCallbackSecretEncrypted: true } });
    if (!conn?.siiCallbackSecretEncrypted) return null;
    return decryptJson<string>(conn.siiCallbackSecretEncrypted);
  }

  private generateCallbackSecret(): string {
    return randomBytes(32).toString('hex');
  }

  private buildCallbackUrl(connectionId: string): string {
    const explicitUrl = this.config.get('SATELLITE_MGMT_URL', { infer: true });
    const host = this.config.get('SATELLITE_HOST', { infer: true }) ?? 'localhost';
    const port = this.config.get('PORT', { infer: true });
    const base = explicitUrl ?? `http://${host}:${port}`;
    return `${base.replace(/\/+$/, '')}/v1/callbacks/sii/${connectionId}`;
  }

  /** Strip the encrypted credentials blob and alias `id` to `_id` (the hub_fe's `SourceConnection` contract predates this Postgres migration). */
  private serialize(conn: SourceConnection): Record<string, unknown> {
    const { credentialsEncrypted, siiCallbackSecretEncrypted, id, ...rest } = conn;
    return { _id: id, ...rest };
  }

  /** Fail fast when the credentials object misses what bearer auth needs. */
  private validateCredentials(authType: SourceAuthType, creds?: SourceCredentials): void {
    if (!creds) throw new BadRequestException(`authType "${authType}" requires credentials`);
    if (!creds.token) {
      throw new BadRequestException(`credentials missing for ${authType}: token`);
    }
  }
}

/** Usa la `clave` explícita de la conexión, o deriva un slug de `name` (minúsculas, espacios → guiones). */
export function resolveClave(conn: Pick<ResolvedSourceConnection, 'clave' | 'name'>): string {
  const explicit = conn.clave?.trim();
  if (explicit) return explicit;
  return conn.name.trim().toLowerCase().replace(/\s+/g, '-');
}
