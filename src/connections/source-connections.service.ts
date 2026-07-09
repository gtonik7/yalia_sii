import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  HandshakeConfig,
  PaginationConfig,
  SourceAuthType,
  SourceConnection,
  SourceCredentials,
} from './entities/source-connection.entity';
import { UpsertSourceConnectionDto } from './dto/upsert-source-connection.dto';
import { decryptJson, encryptJson } from '../core/crypto/crypto.util';
import { isUuid } from '../core/sql/sql-params.util';

/** A connection with credentials decrypted and ready for the HTTP client. */
export interface ResolvedSourceConnection {
  id: string;
  name: string;
  baseUrl: string;
  authType: SourceAuthType;
  credentials: SourceCredentials;
  defaultHeaders: Record<string, string>;
  pagination: PaginationConfig;
  handshake?: HandshakeConfig;
  active: boolean;
}

@Injectable()
export class SourceConnectionsService {
  constructor(
    @InjectRepository(SourceConnection)
    private readonly repo: Repository<SourceConnection>,
  ) {}

  /** List connections without leaking the encrypted credentials blob. */
  async list(): Promise<Record<string, unknown>[]> {
    const items = await this.repo.find({ order: { name: 'ASC' } });
    return items.map((item) => this.serialize(item));
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
        baseUrl: dto.baseUrl.trim().replace(/\/+$/, ''),
        authType: dto.authType,
        credentialsEncrypted: dto.credentials ? encryptJson(dto.credentials) : '',
        defaultHeaders: dto.defaultHeaders ?? {},
        pagination: dto.pagination,
        handshake: dto.handshake ?? null,
        active: dto.active ?? true,
      }),
    );
    return this.serialize(saved);
  }

  async update(id: string, dto: UpsertSourceConnectionDto): Promise<Record<string, unknown>> {
    const existing = await this.findById(id);
    const patch: Partial<SourceConnection> = {
      name: dto.name,
      baseUrl: dto.baseUrl.trim().replace(/\/+$/, ''),
      authType: dto.authType,
      defaultHeaders: dto.defaultHeaders ?? {},
      pagination: dto.pagination,
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
      baseUrl: doc.baseUrl,
      authType: doc.authType,
      credentials,
      defaultHeaders: doc.defaultHeaders ?? {},
      pagination: doc.pagination,
      handshake: doc.handshake ?? undefined,
      active: doc.active,
    };
  }

  /** Strip the encrypted credentials blob and alias `id` to `_id` (the hub_fe's `SourceConnection` contract predates this Postgres migration). */
  private serialize(conn: SourceConnection): Record<string, unknown> {
    const { credentialsEncrypted, id, ...rest } = conn;
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
