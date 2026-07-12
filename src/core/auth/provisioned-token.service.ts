import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { timingSafeEqual } from 'crypto';
import { ProvisionedSecret } from './provisioned-secret.entity';

/** How long the previous token stays valid after a rotation (covers in-flight hub calls). */
const GRACE_MS = 10 * 60 * 1000;
const KEY = 'mgmt-token';

interface TokenState {
  token: string;
  issuedAt: number;
  previousToken?: string;
  previousUntil?: number;
}

/**
 * Holds the hub-provisioned management token in memory (loaded at boot, refreshed
 * on every control-queue delivery) so the MgmtTokenGuard can validate requests
 * without a DB hit per call. Persists to Postgres (`provisioned_secrets`) so the
 * token survives restarts. A rotation keeps the previous token valid for a short
 * grace window. Same public interface as the Mongo variant in the other satellites.
 */
@Injectable()
export class ProvisionedTokenService implements OnModuleInit {
  private readonly logger = new Logger(ProvisionedTokenService.name);
  private state: TokenState | null = null;

  constructor(
    @InjectRepository(ProvisionedSecret)
    private readonly repo: Repository<ProvisionedSecret>,
  ) {}

  async onModuleInit(): Promise<void> {
    const row = await this.repo.findOne({ where: { key: KEY } });
    if (row) {
      this.state = {
        token: row.token,
        issuedAt: Number(row.issuedAt),
        previousToken: row.previousToken ?? undefined,
        previousUntil: row.previousUntil != null ? Number(row.previousUntil) : undefined,
      };
      this.logger.log(`Loaded provisioned mgmt token (issuedAt=${this.state.issuedAt})`);
    }
  }

  /** Persist + cache a freshly provisioned/rotated token, keeping the old one in grace. */
  async set(token: string, issuedAt: number): Promise<void> {
    const previousToken = this.state?.token;
    const previousUntil = previousToken ? Date.now() + GRACE_MS : undefined;
    await this.repo.upsert(
      {
        key: KEY,
        token,
        issuedAt: String(issuedAt),
        previousToken: previousToken ?? null,
        previousUntil: previousUntil != null ? String(previousUntil) : null,
      },
      ['key'],
    );
    this.state = { token, issuedAt, previousToken, previousUntil };
    this.logger.log(`Applied provisioned mgmt token (issuedAt=${issuedAt})`);
  }

  hasToken(): boolean {
    return this.state !== null;
  }

  /** Timing-safe check against the current token, or the previous one within its grace window. */
  accepts(presented: string | undefined): boolean {
    if (!presented || !this.state) return false;
    if (eq(presented, this.state.token)) return true;
    if (
      this.state.previousToken &&
      this.state.previousUntil &&
      Date.now() < this.state.previousUntil &&
      eq(presented, this.state.previousToken)
    ) {
      return true;
    }
    return false;
  }
}

function eq(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}
