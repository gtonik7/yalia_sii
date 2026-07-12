import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { timingSafeEqual } from 'crypto';
import { ProvisionedTokenService } from './provisioned-token.service';

/**
 * Authenticates hub→satellite management calls via `X-Satellite-Token`. Accepts
 * either the hub-provisioned token (delivered over `sat-<key>-control`, incl. the
 * previous one during a rotation grace window) or the legacy env `SATELLITE_MGMT_TOKEN`
 * (bootstrap/override).
 *
 * Fail-closed in production: when NEITHER a provisioned token nor an env token is
 * configured, a management API with no auth is an open door, so it's locked
 * (401). In dev it stays open for local work. Approved satellites always have a
 * provisioned token, so this only bites a misconfigured/unapproved deployment.
 */
@Injectable()
export class MgmtTokenGuard implements CanActivate {
  constructor(
    private readonly cfg: ConfigService,
    private readonly tokens: ProvisionedTokenService,
  ) {}

  canActivate(ctx: ExecutionContext): boolean {
    const req = ctx.switchToHttp().getRequest<{ headers: Record<string, string | undefined> }>();
    const provided = req.headers['x-satellite-token'];
    const envToken = this.cfg.get<string>('SATELLITE_MGMT_TOKEN');
    const hasProvisioned = this.tokens.hasToken();

    // Nothing configured: fail-closed in production, open in dev.
    if (!hasProvisioned && !envToken) {
      if (process.env.NODE_ENV === 'production') {
        throw new UnauthorizedException('Management API locked: no provisioned or env token configured');
      }
      return true;
    }

    if (!provided) throw new UnauthorizedException('X-Satellite-Token required');
    if (hasProvisioned && this.tokens.accepts(provided)) return true;
    if (envToken && eq(provided, envToken)) return true;
    throw new UnauthorizedException('Invalid X-Satellite-Token');
  }
}

function eq(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}
