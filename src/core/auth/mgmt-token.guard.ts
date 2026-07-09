import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { timingSafeEqual } from 'crypto';

@Injectable()
export class MgmtTokenGuard implements CanActivate {
  constructor(private readonly cfg: ConfigService) {}

  canActivate(ctx: ExecutionContext): boolean {
    const token = this.cfg.get<string>('SATELLITE_MGMT_TOKEN');
    if (!token) return true;

    const req = ctx.switchToHttp().getRequest<{ headers: Record<string, string | undefined> }>();
    const provided = req.headers['x-satellite-token'];
    if (!provided) throw new UnauthorizedException('X-Satellite-Token required');

    const a = Buffer.from(provided);
    const b = Buffer.from(token);
    if (a.length !== b.length || !timingSafeEqual(a, b)) {
      throw new UnauthorizedException('Invalid X-Satellite-Token');
    }
    return true;
  }
}
