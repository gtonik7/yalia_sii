import { Injectable } from '@nestjs/common';
import type { AxiosRequestConfig } from 'axios';
import type { ResolvedSourceConnection } from './source-connections.service';

/** Applies a source connection's bearer token to an outgoing axios request. */
@Injectable()
export class SourceAuthService {
  async applyAuth(
    conn: ResolvedSourceConnection,
    config: AxiosRequestConfig,
  ): Promise<AxiosRequestConfig> {
    const headers: Record<string, string> = { ...(config.headers as Record<string, string>) };
    headers.Authorization = `Bearer ${conn.credentials.token}`;
    return { ...config, headers };
  }
}
