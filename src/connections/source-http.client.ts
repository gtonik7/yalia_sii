import { Injectable, Logger } from '@nestjs/common';
import axios, { type AxiosRequestConfig } from 'axios';
import { SourceAuthService } from './source-auth.service';
import type { ResolvedSourceConnection } from './source-connections.service';

/** A single outbound request against a connection. */
export interface SourceRequest {
  /** Path appended to the connection baseUrl; may carry its own query string. */
  path: string;
  method: 'GET' | 'POST';
  /** Static query params merged into every page request. */
  query?: Record<string, string>;
  /** Request body (POST searches). */
  body?: unknown;
}

const REQUEST_TIMEOUT_MS = 60_000;

@Injectable()
export class SourceHttpClient {
  private readonly logger = new Logger(SourceHttpClient.name);

  constructor(private readonly auth: SourceAuthService) {}

  /**
   * Build the absolute URL with static query params merged in.
   *
   * `baseUrl` is taken verbatim, keeping any path AND query string it carries —
   * so a connection can point straight at a full NetSuite restlet URL
   * (`…/restlet.nl?script=123&deploy=1`), exactly like the emitter does. `req.path`
   * is *appended* to that base path (never replaces it); an empty or `/` path means
   * "hit the baseUrl as-is" (the connection "test" handshake).
   */
  private composeUrl(conn: ResolvedSourceConnection, req: SourceRequest): string {
    const url = new URL(conn.baseUrl);
    const rel = (req.path ?? '').replace(/^\/+/, '');
    if (rel) {
      url.pathname = `${url.pathname.replace(/\/+$/, '')}/${rel}`;
    }
    for (const [k, v] of Object.entries(req.query ?? {})) url.searchParams.set(k, v);
    return url.toString();
  }

  /**
   * Connection "test" round-trip: apply auth and fire a single request at the
   * configured handshake (method/path/query/body) and report the HTTP status.
   * Transport errors (DNS, refused, timeout) still throw and are surfaced by
   * the caller.
   */
  async handshake(
    conn: ResolvedSourceConnection,
    hs: { method: string; path?: string; query?: Record<string, string>; body?: unknown },
  ): Promise<{ status: number; statusText: string }> {
    const method = hs.method.toUpperCase();
    const url = this.composeUrl(conn, { path: hs.path ?? '/', method: 'GET', query: hs.query });
    const sendsBody = method !== 'GET' && method !== 'HEAD';

    let config: AxiosRequestConfig = {
      url,
      method,
      headers: { Accept: 'application/json', ...conn.defaultHeaders },
      data: sendsBody ? hs.body : undefined,
      timeout: REQUEST_TIMEOUT_MS,
      // Report any HTTP status (incl. 4xx/5xx) instead of throwing, so the test
      // can tell "auth rejected (401)" apart from "host unreachable".
      validateStatus: () => true,
    };
    config = await this.auth.applyAuth(conn, config);

    const resp = await axios.request(config);
    return { status: resp.status, statusText: resp.statusText };
  }

  /**
   * Single write request (row write-back). The caller owns interpreting the
   * response. Like {@link handshake}, `validateStatus` never throws on 4xx/5xx
   * so the caller can persist a per-row write status instead of losing the
   * local edit to an unhandled exception.
   */
  async send(
    conn: ResolvedSourceConnection,
    req: { method: 'PUT' | 'PATCH' | 'POST'; path: string; query?: Record<string, string> },
    body: unknown,
  ): Promise<{ status: number; data: unknown }> {
    const url = this.composeUrl(conn, { path: req.path, method: 'GET', query: req.query });
    let config: AxiosRequestConfig = {
      url,
      method: req.method,
      headers: { Accept: 'application/json', 'Content-Type': 'application/json', ...conn.defaultHeaders },
      data: body,
      timeout: REQUEST_TIMEOUT_MS,
      validateStatus: () => true,
    };
    config = await this.auth.applyAuth(conn, config);

    const resp = await axios.request(config);
    return { status: resp.status, data: resp.data };
  }
}
