import { Injectable, Logger } from '@nestjs/common';
import axios, { type AxiosRequestConfig, type AxiosResponse } from 'axios';
import { SourceAuthService } from './source-auth.service';
import type { ResolvedSourceConnection } from './source-connections.service';
import type { PaginationConfig } from './entities/source-connection.entity';
import { getArrayByPath, getByPath, getNumberByPath, isTruthyFlag } from '../core/http/json-path.util';

/** A single request the poller wants to page over. */
export interface SourceRequest {
  /** Path appended to the connection baseUrl; may carry its own query string. */
  path: string;
  method: 'GET' | 'POST';
  /** Static query params merged into every page request. */
  query?: Record<string, string>;
  /** Request body (POST searches). */
  body?: unknown;
}

/**
 * Where the next page lives. The poller is stateless about pagination shape: it
 * just carries this token from one `fetchPage` call to the next until it is null.
 */
export type PageState =
  | { kind: 'page'; page: number }
  | { kind: 'offset'; offset: number }
  | { kind: 'cursor'; cursor: string | null }
  | { kind: 'url'; url: string }
  | { kind: 'single' };

export interface PageResult {
  records: Record<string, unknown>[];
  next: PageState | null;
  /** Total record count if the source advertised one (for run progress). */
  total?: number;
}

const REQUEST_TIMEOUT_MS = 60_000;

@Injectable()
export class SourceHttpClient {
  private readonly logger = new Logger(SourceHttpClient.name);

  constructor(private readonly auth: SourceAuthService) {}

  /** Initial page token derived from the connection's pagination strategy. */
  initialState(conn: ResolvedSourceConnection, req: SourceRequest): PageState {
    const p = conn.pagination;
    switch (p.type) {
      case 'page':
        return { kind: 'page', page: p.startPage ?? 1 };
      case 'offset':
        return { kind: 'offset', offset: 0 };
      case 'cursor':
        return { kind: 'cursor', cursor: null };
      case 'link':
      case 'nextUrl':
        return { kind: 'url', url: this.composeUrl(conn, req, {}) };
      case 'none':
      default:
        return { kind: 'single' };
    }
  }

  /** Fetch one page and compute the token for the page after it (or null). */
  async fetchPage(
    conn: ResolvedSourceConnection,
    req: SourceRequest,
    state: PageState,
  ): Promise<PageResult> {
    const p = conn.pagination;
    const extra = this.paginationParams(p, state);
    const url = state.kind === 'url' ? state.url : this.composeUrl(conn, req, extra);

    let config: AxiosRequestConfig = {
      url,
      method: req.method,
      headers: { Accept: 'application/json', ...conn.defaultHeaders },
      data: req.method === 'POST' ? req.body : undefined,
      timeout: REQUEST_TIMEOUT_MS,
    };
    config = await this.auth.applyAuth(conn, config);

    const resp = await axios.request(config);
    const records = getArrayByPath(resp.data, p.recordsPath);
    const total = getNumberByPath(resp.data, p.totalResultsPath);

    return { records, next: this.nextState(conn, req, state, resp, records), total };
  }

  /**
   * Build the absolute URL with static + pagination query params merged in.
   *
   * `baseUrl` is taken verbatim, keeping any path AND query string it carries —
   * so a connection can point straight at a full NetSuite restlet URL
   * (`…/restlet.nl?script=123&deploy=1`), exactly like the emitter does. `req.path`
   * is *appended* to that base path (never replaces it); an empty or `/` path means
   * "hit the baseUrl as-is" (the connection "test" probe). Query precedence,
   * lowest→highest: baseUrl query < req.query < pagination params.
   */
  private composeUrl(
    conn: ResolvedSourceConnection,
    req: SourceRequest,
    extra: Record<string, string | number>,
  ): string {
    const url = new URL(conn.baseUrl);
    const rel = (req.path ?? '').replace(/^\/+/, '');
    if (rel) {
      url.pathname = `${url.pathname.replace(/\/+$/, '')}/${rel}`;
    }
    for (const [k, v] of Object.entries(req.query ?? {})) url.searchParams.set(k, v);
    for (const [k, v] of Object.entries(extra)) url.searchParams.set(k, String(v));
    return url.toString();
  }

  /** Request-side pagination params for the current page token. */
  private paginationParams(p: PaginationConfig, state: PageState): Record<string, string | number> {
    const out: Record<string, string | number> = {};
    if (state.kind === 'page') {
      if (p.pageParam) out[p.pageParam] = state.page;
      if (p.pageSizeParam && p.pageSize) out[p.pageSizeParam] = p.pageSize;
    } else if (state.kind === 'offset') {
      if (p.offsetParam) out[p.offsetParam] = state.offset;
      if (p.limitParam && p.pageSize) out[p.limitParam] = p.pageSize;
    } else if (state.kind === 'cursor' && state.cursor != null && p.cursorParam) {
      out[p.cursorParam] = state.cursor;
    }
    return out;
  }

  /** Decide whether there is a page after this one, and what its token is. */
  private nextState(
    conn: ResolvedSourceConnection,
    req: SourceRequest,
    state: PageState,
    resp: AxiosResponse,
    records: Record<string, unknown>[],
  ): PageState | null {
    const p = conn.pagination;

    switch (state.kind) {
      case 'single':
        return null;

      case 'page': {
        if (p.isLastPath && isTruthyFlag(getByPath(resp.data, p.isLastPath))) return null;
        const totalPages = getNumberByPath(resp.data, p.totalPagesPath);
        if (totalPages !== undefined) {
          const indexZeroBased = state.page - (p.startPage ?? 1);
          if (indexZeroBased + 1 >= totalPages) return null;
        } else if (p.pageSize ? records.length < p.pageSize : records.length === 0) {
          return null;
        }
        return { kind: 'page', page: state.page + 1 };
      }

      case 'offset': {
        const total = getNumberByPath(resp.data, p.totalResultsPath);
        const nextOffset = state.offset + (p.pageSize ?? records.length);
        if (total !== undefined) {
          if (nextOffset >= total) return null;
        } else if (p.pageSize ? records.length < p.pageSize : records.length === 0) {
          return null;
        }
        return { kind: 'offset', offset: nextOffset };
      }

      case 'cursor': {
        const next = getByPath(resp.data, p.nextCursorPath);
        if (next == null || next === '') return null;
        return { kind: 'cursor', cursor: String(next) };
      }

      case 'url': {
        const nextUrl =
          p.type === 'link'
            ? parseLinkNext(resp.headers?.link ?? resp.headers?.Link)
            : asUrl(getByPath(resp.data, p.nextUrlPath));
        return nextUrl ? { kind: 'url', url: nextUrl } : null;
      }
    }
  }

  /** Lightweight reachability check used by the connection "test" endpoint. */
  async probe(conn: ResolvedSourceConnection, req: SourceRequest): Promise<number> {
    const state = this.initialState(conn, req);
    const page = await this.fetchPage(conn, req, state);
    return page.records.length;
  }

  /**
   * Connection "test" round-trip: apply auth and fire a single request at the
   * configured handshake (method/path/query/body). Unlike {@link probe} it does
   * NOT parse `recordsPath` — a handshake is a reachability/auth check, so the
   * response body shape is irrelevant; we just report the HTTP status. Transport
   * errors (DNS, refused, timeout) still throw and are surfaced by the caller.
   */
  async handshake(
    conn: ResolvedSourceConnection,
    hs: { method: string; path?: string; query?: Record<string, string>; body?: unknown },
  ): Promise<{ status: number; statusText: string }> {
    const method = hs.method.toUpperCase();
    const url = this.composeUrl(conn, { path: hs.path ?? '/', method: 'GET', query: hs.query }, {});
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
   * Single non-paginated write request (row write-back). Unlike {@link fetchPage}
   * there is no `recordsPath` parsing — the caller owns interpreting the response.
   * Like {@link handshake}, `validateStatus` never throws on 4xx/5xx so the caller
   * can persist a per-row write status instead of losing the local edit to an
   * unhandled exception.
   */
  async send(
    conn: ResolvedSourceConnection,
    req: { method: 'PUT' | 'PATCH' | 'POST'; path: string; query?: Record<string, string> },
    body: unknown,
  ): Promise<{ status: number; data: unknown }> {
    const url = this.composeUrl(conn, { path: req.path, method: 'GET', query: req.query }, {});
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

/** Parse an RFC 5988 `Link` header and return the URL with rel="next", if any. */
function parseLinkNext(header: unknown): string | null {
  if (typeof header !== 'string') return null;
  for (const part of header.split(',')) {
    const match = /<([^>]+)>\s*;\s*rel="?next"?/i.exec(part.trim());
    if (match) return match[1];
  }
  return null;
}

function asUrl(value: unknown): string | null {
  return typeof value === 'string' && value.trim() !== '' ? value : null;
}
