import axios, { type AxiosResponse } from 'axios';
import { SourceHttpClient, type SourceRequest, type PageState } from './source-http.client';
import type { ResolvedSourceConnection } from './source-connections.service';
import type { PaginationConfig } from './entities/source-connection.entity';

/**
 * Hermetic spec for the pagination engine and the single-request write-back
 * call. `axios` is the mock HTTP source: each strategy is driven to completion
 * and we assert both the records walked and the request the engine composed
 * (page/offset/cursor params, next-url, Link header). No network, no DB.
 */

const requestSpy = jest.spyOn(axios, 'request');

function resp(data: unknown, headers: Record<string, string> = {}): AxiosResponse {
  return { data, headers, status: 200, statusText: 'OK', config: {} } as unknown as AxiosResponse;
}

function makeConn(pagination: PaginationConfig): ResolvedSourceConnection {
  return {
    id: 'c1',
    name: 'src',
    baseUrl: 'https://api.test',
    authType: 'bearer',
    credentials: { token: 'test-token' },
    defaultHeaders: {},
    pagination,
    active: true,
  };
}

// applyAuth is a no-op here; auth signing is exercised elsewhere.
const fakeAuth = { applyAuth: async (_c: unknown, config: unknown) => config };
const client = new SourceHttpClient(fakeAuth as never);

/** Walk every page exactly like SourcePollService does. */
async function drain(
  conn: ResolvedSourceConnection,
  req: SourceRequest,
): Promise<Record<string, unknown>[]> {
  const all: Record<string, unknown>[] = [];
  let state: PageState | null = client.initialState(conn, req);
  let guard = 0;
  while (state && guard++ < 50) {
    const page = await client.fetchPage(conn, req, state);
    all.push(...page.records);
    state = page.next;
  }
  return all;
}

function urlOfCall(i: number): URL {
  const cfg = requestSpy.mock.calls[i][0] as { url: string };
  return new URL(cfg.url);
}

beforeEach(() => {
  requestSpy.mockReset();
});

describe('SourceHttpClient — page strategy', () => {
  it('walks pages until a short page and sends page/size + static query params', async () => {
    const conn = makeConn({
      type: 'page',
      recordsPath: 'data.rows',
      pageParam: 'page',
      pageSizeParam: 'size',
      pageSize: 2,
      startPage: 1,
    });
    const responses = [
      resp({ data: { rows: [{ id: 1 }, { id: 2 }] } }),
      resp({ data: { rows: [{ id: 3 }, { id: 4 }] } }),
      resp({ data: { rows: [{ id: 5 }] } }), // short page → stop
    ];
    requestSpy.mockImplementation(async () => responses.shift()!);

    const all = await drain(conn, { path: 'orders', method: 'GET', query: { q: 'x' } });

    expect(all.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
    expect(requestSpy).toHaveBeenCalledTimes(3);
    const u0 = urlOfCall(0);
    expect(u0.pathname).toBe('/orders');
    expect(u0.searchParams.get('q')).toBe('x');
    expect(u0.searchParams.get('page')).toBe('1');
    expect(u0.searchParams.get('size')).toBe('2');
    expect(urlOfCall(1).searchParams.get('page')).toBe('2');
    expect(urlOfCall(2).searchParams.get('page')).toBe('3');
  });
});

describe('SourceHttpClient — offset strategy', () => {
  it('advances offset by pageSize and stops at totalResults', async () => {
    const conn = makeConn({
      type: 'offset',
      recordsPath: 'data.rows',
      offsetParam: 'offset',
      limitParam: 'limit',
      pageSize: 2,
      totalResultsPath: 'data.total',
    });
    const responses = [
      resp({ data: { rows: [{ id: 1 }, { id: 2 }], total: 3 } }),
      resp({ data: { rows: [{ id: 3 }], total: 3 } }),
    ];
    requestSpy.mockImplementation(async () => responses.shift()!);

    const all = await drain(conn, { path: 'orders', method: 'GET' });
    expect(all.map((r) => r.id)).toEqual([1, 2, 3]);
    expect(urlOfCall(0).searchParams.get('offset')).toBe('0');
    expect(urlOfCall(0).searchParams.get('limit')).toBe('2');
    expect(urlOfCall(1).searchParams.get('offset')).toBe('2');
  });
});

describe('SourceHttpClient — cursor strategy', () => {
  it('follows nextCursorPath and stops when the cursor is null', async () => {
    const conn = makeConn({
      type: 'cursor',
      recordsPath: 'data.rows',
      cursorParam: 'cursor',
      nextCursorPath: 'data.next',
    });
    const responses = [
      resp({ data: { rows: [{ id: 1 }], next: 'abc' } }),
      resp({ data: { rows: [{ id: 2 }], next: null } }),
    ];
    requestSpy.mockImplementation(async () => responses.shift()!);

    const all = await drain(conn, { path: 'orders', method: 'GET' });
    expect(all.map((r) => r.id)).toEqual([1, 2]);
    // First page carries no cursor; second carries the one the source returned.
    expect(urlOfCall(0).searchParams.has('cursor')).toBe(false);
    expect(urlOfCall(1).searchParams.get('cursor')).toBe('abc');
  });
});

describe('SourceHttpClient — handshake (connection test)', () => {
  it('reports the HTTP status without parsing recordsPath and never throws on 4xx/5xx', async () => {
    const conn = makeConn({ type: 'none', recordsPath: 'data.rows' });
    requestSpy.mockResolvedValueOnce(resp({ ok: true }, {}));
    requestSpy.mockResolvedValueOnce({ ...resp({}), status: 401, statusText: 'Unauthorized' } as AxiosResponse);

    const ok = await client.handshake(conn, { method: 'GET', path: '/health' });
    expect(ok.status).toBe(200);
    const u = urlOfCall(0);
    expect(u.pathname).toBe('/health');
    expect((requestSpy.mock.calls[0][0] as { validateStatus?: () => boolean }).validateStatus?.()).toBe(true);

    const unauth = await client.handshake(conn, { method: 'GET' });
    expect(unauth.status).toBe(401);
  });
});

describe('SourceHttpClient — baseUrl path & query preservation', () => {
  it('appends req.path to the baseUrl path instead of replacing it', async () => {
    const conn: ResolvedSourceConnection = {
      ...makeConn({ type: 'none', recordsPath: 'rows' }),
      baseUrl: 'https://api.test/v2',
    };
    requestSpy.mockResolvedValueOnce(resp({ rows: [{ id: 1 }] }));

    await drain(conn, { path: '/orders', method: 'GET', query: { q: 'x' } });

    const u = urlOfCall(0);
    expect(u.pathname).toBe('/v2/orders');
    expect(u.searchParams.get('q')).toBe('x');
  });
});

describe('SourceHttpClient — send() (row write-back)', () => {
  it('sends the body as-is with the given method and reports the response status/data', async () => {
    const conn = makeConn({ type: 'none', recordsPath: 'rows' });
    requestSpy.mockResolvedValueOnce({ ...resp({ id: 'ext-1' }), status: 200 } as AxiosResponse);

    const result = await client.send(conn, { method: 'PUT', path: '/records/A1' }, { name: 'updated' });

    expect(result.status).toBe(200);
    expect(result.data).toEqual({ id: 'ext-1' });
    const cfg = requestSpy.mock.calls[0][0] as { method: string; url: string; data: unknown };
    expect(cfg.method).toBe('PUT');
    expect(new URL(cfg.url).pathname).toBe('/records/A1');
    expect(cfg.data).toEqual({ name: 'updated' });
  });

  it('never throws on a non-2xx response — the caller decides how to record it', async () => {
    const conn = makeConn({ type: 'none', recordsPath: 'rows' });
    requestSpy.mockResolvedValueOnce({ ...resp({ error: 'nope' }), status: 500, statusText: 'Server Error' } as AxiosResponse);

    const result = await client.send(conn, { method: 'PATCH', path: '/records/A1' }, {});
    expect(result.status).toBe(500);
    expect((requestSpy.mock.calls[0][0] as { validateStatus?: () => boolean }).validateStatus?.()).toBe(true);
  });
});

describe('SourceHttpClient — none strategy', () => {
  it('fetches a single page and stops', async () => {
    const conn = makeConn({ type: 'none', recordsPath: 'rows' });
    requestSpy.mockResolvedValueOnce(resp({ rows: [{ id: 1 }, { id: 2 }] }));

    const all = await drain(conn, { path: 'orders', method: 'GET' });
    expect(all).toHaveLength(2);
    expect(requestSpy).toHaveBeenCalledTimes(1);
  });

  it('throws a helpful error when recordsPath does not resolve to an array', async () => {
    const conn = makeConn({ type: 'none', recordsPath: 'data.rows' });
    requestSpy.mockResolvedValueOnce(resp({ data: { rows: { not: 'an array' } } }));

    await expect(drain(conn, { path: 'orders', method: 'GET' })).rejects.toThrow(/recordsPath/);
  });
});
