import axios, { type AxiosResponse } from 'axios';
import { SourceHttpClient } from './source-http.client';
import type { ResolvedSourceConnection } from './source-connections.service';

/**
 * Hermetic spec for the handshake (connection test) and single-request
 * write-back call. `axios` is the mock HTTP source. No network, no DB.
 */

const requestSpy = jest.spyOn(axios, 'request');

function resp(data: unknown, headers: Record<string, string> = {}): AxiosResponse {
  return { data, headers, status: 200, statusText: 'OK', config: {} } as unknown as AxiosResponse;
}

function makeConn(): ResolvedSourceConnection {
  return {
    id: 'c1',
    name: 'src',
    clave: null,
    baseUrl: 'https://api.test',
    authType: 'bearer',
    credentials: { token: 'test-token' },
    defaultHeaders: {},
    active: true,
  };
}

// applyAuth is a no-op here; auth signing is exercised elsewhere.
const fakeAuth = { applyAuth: async (_c: unknown, config: unknown) => config };
const client = new SourceHttpClient(fakeAuth as never);

function urlOfCall(i: number): URL {
  const cfg = requestSpy.mock.calls[i][0] as { url: string };
  return new URL(cfg.url);
}

beforeEach(() => {
  requestSpy.mockReset();
});

describe('SourceHttpClient — handshake (connection test)', () => {
  it('reports the HTTP status and never throws on 4xx/5xx', async () => {
    const conn = makeConn();
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
    const conn: ResolvedSourceConnection = { ...makeConn(), baseUrl: 'https://api.test/v2' };
    requestSpy.mockResolvedValueOnce({ ...resp({ id: 'ext-1' }), status: 200 } as AxiosResponse);

    await client.send(conn, { method: 'PUT', path: '/orders' }, {});

    const u = urlOfCall(0);
    expect(u.pathname).toBe('/v2/orders');
  });
});

describe('SourceHttpClient — send() (row write-back)', () => {
  it('sends the body as-is with the given method and reports the response status/data', async () => {
    const conn = makeConn();
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
    const conn = makeConn();
    requestSpy.mockResolvedValueOnce({ ...resp({ error: 'nope' }), status: 500, statusText: 'Server Error' } as AxiosResponse);

    const result = await client.send(conn, { method: 'PATCH', path: '/records/A1' }, {});
    expect(result.status).toBe(500);
    expect((requestSpy.mock.calls[0][0] as { validateStatus?: () => boolean }).validateStatus?.()).toBe(true);
  });
});
