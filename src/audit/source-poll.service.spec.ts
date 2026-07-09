import axios, { type AxiosResponse } from 'axios';
import { SourcePollService } from './source-poll.service';
import { SourceHttpClient } from '../connections/source-http.client';
import type { ResolvedSourceConnection } from '../connections/source-connections.service';
import type { TableTemplate } from '../tables/entities/table-template.entity';

const requestSpy = jest.spyOn(axios, 'request');

function resp(data: unknown): AxiosResponse {
  return { data, headers: {}, status: 200, statusText: 'OK', config: {} } as unknown as AxiosResponse;
}

function makeConn(): ResolvedSourceConnection {
  return {
    id: 'c1',
    name: 'NetSuite',
    baseUrl: 'https://api.test',
    authType: 'bearer',
    credentials: { token: 'test-token' },
    defaultHeaders: {},
    pagination: { type: 'page', recordsPath: 'data.rows', pageParam: 'page', pageSize: 2, startPage: 1 },
    active: true,
  };
}

function makeTemplate(): TableTemplate {
  return {
    key: 'orders',
    label: 'Orders',
    perConnection: false,
    idField: 'id',
    columns: [],
    audit: {
      connectionId: 'c1',
      method: 'GET',
      path: 'orders',
      recordsPath: 'data.rows',
      incremental: { updatedAtField: 'updatedAt', sinceParam: 'since', sinceIn: 'query', sinceFormat: 'iso' },
    },
  } as unknown as TableTemplate;
}

interface IngestCall {
  connectionId: string;
  count: number;
}

function makeRuns() {
  let resolveDone!: () => void;
  const done = new Promise<void>((r) => (resolveDone = r));
  return {
    created: undefined as unknown,
    progressCount: 0,
    completed: undefined as Record<string, number> | undefined,
    failed: undefined as string | undefined,
    done,
    async create(input: unknown) {
      this.created = input;
      return { id: 'run-1' };
    },
    async progress() {
      this.progressCount++;
    },
    async complete(_id: string, counts: Record<string, number>) {
      this.completed = counts;
      resolveDone();
    },
    async fail(_id: string, message: string) {
      this.failed = message;
      resolveDone();
    },
  };
}

/** Fake `SourcePollState` repository (findOne) + `DataSource` (raw upsert query). */
function makeStateDoubles(existing: { lastUpdatedAt?: string } | null) {
  const upserts: Array<{ params: unknown[] }> = [];
  const stateRepo = {
    findOne: async () => existing,
  };
  const dataSource = {
    query: async (_sql: string, params: unknown[]) => {
      upserts.push({ params });
      return [];
    },
  };
  return { upserts, stateRepo, dataSource };
}

function buildService(opts: { template?: TableTemplate; existingState?: { lastUpdatedAt?: string } | null }) {
  const template = opts.template ?? makeTemplate();
  const conn = makeConn();
  const ingestCalls: IngestCall[] = [];

  const templates = { getByKey: async () => template };
  const rows = {
    ingest: async (_t: TableTemplate, records: Record<string, unknown>[], connectionId: string) => {
      ingestCalls.push({ connectionId, count: records.length });
      return { inserted: records.length, upserted: 0 };
    },
  };
  const connections = { resolveById: async () => conn };
  const client = new SourceHttpClient({ applyAuth: async (_c: unknown, c: unknown) => c } as never);
  const runs = makeRuns();
  const { upserts, stateRepo, dataSource } = makeStateDoubles(opts.existingState ?? null);

  const service = new SourcePollService(
    templates as never,
    rows as never,
    connections as never,
    client,
    runs as never,
    stateRepo as never,
    dataSource as never,
  );

  return { service, runs, upserts, ingestCalls, template };
}

beforeEach(() => {
  requestSpy.mockReset();
});

describe('SourcePollService — incremental audit poll', () => {
  it('injects the watermark, pages the source, ingests under the connectionId, and advances the watermark', async () => {
    const { service, runs, upserts, ingestCalls } = buildService({
      existingState: { lastUpdatedAt: '2024-01-01T00:00:00.000Z' },
    });

    const responses = [
      resp({ data: { rows: [{ id: 1, updatedAt: '2024-01-02T00:00:00.000Z' }, { id: 2, updatedAt: '2024-01-03T00:00:00.000Z' }] } }),
      resp({ data: { rows: [{ id: 3, updatedAt: '2024-01-05T00:00:00.000Z' }] } }), // short page → stop
    ];
    requestSpy.mockImplementation(async () => responses.shift()!);

    const runId = await service.poll({ tableKey: 'orders', trigger: 'manual' });
    await runs.done;

    expect(runId).toBe('run-1');

    // The run records the watermark floor it started from.
    expect((runs.created as { since?: string }).since).toBe('2024-01-01T00:00:00.000Z');

    // The first request carried the incremental `since` param.
    const firstUrl = new URL((requestSpy.mock.calls[0][0] as { url: string }).url);
    expect(firstUrl.searchParams.get('since')).toBe('2024-01-01T00:00:00.000Z');

    // Every page's records were ingested under the audit connectionId.
    expect(ingestCalls).toEqual([
      { connectionId: 'c1', count: 2 },
      { connectionId: 'c1', count: 1 },
    ]);

    // Watermark advanced to the newest updatedAt seen and counted what it saw.
    // Upsert params: [table_key, connection_id, last_updated_at, total_seen].
    expect(upserts).toHaveLength(1);
    expect(upserts[0].params[2]).toBe('2024-01-05T00:00:00.000Z');
    expect(upserts[0].params[3]).toBe(3);

    // Run completed with the right totals.
    expect(runs.completed).toEqual({ pages: 2, fetched: 3, inserted: 3, upserted: 0 });
    expect(runs.failed).toBeUndefined();
  });

  it('sends no `since` on the first ever run (no prior watermark)', async () => {
    const { service, runs } = buildService({ existingState: null });
    requestSpy.mockResolvedValueOnce(resp({ data: { rows: [{ id: 1, updatedAt: '2024-02-01T00:00:00.000Z' }] } }));

    await service.poll({ tableKey: 'orders' });
    await runs.done;

    const firstUrl = new URL((requestSpy.mock.calls[0][0] as { url: string }).url);
    expect(firstUrl.searchParams.has('since')).toBe(false);
    expect((runs.created as { since: string | null }).since).toBeNull();
  });

  it('marks the run failed (not completed) when the source errors mid-poll', async () => {
    const { service, runs, upserts } = buildService({ existingState: null });
    requestSpy.mockRejectedValueOnce(new Error('boom: 503 from source'));

    await service.poll({ tableKey: 'orders' });
    await runs.done;

    expect(runs.completed).toBeUndefined();
    expect(runs.failed).toMatch(/boom: 503/);
    // A failed run must not advance the watermark.
    expect(upserts).toHaveLength(0);
  });
});

describe('SourcePollService — validation', () => {
  it('rejects a table that has no audit config', async () => {
    const template = makeTemplate();
    delete (template as { audit?: unknown }).audit;
    const { service } = buildService({ template });

    await expect(service.poll({ tableKey: 'orders' })).rejects.toThrow(/no audit config/);
  });

  it('rejects when neither the audit config nor the call supplies a connectionId', async () => {
    const template = makeTemplate();
    (template.audit as { connectionId: string }).connectionId = '';
    const { service } = buildService({ template });

    await expect(service.poll({ tableKey: 'orders' })).rejects.toThrow(/no connectionId/);
  });
});
