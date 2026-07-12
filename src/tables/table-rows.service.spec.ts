import { DataSource } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { Logger } from '@nestjs/common';
import { Queue, type Job } from 'bullmq';
import { TableRowsService } from './table-rows.service';
import { WriteEventProcessor } from './write-event.processor';
import { TableWriteBatchService } from './table-write-batch.service';
import { SiiResultProcessor } from '../callbacks/sii-result.processor';
import { TableRow } from './entities/table-row.entity';
import { TableTemplate } from './entities/table-template.entity';
import { DatasetQuery } from '../datasets/dataset.types';
import type { ResolvedSourceConnection } from '../connections/source-connections.service';
import type { WriteEventJobData } from './write-event.types';
import { QUEUES } from '../core/queues/queues.constants';

/** Reads a `sendMock` call's `{customerId, payload}` body — `payload` is ALWAYS an array now. */
function payloadRows(call: unknown[]): { id: string }[] {
  return (call[2] as { payload: { id: string }[] }).payload;
}

/**
 * Integration spec for TableRowsService against a real Postgres/TimescaleDB
 * instance (the JSONB filter/sort/upsert semantics this service relies on —
 * ILIKE, ::numeric/::boolean casts, tsvector search, partial-index ON
 * CONFLICT — can't be faithfully re-derived by an in-memory fake without
 * re-implementing Postgres itself).
 *
 * PREREQUISITE: the dev Timescale container must be up AND migrated before
 * running this spec: `docker compose -f docker-compose.dev.yml up -d` then
 * `npm run migration:run` (see .env for the connection details this test
 * reuses). Connects with `synchronize: false` and reuses the real migrated
 * schema — `synchronize: true` against an already-migrated DB trips over
 * TypeORM's generated-column metadata handling (`typeorm_metadata` table),
 * and the hypertable/compression DDL isn't something synchronize can express
 * anyway. The `idField` upsert arbiter index (normally managed by
 * TableIndexManagerService for real templates) is created by hand below
 * since these tests build TableTemplate objects in-memory without ever going
 * through TableTemplatesService.
 */

const DB_HOST = process.env.DB_HOST ?? 'localhost';
const DB_PORT = Number(process.env.DB_PORT ?? 5434);
const DB_USER = process.env.DB_USER ?? 'yalia';
const DB_PASSWORD = process.env.DB_PASSWORD ?? 'yalia';
const DB_NAME = process.env.DB_NAME ?? 'yalia_sii';

const REDIS_HOST = process.env.REDIS_HOST ?? 'localhost';
const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6380);

let dataSource: DataSource;
// Real BullMQ queue against dev Redis — the event-mode edit path enqueues a
// targeted single-row send that the WriteEventProcessor drains (see the
// "Event send" describe block below); driving it end-to-end through a real
// queue is more faithful than a fake. Shares this one file/worker with the
// rest of these specs (rather than a standalone spec file) specifically so its
// TRUNCATE table_rows can't race against another file's in a separate Jest
// worker — Postgres deadlocks (or silently-wrong row counts) resulted from
// splitting this out before.
let queue: Queue<WriteEventJobData>;

beforeAll(async () => {
  dataSource = new DataSource({
    type: 'postgres',
    host: DB_HOST,
    port: DB_PORT,
    username: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    entities: [TableRow],
    synchronize: false,
  });
  try {
    await dataSource.initialize();
    // Sanity check that migrations have actually run — a clearer failure than
    // whatever error the first real query below would produce otherwise.
    await dataSource.query('SELECT 1 FROM table_rows LIMIT 0');
  } catch (err) {
    const cause = err instanceof Error ? err.message : String(err);
    throw new Error(
      `No se pudo conectar/usar Postgres/TimescaleDB de desarrollo en ${DB_HOST}:${DB_PORT}. ` +
        `Levantar el contenedor y migrar primero: docker compose -f docker-compose.dev.yml up -d && npm run migration:run\n` +
        `Causa original: ${cause}`,
    );
  }

  // Arbiter index for the idField upsert path — normally created reactively by
  // TableIndexManagerService when a template with idField='id' is saved.
  // Every template in this spec uses key='orders'.
  await dataSource.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ix_test_orders_id
      ON table_rows (connection_id, (data ->> 'id'))
      WHERE table_key = 'orders'
  `);

  queue = new Queue<WriteEventJobData>(QUEUES.WRITE_EVENT, {
    connection: { host: REDIS_HOST, port: REDIS_PORT },
  });
  await queue.obliterate({ force: true }).catch(() => {});
});

afterAll(async () => {
  await queue?.obliterate({ force: true }).catch(() => {});
  await queue?.close();
  if (dataSource?.isInitialized) {
    await dataSource.query('DROP INDEX IF EXISTS ix_test_orders_id');
    await dataSource.destroy();
  }
});

beforeEach(async () => {
  await dataSource.query('TRUNCATE table_rows');
  await queue.obliterate({ force: true }).catch(() => {});
});

function makeTemplate(over: Partial<TableTemplate>): TableTemplate {
  return {
    key: 'orders',
    label: 'Orders',
    idField: '',
    columns: [
      { key: 'id', label: 'ID', type: 'string', filterable: true, sortable: true },
      { key: 'status', label: 'Status', type: 'string', filterable: true, sortable: false },
      { key: 'total', label: 'Total', type: 'number', filterable: true, sortable: true },
    ],
    ...over,
  } as TableTemplate;
}

function query(over: Partial<DatasetQuery>): DatasetQuery {
  return { page: 1, pageSize: 50, ...over };
}

async function firstRowId(tableKey: string): Promise<string> {
  const rows: { id: string }[] = await dataSource.query(
    `SELECT id FROM table_rows WHERE table_key = $1 ORDER BY created_at LIMIT 1`,
    [tableKey],
  );
  return rows[0].id;
}

async function allRows(tableKey: string): Promise<{ id: string; data: Record<string, unknown> }[]> {
  return dataSource.query(`SELECT id, data FROM table_rows WHERE table_key = $1 ORDER BY created_at`, [tableKey]);
}

interface SubmissionRow {
  id: string;
  submission_status: string | null;
  batch_id: string | null;
  write_status: 'sent' | 'error' | null;
  write_error: string | null;
}

async function submissionState(tableKey: string): Promise<SubmissionRow[]> {
  return dataSource.query(
    `SELECT id, submission_status, batch_id, write_status, write_error FROM table_rows WHERE table_key = $1 ORDER BY created_at`,
    [tableKey],
  );
}

async function submissionStatusForDataId(tableKey: string, dataId: string): Promise<string | null> {
  const [row]: { submission_status: string | null }[] = await dataSource.query(
    `SELECT submission_status FROM table_rows WHERE table_key = $1 AND (data ->> 'id') = $2`,
    [tableKey, dataId],
  );
  return row?.submission_status ?? null;
}

async function rowIdForDataId(tableKey: string, dataId: string): Promise<string> {
  const [row]: { id: string }[] = await dataSource.query(
    `SELECT id FROM table_rows WHERE table_key = $1 AND (data ->> 'id') = $2`,
    [tableKey, dataId],
  );
  return row.id;
}

describe('TableRowsService — per-connection navigation symmetry', () => {
  let service: TableRowsService;

  beforeEach(() => {
    service = new TableRowsService(dataSource, {} as never, {} as never, {} as never, { record: async () => ({}) } as never);
  });

  it('scopes rows to the connectionId on ingest and returns them only for that connection', async () => {
    const tpl = makeTemplate({ idField: 'id' });

    await service.ingest(tpl, [{ id: 'A1', status: 'paid' }], 'conn-A', 'trace-1');
    await service.ingest(tpl, [{ id: 'B1', status: 'paid' }], 'conn-B', 'trace-2');

    const fromA = await service.query(tpl, query({ connectionId: 'conn-A' }));
    expect(fromA.total).toBe(1);
    expect(fromA.rows[0].id).toBe('A1');

    const fromB = await service.query(tpl, query({ connectionId: 'conn-B' }));
    expect(fromB.total).toBe(1);
    expect(fromB.rows[0].id).toBe('B1');

    const fromC = await service.query(tpl, query({ connectionId: 'conn-C' }));
    expect(fromC.total).toBe(0);
  });

  it('upserts by idField scoped to the connection (no cross-connection collision)', async () => {
    const tpl = makeTemplate({ idField: 'id' });

    await service.ingest(tpl, [{ id: 'X', status: 'pending' }], 'conn-A', 't1');
    await service.ingest(tpl, [{ id: 'X', status: 'paid' }], 'conn-A', 't2');
    await service.ingest(tpl, [{ id: 'X', status: 'shipped' }], 'conn-B', 't3');

    const a = await service.query(tpl, query({ connectionId: 'conn-A' }));
    expect(a.total).toBe(1);
    expect(a.rows[0].status).toBe('paid');

    const b = await service.query(tpl, query({ connectionId: 'conn-B' }));
    expect(b.total).toBe(1);
    expect(b.rows[0].status).toBe('shipped');
  });

  it('exposes write/submission fields in the listing, not just in a single-row update response', async () => {
    const tpl = makeTemplate({ idField: 'id' });
    await service.ingest(tpl, [{ id: 'A1', status: 'draft' }], '', 't1');
    const rowId = await firstRowId('orders');
    await dataSource.query(
      `UPDATE table_rows SET write_status='sent', write_error=NULL, last_written_at=now(), external_ref='ext-1', submission_status='pending', sii_response='{"state":"PENDING"}'::jsonb WHERE id = $1`,
      [rowId],
    );

    const res = await service.query(tpl, query({}));

    expect(res.rows[0]).toMatchObject({
      _writeStatus: 'sent',
      _externalRef: 'ext-1',
      _submissionStatus: 'pending',
      _siiResponse: { state: 'PENDING' },
    });
    expect(res.rows[0]._lastWrittenAt).toBeInstanceOf(Date);
  });
});

describe('TableRowsService — query honors only declared filterable/sortable columns', () => {
  let service: TableRowsService;
  const tpl = makeTemplate({
    columns: [
      { key: 'id', label: 'ID', type: 'string', filterable: true, sortable: true },
      { key: 'status', label: 'Status', type: 'string', filterable: false, sortable: false },
      { key: 'total', label: 'Total', type: 'number', filterable: true, sortable: true },
    ],
  });

  beforeEach(async () => {
    service = new TableRowsService(dataSource, {} as never, {} as never, {} as never, { record: async () => ({}) } as never);
    await service.ingest(tpl, [{ id: 'A', status: 'paid', total: 30 }], '', 't');
    await service.ingest(tpl, [{ id: 'B', status: 'open', total: 10 }], '', 't');
    await service.ingest(tpl, [{ id: 'C', status: 'paid', total: 20 }], '', 't');
  });

  it('applies a filter on a filterable string column (substring, case-insensitive)', async () => {
    const res = await service.query(tpl, query({ filters: { id: 'a' } }));
    expect(res.rows.map((r) => r.id)).toEqual(['A']);
  });

  it('sorts by a sortable column ascending', async () => {
    const asc = await service.query(tpl, query({ sort: { key: 'total', dir: 'asc' } }));
    expect(asc.rows.map((r) => r.total)).toEqual([10, 20, 30]);
  });

  it('ignores free-text search (search_vector removed — filter by column instead)', async () => {
    // `search` is now a no-op: the STORED tsvector column + GIN index were dropped
    // for storage (see DropTableRowsSearchVector). The query returns everything the
    // other filters allow, unaffected by `search`.
    const res = await service.query(tpl, query({ search: 'open' }));
    const all = await service.query(tpl, query({}));
    expect(res.total).toBe(all.total);
  });
});

describe('TableRowsService — date-range filter on a "date" column', () => {
  let service: TableRowsService;
  const tpl = makeTemplate({
    columns: [
      { key: 'id', label: 'ID', type: 'string', filterable: true, sortable: true },
      { key: 'issuedAt', label: 'Issued', type: 'date', filterable: true, sortable: true },
    ],
  });

  beforeEach(async () => {
    service = new TableRowsService(dataSource, {} as never, {} as never, {} as never, { record: async () => ({}) } as never);
    // Mixed precision on purpose — a bare date and a full ISO timestamp both
    // need to compare correctly against each other, which naive text
    // comparison can't guarantee across differing lengths/formats.
    await service.ingest(tpl, [{ id: 'A', issuedAt: '2026-01-05' }], '', 't');
    await service.ingest(tpl, [{ id: 'B', issuedAt: '2026-06-15T10:00:00.000Z' }], '', 't');
    await service.ingest(tpl, [{ id: 'C', issuedAt: '2026-12-20' }], '', 't');
    // Garbage that violates the declared "date" type — must not crash the query,
    // just never match a range filter.
    await service.ingest(tpl, [{ id: 'D', issuedAt: 'not-a-date' }], '', 't');
  });

  it('includes a row exactly at the "desde" bound (date-only value, date-only bound)', async () => {
    const res = await service.query(tpl, query({ filters: { issuedAt_from: '2026-01-05' } }));
    expect(res.rows.map((r) => r.id)).toEqual(expect.arrayContaining(['A', 'B', 'C']));
    expect(res.rows.map((r) => r.id)).not.toContain('D');
  });

  it('narrows to a chronological range spanning mixed date/datetime precision', async () => {
    const res = await service.query(tpl, query({ filters: { issuedAt_from: '2026-02-01', issuedAt_until: '2026-07-01' } }));
    expect(res.rows.map((r) => r.id)).toEqual(['B']);
  });

  it('excludes a row whose stored value is not a parseable date, without failing the query', async () => {
    const res = await service.query(tpl, query({ filters: { issuedAt_from: '2020-01-01' } }));
    expect(res.rows.map((r) => r.id)).toEqual(expect.arrayContaining(['A', 'B', 'C']));
    expect(res.rows.map((r) => r.id)).not.toContain('D');
  });

  it('handles epoch milliseconds (pure-numeric values) in range filters', async () => {
    const tplNumeric = makeTemplate({
      columns: [
        { key: 'id', label: 'ID', type: 'string', filterable: true, sortable: true },
        { key: 'createdMs', label: 'Created (ms)', type: 'date', filterable: true, sortable: true },
      ],
    });
    // Ingest one row with ISO date, another with epoch milliseconds.
    const ms1 = new Date('2026-02-15').getTime(); // 1739635200000
    const ms2 = new Date('2026-06-10').getTime(); // 1754822400000
    await service.ingest(tplNumeric, [{ id: 'X', createdMs: ms1 }], '', 't');
    await service.ingest(tplNumeric, [{ id: 'Y', createdMs: '2026-12-25' }], '', 't');
    await service.ingest(tplNumeric, [{ id: 'Z', createdMs: ms2 }], '', 't');

    // Range filter: 2026-05-01 to 2026-07-01 should match only the epoch-ms row Z (June 10).
    const res = await service.query(tplNumeric, query({ filters: { createdMs_from: '2026-05-01', createdMs_until: '2026-07-01' } }));
    expect(res.rows.map((r) => r.id)).toEqual(['Z']);
  });
});

describe('TableRowsService — updateAndWrite (row edit + submission queuing)', () => {
  let service: TableRowsService;
  let sendMock: jest.Mock;
  let resolveByIdMock: jest.Mock;
  let enqueueMock: jest.Mock;
  const conn: ResolvedSourceConnection = {
    id: 'conn-1',
    name: 'SII',
    clave: null,
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    active: true,
  };

  async function buildService(tpl: TableTemplate, seedRows: Record<string, unknown>[] = [], seedConnectionId = '') {
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn();
    enqueueMock = jest.fn().mockResolvedValue({ id: 'job-1' });
    const fakeConnections = { resolveById: resolveByIdMock };
    const fakeClient = { send: sendMock };
    const fakeQueue = { add: enqueueMock };
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, fakeQueue as never, { record: async () => ({}) } as never);
    for (const data of seedRows) await service.ingest(tpl, [data], seedConnectionId, 't');
    enqueueMock.mockClear(); // creation never enqueues now, but clear defensively
  }

  it('saves the edit locally even when no `write` is configured on the template', async () => {
    const tpl = makeTemplate({ idField: 'id', write: undefined });
    await buildService(tpl, [{ id: 'A1', status: 'draft' }]);
    const rowId = await firstRowId('orders');

    const result = await service.updateAndWrite(tpl, undefined, rowId, { id: 'A1', status: 'reviewed' });

    expect(result.row.status).toBe('reviewed');
    expect(result.external).toBeUndefined();
    expect(sendMock).not.toHaveBeenCalled();
    expect(enqueueMock).not.toHaveBeenCalled();
  });

  it('saves locally, marks the row queued and enqueues an immediate targeted event send — never calling the external system inline', async () => {
    const tpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'event', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }] },
    });
    await buildService(tpl, [{ id: 'A1', status: 'draft' }], 'conn-1');
    const rowId = await firstRowId('orders');

    const result = await service.updateAndWrite(tpl, 'conn-1', rowId, { id: 'A1', status: 'reviewed' });

    expect(result.row.status).toBe('reviewed');
    expect(result.external).toEqual({ attempted: true, status: 'queued' });
    expect(sendMock).not.toHaveBeenCalled(); // no synchronous/inline call to the external system, ever

    const [state] = await submissionState('orders');
    expect(state.submission_status).toBe('queued');
    expect(state.batch_id).toBeNull();

    expect(enqueueMock).toHaveBeenCalledTimes(1);
    const [name, data, opts] = enqueueMock.mock.calls[0];
    expect(name).toBe('event');
    expect(data).toEqual({ tableKey: 'orders', rowId, connectionId: 'conn-1' });
    expect(opts).toMatchObject({ jobId: `write-event-${rowId}`, delay: 0 });
  });

  it('does not enqueue an event send for a template in schedule mode (relies solely on the internal cron)', async () => {
    const tpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'schedule', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }] },
    });
    await buildService(tpl, [{ id: 'A1', status: 'draft' }], 'conn-1');
    const rowId = await firstRowId('orders');

    const result = await service.updateAndWrite(tpl, 'conn-1', rowId, { id: 'A1', status: 'reviewed' });

    expect(result.external).toEqual({ attempted: true, status: 'queued' });
    expect(enqueueMock).not.toHaveBeenCalled();
    const [state] = await submissionState('orders');
    expect(state.submission_status).toBe('queued');
  });

  it('rejects an edit whose connection has no write.connections rule, without saving anything', async () => {
    const tpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'event', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }] },
    });
    await buildService(tpl, [{ id: 'A1', status: 'draft' }], 'conn-B');
    const rowId = await firstRowId('orders');

    await expect(service.updateAndWrite(tpl, 'conn-B', rowId, { id: 'A1', status: 'reviewed' })).rejects.toThrow(
      /not allowed to write back/,
    );
    await expect(service.updateAndWrite(tpl, undefined, rowId, { id: 'A1', status: 'reviewed' })).rejects.toThrow(
      /not allowed to write back/,
    );

    const [state] = await submissionState('orders');
    expect(state.submission_status).toBe('queued'); // set by ingest itself, untouched by the rejected edit
    expect(state.write_status).toBeNull(); // never attempted
    expect(sendMock).not.toHaveBeenCalled();
    expect(enqueueMock).not.toHaveBeenCalled();
  });

  it('throws NotFoundException for an unknown row id', async () => {
    const tpl = makeTemplate({ idField: 'id' });
    await buildService(tpl, []);
    const validButMissing = '00000000-0000-4000-8000-000000000000';
    await expect(service.updateAndWrite(tpl, undefined, validButMissing, {})).rejects.toThrow(/not found/);
  });

  it('throws BadRequestException for a malformed row id', async () => {
    const tpl = makeTemplate({ idField: 'id' });
    await buildService(tpl, []);
    await expect(service.updateAndWrite(tpl, undefined, 'not-a-uuid', {})).rejects.toThrow(/Invalid row id/);
  });
});

describe('TableRowsService — submitGroup (batch send core)', () => {
  let service: TableRowsService;
  let sendMock: jest.Mock;
  let resolveByIdMock: jest.Mock;
  const conn: ResolvedSourceConnection = {
    id: 'conn-1',
    name: 'SII',
    clave: null,
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    active: true,
  };
  const tpl = makeTemplate({
    idField: 'id',
    write: { trigger: 'event', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }] },
  });

  async function buildService(seedRows: Record<string, unknown>[] = []) {
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn();
    const fakeConnections = { resolveById: resolveByIdMock };
    const fakeClient = { send: sendMock };
    const fakeQueue = { add: jest.fn().mockResolvedValue({ id: 'job-1' }) };
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, fakeQueue as never, { record: async () => ({}) } as never);
    for (const data of seedRows) await service.ingest(tpl, [data], 'conn-1', 't');
  }

  it('does nothing and makes no HTTP call for an empty group', async () => {
    await buildService([]);
    const result = await service.submitGroup(tpl, []);
    expect(result).toBeNull();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('marks every row pending with a shared batch_id on a 2xx ack, sending {customerId, payload} with payload as an array for a multi-row group', async () => {
    await buildService([{ id: 'A1', status: 'draft' }, { id: 'B1', status: 'draft' }]);
    const rows = await allRows('orders');
    sendMock.mockResolvedValue({ status: 202, data: { received: true } });

    const result = await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(result).toEqual({ batchId: expect.any(String), status: 'sent' });
    expect(sendMock).toHaveBeenCalledWith(
      conn,
      { method: 'POST', path: '/invoices', query: undefined },
      {
        clientId: 'sii',
        payload: [
          { internal_ref: rows[0].id, ...rows[0].data },
          { internal_ref: rows[1].id, ...rows[1].data },
        ],
      },
    );

    const state = await submissionState('orders');
    expect(state).toHaveLength(2);
    for (const row of state) {
      expect(row.submission_status).toBe('pending');
      expect(row.write_status).toBe('sent');
      expect(row.batch_id).toBe(result!.batchId);
    }
  });

  it('sends payload as an array of 1 (not a bare object) for a single-row group', async () => {
    await buildService([{ id: 'A1', status: 'draft' }]);
    const rows = await allRows('orders');
    sendMock.mockResolvedValue({ status: 202, data: { received: true } });

    await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(sendMock).toHaveBeenCalledWith(
      conn,
      { method: 'POST', path: '/invoices', query: undefined },
      { clientId: 'sii', payload: [{ internal_ref: rows[0].id, ...rows[0].data }] },
    );
  });

  it('uses the connection explicit clave instead of deriving one from name, when set', async () => {
    const connWithClave = { ...conn, clave: 'acme-01' };
    resolveByIdMock = jest.fn().mockResolvedValue(connWithClave);
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    const fakeConnections = { resolveById: resolveByIdMock };
    const fakeClient = { send: sendMock };
    const fakeQueue = { add: jest.fn().mockResolvedValue({ id: 'job-1' }) };
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, fakeQueue as never, { record: async () => ({}) } as never);
    await service.ingest(tpl, [{ id: 'A1', status: 'draft' }], 'conn-1', 't');
    const rows = await allRows('orders');

    await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(sendMock).toHaveBeenCalledWith(
      connWithClave,
      { method: 'POST', path: '/invoices', query: undefined },
      { clientId: 'acme-01', payload: [{ internal_ref: rows[0].id, ...rows[0].data }] },
    );
  });

  it('transforms snake_case keys to camelCase in the payload', async () => {
    await buildService([{ id: 'A1', vat_rate: '21%', amount_tax: '100.50', owner_name: 'Test Corp' }]);
    const rows = await allRows('orders');
    sendMock.mockResolvedValue({ status: 202, data: { received: true } });

    await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(sendMock).toHaveBeenCalledTimes(1);
    const call = sendMock.mock.calls[0];
    const sentPayload = call[2] as { clientId: string; payload: unknown[] };
    const sentRow = sentPayload.payload[0] as Record<string, unknown>;
    expect(sentRow.vatRate).toBe('21%');
    expect(sentRow.amountTax).toBe('100.50');
    expect(sentRow.ownerName).toBe('Test Corp');
    expect(sentRow.vat_rate).toBeUndefined();
    expect(sentRow.amount_tax).toBeUndefined();
    expect(sentRow.owner_name).toBeUndefined();
  });

  it('reverts every row to queued with write_status=error on a non-2xx ack', async () => {
    await buildService([{ id: 'A1', status: 'draft' }, { id: 'B1', status: 'draft' }]);
    const rows = await allRows('orders');
    sendMock.mockResolvedValue({ status: 500, data: { message: 'boom' } });

    const result = await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(result?.status).toBe('error');
    const state = await submissionState('orders');
    for (const row of state) {
      expect(row.submission_status).toBe('queued');
      expect(row.write_status).toBe('error');
      expect(row.write_error).toContain('500');
      expect(row.batch_id).toBe(result!.batchId);
    }
  });

  it('reverts every row to queued with write_status=error when the transport call throws', async () => {
    await buildService([{ id: 'A1', status: 'draft' }, { id: 'B1', status: 'draft' }]);
    const rows = await allRows('orders');
    sendMock.mockRejectedValue(new Error('ECONNREFUSED'));

    const result = await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(result?.status).toBe('error');
    const state = await submissionState('orders');
    for (const row of state) {
      expect(row.submission_status).toBe('queued');
      expect(row.write_error).toContain('ECONNREFUSED');
    }
  });

  it('throws when the template has no write config', async () => {
    const noWriteTpl = makeTemplate({ idField: 'id', write: undefined });
    await buildService([{ id: 'A1', status: 'draft' }]);
    const rows = await allRows('orders');
    await expect(service.submitGroup(noWriteTpl, rows)).rejects.toThrow(/no write config/);
  });

  it('routes through opts.connectionId, per the connection its rows were ingested under', async () => {
    const perConnTpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'event', connections: [{ connectionId: 'conn-B', method: 'POST', path: '/invoices' }] },
    });
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, { send: sendMock } as never, { add: jest.fn().mockResolvedValue({ id: 'job-1' }) } as never, {
      record: async () => ({}),
    } as never);
    await service.ingest(perConnTpl, [{ id: 'A1', status: 'draft' }], 'conn-B', 't');
    const rows = await allRows('orders');

    await service.submitGroup(perConnTpl, rows, { connectionId: 'conn-B' });

    expect(resolveByIdMock).toHaveBeenCalledWith('conn-B');
  });

  it('rejects (marks error, never calls the external system) when no opts.connectionId is given — there is no fallback connection', async () => {
    const perConnTpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'event', connections: [{ connectionId: 'conn-B', method: 'POST', path: '/invoices' }] },
    });
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, { send: sendMock } as never, { add: jest.fn().mockResolvedValue({ id: 'job-1' }) } as never, {
      record: async () => ({}),
    } as never);
    await service.ingest(perConnTpl, [{ id: 'A1', status: 'draft' }], 'conn-B', 't');
    const rows = await allRows('orders');

    const result = await service.submitGroup(perConnTpl, rows);

    expect(result?.status).toBe('error');
    expect(result?.error).toMatch(/not allowed to write back/);
    expect(resolveByIdMock).not.toHaveBeenCalled();
    expect(sendMock).not.toHaveBeenCalled();
    const [state] = await submissionState('orders');
    expect(state.submission_status).toBe('queued'); // stays retryable
    expect(state.write_status).toBe('error');
  });

  it('rejects (marks error) a connectionId with no write.connections rule', async () => {
    const perConnTpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'event', connections: [{ connectionId: 'conn-B', method: 'POST', path: '/invoices' }] },
    });
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, { send: sendMock } as never, { add: jest.fn().mockResolvedValue({ id: 'job-1' }) } as never, {
      record: async () => ({}),
    } as never);
    await service.ingest(perConnTpl, [{ id: 'A1', status: 'draft' }], 'conn-C', 't');
    const rows = await allRows('orders');

    const result = await service.submitGroup(perConnTpl, rows, { connectionId: 'conn-C' });

    expect(result?.status).toBe('error');
    expect(resolveByIdMock).not.toHaveBeenCalled();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('records a reduced payload preview (first 2 rows + true total) on a successful send', async () => {
    const recordMock = jest.fn().mockResolvedValue({});
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: { ok: true } });
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, { send: sendMock } as never, { add: jest.fn() } as never, { record: recordMock } as never);
    await service.ingest(tpl, [{ id: 'A1' }, { id: 'A2' }, { id: 'A3' }], 'conn-1', 't');
    const rows = await allRows('orders');

    await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(recordMock).toHaveBeenCalledTimes(1);
    const arg = recordMock.mock.calls[0][0] as { rowCount: number; payloadPreview: { clientId: string; payload: unknown[] }; responseBody: unknown };
    expect(arg.rowCount).toBe(3);
    expect(arg.payloadPreview.payload).toHaveLength(3);
    expect(arg.payloadPreview.clientId).toBe('sii');
    expect(arg.responseBody ?? null).toBeNull(); // no body kept on a 2xx
  });

  it('records the external response body and a detailed error message on a non-2xx', async () => {
    const recordMock = jest.fn().mockResolvedValue({});
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn().mockResolvedValue({ status: 400, data: { code: 'E123', reason: 'bad NIF' } });
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, { send: sendMock } as never, { add: jest.fn() } as never, { record: recordMock } as never);
    await service.ingest(tpl, [{ id: 'A1' }], 'conn-1', 't');
    const rows = await allRows('orders');

    const result = await service.submitGroup(tpl, rows, { connectionId: 'conn-1' });

    expect(result?.status).toBe('error');
    expect(result?.error).toContain('400');
    expect(result?.error).toContain('bad NIF'); // body detail, not just the bare status
    const arg = recordMock.mock.calls[0][0] as { responseBody: unknown; errorMessage: string };
    expect(arg.responseBody).toEqual({ code: 'E123', reason: 'bad NIF' });
    expect(arg.errorMessage).toContain('bad NIF');
  });
});

async function eventJobsFor(tableKey: string): Promise<Job<WriteEventJobData>[]> {
  const jobs = await queue.getJobs(['waiting', 'delayed', 'active']);
  return jobs.filter((j) => j.data.tableKey === tableKey);
}

describe('Event send — updateAndWrite enqueue + WriteEventProcessor drain', () => {
  const conn: ResolvedSourceConnection = {
    id: 'conn-1',
    name: 'SII',
    clave: null,
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    active: true,
  };

  let service: TableRowsService;
  let sendMock: jest.Mock;
  let templatesStub: { findByKey: jest.Mock };
  let processor: WriteEventProcessor;

  const tpl = makeTemplate({
    idField: 'id',
    write: { trigger: 'event', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }] },
  });

  function build(template: TableTemplate = tpl) {
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: { received: true } });
    const fakeConnections = { resolveById: jest.fn().mockResolvedValue(conn) };
    const fakeClient = { send: sendMock };
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, queue as never, { record: async () => ({}) } as never);
    templatesStub = { findByKey: jest.fn().mockResolvedValue(template) };
    processor = new WriteEventProcessor(dataSource, templatesStub as never, service);
  }

  it('an event-mode edit enqueues a targeted job that submits exactly that row as an array of 1', async () => {
    build();
    await service.ingest(tpl, [{ id: 'A1', status: 'draft' }], 'conn-1', 't1');
    const rowId = await firstRowId('orders');

    await service.updateAndWrite(tpl, 'conn-1', rowId, { id: 'A1', status: 'reviewed' });

    const [job] = await eventJobsFor(tpl.key);
    expect(job).toBeDefined();
    expect(job.data).toEqual({ tableKey: 'orders', rowId, connectionId: 'conn-1' });

    await processor.process(job);

    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(payloadRows(sendMock.mock.calls[0]).map((r) => r.id)).toEqual(['A1']);
    expect(await submissionStatusForDataId('orders', 'A1')).toBe('pending');
  });

  it('no-ops when the targeted row is no longer queued (already sent/pending)', async () => {
    build();
    const submitGroupSpy = jest.spyOn(service, 'submitGroup');
    await service.ingest(tpl, [{ id: 'A1', status: 'draft' }], '', 't1');
    const rowId = await firstRowId('orders');
    await dataSource.query(`UPDATE table_rows SET submission_status = 'pending' WHERE id = $1`, [rowId]);

    await processor.process({ data: { tableKey: 'orders', rowId, connectionId: null } } as never);

    expect(submitGroupSpy).not.toHaveBeenCalled();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('skips without sending when the template no longer has a write config', async () => {
    build();
    const submitGroupSpy = jest.spyOn(service, 'submitGroup');
    await service.ingest(tpl, [{ id: 'A1', status: 'draft' }], '', 't1');
    const rowId = await firstRowId('orders');
    templatesStub.findByKey.mockResolvedValue({ ...tpl, write: null });

    await processor.process({ data: { tableKey: 'orders', rowId, connectionId: null } } as never);

    expect(submitGroupSpy).not.toHaveBeenCalled();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('scopes the send to the job connectionId (a job for another connection finds nothing)', async () => {
    const perConnTpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'event', connections: [{ connectionId: 'conn-A', method: 'POST', path: '/invoices' }] },
    });
    build(perConnTpl);
    await service.ingest(perConnTpl, [{ id: 'A1', status: 'draft' }], 'conn-A', 't1');
    const rowId = await rowIdForDataId('orders', 'A1');

    // Wrong connection → fetchRowsByIds scoped to conn-B matches nothing.
    await processor.process({ data: { tableKey: 'orders', rowId, connectionId: 'conn-B' } } as never);
    expect(sendMock).not.toHaveBeenCalled();

    // Right connection → sends the row as an array of 1.
    await processor.process({ data: { tableKey: 'orders', rowId, connectionId: 'conn-A' } } as never);
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(payloadRows(sendMock.mock.calls[0]).map((r) => r.id)).toEqual(['A1']);
  });
});

describe('TableWriteBatchService — schedule-mode full sweep (table.write.batchSubmit)', () => {
  const conn: ResolvedSourceConnection = {
    id: 'conn-1',
    name: 'SII',
    clave: null,
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    active: true,
  };

  const tpl = makeTemplate({
    idField: 'id',
    write: {
      trigger: 'schedule',
      connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }],
      batch: { groupBy: ['counterpartyTaxId'], maxBatchSize: 2 },
    },
  });

  let service: TableRowsService;
  let batchService: TableWriteBatchService;
  let sendMock: jest.Mock;
  let enqueueMock: jest.Mock;
  let recordRunMock: jest.Mock;
  let templatesStub: { getByKey: jest.Mock };

  function build() {
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    enqueueMock = jest.fn().mockResolvedValue({ id: 'job-1' });
    const fakeConnections = { resolveById: jest.fn().mockResolvedValue(conn) };
    const fakeClient = { send: sendMock };
    const fakeQueue = { add: enqueueMock };
    recordRunMock = jest.fn().mockResolvedValue({});
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, fakeQueue as never, {
      record: recordRunMock,
    } as never);
    templatesStub = { getByKey: jest.fn().mockResolvedValue(tpl) };
    batchService = new TableWriteBatchService(templatesStub as never, service, dataSource);
  }

  it('partitions queued rows by groupBy and chunks each partition by maxBatchSize, without leaking across groups — and never enqueues an event-mode sweep', async () => {
    build();
    // Group B123: 3 rows (maxBatchSize=2 → chunks of 2 + 1); group C456: 1 row.
    await service.ingest(
      tpl,
      [
        { id: 'A1', counterpartyTaxId: 'B123' },
        { id: 'A2', counterpartyTaxId: 'B123' },
        { id: 'A3', counterpartyTaxId: 'B123' },
        { id: 'A4', counterpartyTaxId: 'C456' },
      ],
      'conn-1',
      't1',
    );
    expect(enqueueMock).not.toHaveBeenCalled(); // trigger==='schedule' never debounce-enqueues

    await batchService.submitAllQueued(tpl);

    expect(sendMock).toHaveBeenCalledTimes(3); // 2 chunks for B123 + 1 for C456
    const idsSent = sendMock.mock.calls.map((call) => payloadRows(call).map((r) => r.id).sort());
    expect(idsSent).toContainEqual(['A1', 'A2']);
    expect(idsSent).toContainEqual(['A3']);
    expect(idsSent).toContainEqual(['A4']);

    const state = await submissionState('orders');
    expect(state).toHaveLength(4);
    for (const row of state) expect(row.submission_status).toBe('pending');

    // One write-run recorded per outbound batch, tagged with the sweep trigger.
    expect(recordRunMock).toHaveBeenCalledTimes(3);
    for (const call of recordRunMock.mock.calls) {
      expect(call[0]).toMatchObject({ tableKey: 'orders', trigger: 'schedule', status: 'sent' });
    }
  });

  it('throws via trigger() when the template has no write config, without starting a background sweep', async () => {
    build();
    templatesStub.getByKey.mockResolvedValue({ ...tpl, write: null });

    await expect(batchService.trigger('orders')).rejects.toThrow(/no write config/);
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('does nothing when no rows are queued', async () => {
    build();
    await batchService.submitAllQueued(tpl);
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('caps each sweep at write.batch.maxRecordsPerPoll rows, leaving the rest queued for the next pass', async () => {
    const cappedTpl = makeTemplate({
      idField: 'id',
      write: {
        trigger: 'schedule',
        connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }],
        // groupBy so we also prove the per-table cap applies across groups, not per group.
        batch: { groupBy: ['counterpartyTaxId'], maxRecordsPerPoll: 3 },
      },
    });
    build();
    templatesStub.getByKey.mockResolvedValue(cappedTpl);
    await service.ingest(
      cappedTpl,
      [
        { id: 'A1', counterpartyTaxId: 'B123' },
        { id: 'A2', counterpartyTaxId: 'B123' },
        { id: 'A3', counterpartyTaxId: 'C456' },
        { id: 'A4', counterpartyTaxId: 'C456' },
        { id: 'A5', counterpartyTaxId: 'C456' },
      ],
      'conn-1',
      't1',
    );

    await batchService.submitAllQueued(cappedTpl);

    // Exactly 3 rows go out this pass (total across all groups), 2 stay queued.
    const totalSent = sendMock.mock.calls.reduce((n, call) => n + payloadRows(call).length, 0);
    expect(totalSent).toBe(3);
    const state = await submissionState('orders');
    expect(state.filter((r) => r.submission_status === 'pending')).toHaveLength(3);
    expect(state.filter((r) => r.submission_status === 'queued')).toHaveLength(2);

    // Next pass drains the remaining 2.
    await batchService.submitAllQueued(cappedTpl);
    const after = await submissionState('orders');
    expect(after.filter((r) => r.submission_status === 'pending')).toHaveLength(5);
    expect(after.filter((r) => r.submission_status === 'queued')).toHaveLength(0);
  });

  it('also submits queued rows for an event-mode template — the safety-net path, not gated by write.trigger', async () => {
    const eventTpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'event', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }], batch: { groupBy: [] } },
    });
    build();
    templatesStub.getByKey.mockResolvedValue(eventTpl);
    // Bypass the debounced enqueue entirely (as if its job silently landed
    // mid-flight per the plan's same-jobId race) — the row is simply `queued`.
    await service.ingest(eventTpl, [{ id: 'A1' }], 'conn-1', 't1');

    await batchService.submitAllQueued(eventTpl);

    expect(sendMock).toHaveBeenCalledTimes(1);
    const [row] = await submissionState('orders');
    expect(row.submission_status).toBe('pending');
  });

  it('submits each connection\'s queued rows through its own connection instead of the template default', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'schedule', connections: [{ connectionId: 'conn-A', method: 'POST', path: '/invoices' }, { connectionId: 'conn-B', method: 'POST', path: '/invoices' }], batch: { groupBy: [] } },
    });
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    const fakeClient = { send: sendMock };
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, fakeClient as never, { add: jest.fn() } as never, {
      record: async () => ({}),
    } as never);
    templatesStub = { getByKey: jest.fn().mockResolvedValue(perConnTpl) };
    batchService = new TableWriteBatchService(templatesStub as never, service, dataSource);

    await service.ingest(perConnTpl, [{ id: 'A1' }], 'conn-A', 't1');
    await service.ingest(perConnTpl, [{ id: 'B1' }], 'conn-B', 't2');

    await batchService.submitAllQueued(perConnTpl);

    expect(sendMock).toHaveBeenCalledTimes(2); // one batch per connection, never merged
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-A');
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-B');
    const state = await submissionState('orders');
    for (const row of state) expect(row.submission_status).toBe('pending');
  });

  it('trigger() with a connectionId only counts and sweeps that connection\'s queued rows', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({
      idField: 'id',
      write: { trigger: 'schedule', connections: [{ connectionId: 'conn-A', method: 'POST', path: '/invoices' }, { connectionId: 'conn-B', method: 'POST', path: '/invoices' }], batch: { groupBy: [] } },
    });
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    const fakeClient = { send: sendMock };
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, fakeClient as never, { add: jest.fn() } as never, {
      record: async () => ({}),
    } as never);
    templatesStub = { getByKey: jest.fn().mockResolvedValue(perConnTpl) };
    batchService = new TableWriteBatchService(templatesStub as never, service, dataSource);

    await service.ingest(perConnTpl, [{ id: 'A1' }], 'conn-A', 't1');
    await service.ingest(perConnTpl, [{ id: 'B1' }], 'conn-B', 't2');

    const { queued } = await batchService.trigger('orders', 'schedule', 'conn-A');
    expect(queued).toBe(1); // pre-count scoped to conn-A only

    // trigger() fires the actual sweep in the background — poll briefly instead
    // of assuming a fixed number of microtask ticks completes it.
    for (let i = 0; i < 50 && sendMock.mock.calls.length === 0; i++) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-A');
    expect(resolveByIdMock).not.toHaveBeenCalledWith('conn-B');
    expect(await submissionStatusForDataId('orders', 'A1')).toBe('pending');
    expect(await submissionStatusForDataId('orders', 'B1')).toBe('queued'); // untouched — not in scope
  });
});

describe('TableWriteBatchService — submitByIds (force-submit a selection, table.write.submitRows)', () => {
  const conn: ResolvedSourceConnection = {
    id: 'conn-1',
    name: 'SII',
    clave: null,
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    active: true,
  };

  let service: TableRowsService;
  let batchService: TableWriteBatchService;
  let sendMock: jest.Mock;

  function build(tpl: TableTemplate, resolveById?: jest.Mock) {
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    const fakeConnections = { resolveById: resolveById ?? jest.fn().mockResolvedValue(conn) };
    service = new TableRowsService(dataSource, fakeConnections as never, { send: sendMock } as never, { add: jest.fn().mockResolvedValue({ id: 'job-1' }) } as never, {
      record: async () => ({}),
    } as never);
    const templatesStub = { getByKey: jest.fn().mockResolvedValue(tpl) };
    batchService = new TableWriteBatchService(templatesStub as never, service, dataSource);
  }

  it('submits only the selected queued/error rows and skips already accepted/pending ones', async () => {
    const tpl = makeTemplate({ idField: 'id', write: { trigger: 'schedule', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }], batch: { groupBy: [] } } });
    build(tpl);
    await service.ingest(tpl, [{ id: 'A1' }, { id: 'A2' }, { id: 'A3' }], 'conn-1', 't1');
    // A2 already accepted by SII provider (pending), A3 already terminal (CORRECTO) — must not be re-sent.
    await dataSource.query(`UPDATE table_rows SET submission_status = 'pending' WHERE table_key = 'orders' AND (data ->> 'id') = 'A2'`);
    await dataSource.query(`UPDATE table_rows SET submission_status = 'CORRECTO' WHERE table_key = 'orders' AND (data ->> 'id') = 'A3'`);

    const ids = await Promise.all(['A1', 'A2', 'A3'].map((d) => rowIdForDataId('orders', d)));
    const result = await batchService.submitByIds('orders', ids);

    expect(result).toEqual({ submitted: 1, skipped: 2 });
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(payloadRows(sendMock.mock.calls[0]).map((r) => r.id)).toEqual(['A1']); // only A1 sent
    expect(await submissionStatusForDataId('orders', 'A1')).toBe('pending'); // freshly sent → provider ack
    expect(await submissionStatusForDataId('orders', 'A2')).toBe('pending'); // untouched
    expect(await submissionStatusForDataId('orders', 'A3')).toBe('CORRECTO'); // untouched
  });

  it('re-sends an SII-rejected (ERROR) selected row', async () => {
    const tpl = makeTemplate({ idField: 'id', write: { trigger: 'schedule', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }], batch: { groupBy: [] } } });
    build(tpl);
    await service.ingest(tpl, [{ id: 'A1' }], 'conn-1', 't1');
    await dataSource.query(`UPDATE table_rows SET submission_status = 'ERROR' WHERE table_key = 'orders' AND (data ->> 'id') = 'A1'`);

    const result = await batchService.submitByIds('orders', [await rowIdForDataId('orders', 'A1')]);

    expect(result).toEqual({ submitted: 1, skipped: 0 });
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(await submissionStatusForDataId('orders', 'A1')).toBe('pending');
  });

  it('partitions the selection per ingestion connection (each through its own connection)', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({ idField: 'id', write: { trigger: 'schedule', connections: [{ connectionId: 'conn-A', method: 'POST', path: '/invoices' }, { connectionId: 'conn-B', method: 'POST', path: '/invoices' }], batch: { groupBy: [] } } });
    build(perConnTpl, resolveByIdMock);
    await service.ingest(perConnTpl, [{ id: 'A1' }], 'conn-A', 't1');
    await service.ingest(perConnTpl, [{ id: 'B1' }], 'conn-B', 't2');

    const ids = [await rowIdForDataId('orders', 'A1'), await rowIdForDataId('orders', 'B1')];
    const result = await batchService.submitByIds('orders', ids);

    expect(result).toEqual({ submitted: 2, skipped: 0 });
    expect(sendMock).toHaveBeenCalledTimes(2); // never merged across connections
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-A');
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-B');
  });

  it('force-submit rejects rows from a connection with no write.connections rule — no HTTP call, marked error', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const tpl = makeTemplate({
      idField: 'id',
      write: {
        trigger: 'schedule',
        connections: [{ connectionId: 'conn-A', method: 'POST', path: '/invoices' }],
        batch: { groupBy: [] },
      },
    });
    build(tpl, resolveByIdMock);
    await service.ingest(tpl, [{ id: 'A1' }], 'conn-A', 't1');
    await service.ingest(tpl, [{ id: 'B1' }], 'conn-B', 't2');

    const ids = [await rowIdForDataId('orders', 'A1'), await rowIdForDataId('orders', 'B1')];
    const result = await batchService.submitByIds('orders', ids);

    expect(result).toEqual({ submitted: 2, skipped: 0 }); // both were eligible; conn-B's just fails to send
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-A');
    expect(resolveByIdMock).not.toHaveBeenCalledWith('conn-B');

    expect(await submissionStatusForDataId('orders', 'A1')).toBe('pending');
    const [rowB] = await dataSource.query(
      `SELECT write_status, write_error, submission_status FROM table_rows WHERE table_key = 'orders' AND (data ->> 'id') = 'B1'`,
    );
    expect(rowB.write_status).toBe('error');
    expect(rowB.write_error).toMatch(/not allowed to write back/);
    expect(rowB.submission_status).toBe('queued'); // stays retryable
  });

  it('a connectionId scopes the selection (rows from other connections are skipped)', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({ idField: 'id', write: { trigger: 'schedule', connections: [{ connectionId: 'conn-A', method: 'POST', path: '/invoices' }, { connectionId: 'conn-B', method: 'POST', path: '/invoices' }], batch: { groupBy: [] } } });
    build(perConnTpl, resolveByIdMock);
    await service.ingest(perConnTpl, [{ id: 'A1' }], 'conn-A', 't1');
    await service.ingest(perConnTpl, [{ id: 'B1' }], 'conn-B', 't2');

    const ids = [await rowIdForDataId('orders', 'A1'), await rowIdForDataId('orders', 'B1')];
    const result = await batchService.submitByIds('orders', ids, 'conn-A');

    expect(result).toEqual({ submitted: 1, skipped: 1 });
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-A');
    expect(resolveByIdMock).not.toHaveBeenCalledWith('conn-B');
    expect(await submissionStatusForDataId('orders', 'B1')).toBe('queued'); // out of scope, untouched
  });

  it('respects write.batch.groupBy and maxBatchSize when partitioning the selection', async () => {
    const tpl = makeTemplate({ idField: 'id', write: { trigger: 'schedule', connections: [{ connectionId: 'conn-1', method: 'POST', path: '/invoices' }], batch: { groupBy: ['counterpartyTaxId'], maxBatchSize: 2 } } });
    build(tpl);
    await service.ingest(tpl, [
      { id: 'A1', counterpartyTaxId: 'B123' },
      { id: 'A2', counterpartyTaxId: 'B123' },
      { id: 'A3', counterpartyTaxId: 'B123' },
      { id: 'A4', counterpartyTaxId: 'C456' },
    ], 'conn-1', 't1');

    const ids = await Promise.all(['A1', 'A2', 'A3', 'A4'].map((d) => rowIdForDataId('orders', d)));
    const result = await batchService.submitByIds('orders', ids);

    expect(result).toEqual({ submitted: 4, skipped: 0 });
    expect(sendMock).toHaveBeenCalledTimes(3); // B123 → chunks of 2+1, C456 → 1
    const idsSent = sendMock.mock.calls.map((call) => payloadRows(call).length).sort();
    expect(idsSent).toEqual([1, 1, 2]);
  });

  it('throws when the template has no write config', async () => {
    const tpl = makeTemplate({ idField: 'id', write: null });
    build(tpl);
    await service.ingest(makeTemplate({ idField: 'id' }), [{ id: 'A1' }], '', 't1');
    await expect(batchService.submitByIds('orders', [await rowIdForDataId('orders', 'A1')])).rejects.toThrow(/no write config/);
  });
});

describe('SiiResultProcessor — inbound SII-result correlation by internal_ref', () => {
  async function seedRow(data: Record<string, unknown>): Promise<string> {
    const [{ id }]: { id: string }[] = await dataSource.query(
      `INSERT INTO table_rows (table_key, connection_id, data, submission_status)
       VALUES ('orders', '', $1::jsonb, 'pending') RETURNING id`,
      [JSON.stringify(data)],
    );
    return id;
  }

  function processJob(payload: unknown): Promise<void> {
    const processor = new SiiResultProcessor(dataSource);
    return processor.process({ data: { payload } } as never);
  }

  it('updates only submission_status/sii_response for the matching row — never touching data or id', async () => {
    const rowId = await seedRow({ status: 'draft', amount: 100 });

    await processJob({ state: 'ERROR', errorCode: '4114', internal_ref: rowId, timestamp: 't', siiResponse: 'boom' });

    const [row] = await dataSource.query(
      `SELECT data, submission_status, sii_response FROM table_rows WHERE id = $1`,
      [rowId],
    );
    expect(row.data).toEqual({ status: 'draft', amount: 100 });
    expect(row.submission_status).toBe('ERROR');
    expect(row.sii_response).toMatchObject({ internal_ref: rowId, state: 'ERROR' });
  });

  it('applies a batch (array) callback to every matching row in one pass', async () => {
    const idA = await seedRow({ n: 1 });
    const idB = await seedRow({ n: 2 });

    await processJob([
      { state: 'CORRECTO', internal_ref: idA },
      { state: 'ERROR', internal_ref: idB },
    ]);

    const rows: { id: string; submission_status: string }[] = await dataSource.query(
      `SELECT id, submission_status FROM table_rows WHERE table_key = 'orders' ORDER BY id`,
    );
    expect(rows).toEqual(
      expect.arrayContaining([
        { id: idA, submission_status: 'CORRECTO' },
        { id: idB, submission_status: 'ERROR' },
      ]),
    );
  });

  it('ignores an unknown internal_ref without failing the rest of the batch, warning only for the unmatched one', async () => {
    const idKnown = await seedRow({ n: 1 });
    const unknownRef = randomUUID();
    const warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);

    await expect(
      processJob([
        { state: 'ERROR', internal_ref: unknownRef },
        { state: 'CORRECTO', internal_ref: idKnown },
      ]),
    ).resolves.toBeUndefined();

    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining(unknownRef));
    warnSpy.mockRestore();

    const [row] = await dataSource.query(`SELECT submission_status FROM table_rows WHERE id = $1`, [idKnown]);
    expect(row.submission_status).toBe('CORRECTO');
  });
});
