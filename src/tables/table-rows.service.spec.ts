import { DataSource } from 'typeorm';
import { Logger } from '@nestjs/common';
import { Queue, type Job } from 'bullmq';
import { TableRowsService } from './table-rows.service';
import { WriteSweepProcessor } from './write-sweep.processor';
import { TableWriteBatchService } from './table-write-batch.service';
import { AeatResultProcessor } from '../callbacks/aeat-result.processor';
import { TableRow } from './entities/table-row.entity';
import { TableTemplate } from './entities/table-template.entity';
import { DatasetQuery } from '../datasets/dataset.types';
import type { ResolvedSourceConnection } from '../connections/source-connections.service';
import type { WriteSweepJobData } from './write-sweep.types';
import { QUEUES } from '../core/queues/queues.constants';

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
// Real BullMQ queue against dev Redis — the jobId-dedup debounce behavior the
// event-mode write pipeline leans on isn't something an in-memory fake queue
// can faithfully reproduce (see the "write-sweep" describe block below).
// Shares this one file/worker with the rest of these specs (rather than a
// standalone spec file) specifically so its TRUNCATE table_rows can't race
// against another file's in a separate Jest worker — Postgres deadlocks (or
// silently-wrong row counts) resulted from splitting this out before.
let queue: Queue<WriteSweepJobData>;

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

  queue = new Queue<WriteSweepJobData>(QUEUES.WRITE_SWEEP, {
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
    perConnection: false,
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

  it('scopes rows to the connectionId on ingest and returns them only for that connection (perConnection=true)', async () => {
    const tpl = makeTemplate({ perConnection: true, idField: 'id' });

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
    const tpl = makeTemplate({ perConnection: true, idField: 'id' });

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
      `UPDATE table_rows SET write_status='sent', write_error=NULL, last_written_at=now(), external_ref='ext-1', submission_status='pending', aeat_response='{"state":"PENDING"}'::jsonb WHERE id = $1`,
      [rowId],
    );

    const res = await service.query(tpl, query({}));

    expect(res.rows[0]).toMatchObject({
      _writeStatus: 'sent',
      _externalRef: 'ext-1',
      _submissionStatus: 'pending',
      _aeatResponse: { state: 'PENDING' },
    });
    expect(res.rows[0]._lastWrittenAt).toBeInstanceOf(Date);
  });
});

describe('TableRowsService — query honors only declared filterable/sortable columns', () => {
  let service: TableRowsService;
  const tpl = makeTemplate({
    perConnection: false,
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

  it('free-text search matches across string fields', async () => {
    const res = await service.query(tpl, query({ search: 'open' }));
    expect(res.total).toBe(1);
    expect(res.rows[0].id).toBe('B');
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
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    pagination: { type: 'none', recordsPath: '' },
    active: true,
  };

  async function buildService(tpl: TableTemplate, seedRows: Record<string, unknown>[] = []) {
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn();
    enqueueMock = jest.fn().mockResolvedValue({ id: 'job-1' });
    const fakeConnections = { resolveById: resolveByIdMock };
    const fakeClient = { send: sendMock };
    const fakeQueue = { add: enqueueMock };
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, fakeQueue as never, { record: async () => ({}) } as never);
    for (const data of seedRows) await service.ingest(tpl, [data], '', 't');
    enqueueMock.mockClear(); // ignore sweep jobs enqueued by the seeding ingest() calls above
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

  it('saves locally, marks the row queued and enqueues a debounced sweep — never calling the external system inline', async () => {
    const tpl = makeTemplate({
      idField: 'id',
      write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'event' },
    });
    await buildService(tpl, [{ id: 'A1', status: 'draft' }]);
    const rowId = await firstRowId('orders');

    const result = await service.updateAndWrite(tpl, undefined, rowId, { id: 'A1', status: 'reviewed' });

    expect(result.row.status).toBe('reviewed');
    expect(result.external).toEqual({ attempted: true, status: 'queued' });
    expect(sendMock).not.toHaveBeenCalled(); // no synchronous/inline call to the external system, ever

    const [state] = await submissionState('orders');
    expect(state.submission_status).toBe('queued');
    expect(state.batch_id).toBeNull();

    expect(enqueueMock).toHaveBeenCalledTimes(1);
    const [, , opts] = enqueueMock.mock.calls[0];
    expect(opts).toMatchObject({ jobId: expect.stringContaining('write-sweep:orders:'), delay: expect.any(Number) });
  });

  it('does not enqueue a sweep for a template in schedule mode (relies solely on the hub-triggered sweep)', async () => {
    const tpl = makeTemplate({
      idField: 'id',
      write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule' },
    });
    await buildService(tpl, [{ id: 'A1', status: 'draft' }]);
    const rowId = await firstRowId('orders');

    const result = await service.updateAndWrite(tpl, undefined, rowId, { id: 'A1', status: 'reviewed' });

    expect(result.external).toEqual({ attempted: true, status: 'queued' });
    expect(enqueueMock).not.toHaveBeenCalled();
    const [state] = await submissionState('orders');
    expect(state.submission_status).toBe('queued');
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
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    pagination: { type: 'none', recordsPath: '' },
    active: true,
  };
  const tpl = makeTemplate({
    idField: 'id',
    write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'event' },
  });

  async function buildService(seedRows: Record<string, unknown>[] = []) {
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn();
    const fakeConnections = { resolveById: resolveByIdMock };
    const fakeClient = { send: sendMock };
    const fakeQueue = { add: jest.fn().mockResolvedValue({ id: 'job-1' }) };
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, fakeQueue as never, { record: async () => ({}) } as never);
    for (const data of seedRows) await service.ingest(tpl, [data], '', 't');
  }

  it('does nothing and makes no HTTP call for an empty group', async () => {
    await buildService([]);
    const result = await service.submitGroup(tpl, []);
    expect(result).toBeNull();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('marks every row pending with a shared batch_id on a 2xx ack, sending the whole group as one array body', async () => {
    await buildService([{ id: 'A1', status: 'draft' }, { id: 'B1', status: 'draft' }]);
    const rows = await allRows('orders');
    sendMock.mockResolvedValue({ status: 202, data: { received: true } });

    const result = await service.submitGroup(tpl, rows);

    expect(result).toEqual({ batchId: expect.any(String), status: 'sent' });
    expect(sendMock).toHaveBeenCalledWith(
      conn,
      { method: 'POST', path: '/invoices', query: undefined },
      [rows[0].data, rows[1].data],
    );

    const state = await submissionState('orders');
    expect(state).toHaveLength(2);
    for (const row of state) {
      expect(row.submission_status).toBe('pending');
      expect(row.write_status).toBe('sent');
      expect(row.batch_id).toBe(result!.batchId);
    }
  });

  it('reverts every row to queued with write_status=error on a non-2xx ack', async () => {
    await buildService([{ id: 'A1', status: 'draft' }, { id: 'B1', status: 'draft' }]);
    const rows = await allRows('orders');
    sendMock.mockResolvedValue({ status: 500, data: { message: 'boom' } });

    const result = await service.submitGroup(tpl, rows);

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

    const result = await service.submitGroup(tpl, rows);

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

  it('routes through opts.connectionId instead of the template default when the table is perConnection', async () => {
    const perConnTpl = makeTemplate({
      perConnection: true,
      idField: 'id',
      write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'event' },
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

  it('falls back to the template default connection when perConnection but no opts.connectionId is given', async () => {
    const perConnTpl = makeTemplate({
      perConnection: true,
      idField: 'id',
      write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'event' },
    });
    resolveByIdMock = jest.fn().mockResolvedValue(conn);
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: {} });
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, { send: sendMock } as never, { add: jest.fn().mockResolvedValue({ id: 'job-1' }) } as never, {
      record: async () => ({}),
    } as never);
    await service.ingest(perConnTpl, [{ id: 'A1', status: 'draft' }], 'conn-B', 't');
    const rows = await allRows('orders');

    await service.submitGroup(perConnTpl, rows);

    expect(resolveByIdMock).toHaveBeenCalledWith('conn-1');
  });
});

async function delayedJobsFor(tableKey: string): Promise<Job<WriteSweepJobData>[]> {
  const delayed = await queue.getJobs(['delayed']);
  return delayed.filter((j) => j.data.tableKey === tableKey);
}

describe('Event-mode write pipeline — debounced enqueue + WriteSweepProcessor re-query', () => {
  const conn: ResolvedSourceConnection = {
    id: 'conn-1',
    name: 'SII',
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    pagination: { type: 'none', recordsPath: '' },
    active: true,
  };

  let service: TableRowsService;
  let sendMock: jest.Mock;
  let templatesStub: { findByKey: jest.Mock };
  let processor: WriteSweepProcessor;

  const tpl = makeTemplate({
    idField: 'id',
    write: {
      connectionId: 'conn-1',
      method: 'POST',
      path: '/invoices',
      trigger: 'event',
      batch: { groupBy: ['counterpartyTaxId'] },
    },
  });

  function build() {
    sendMock = jest.fn().mockResolvedValue({ status: 202, data: { received: true } });
    const fakeConnections = { resolveById: jest.fn().mockResolvedValue(conn) };
    const fakeClient = { send: sendMock };
    service = new TableRowsService(dataSource, fakeConnections as never, fakeClient as never, queue as never, { record: async () => ({}) } as never);
    templatesStub = { findByKey: jest.fn().mockResolvedValue(tpl) };
    processor = new WriteSweepProcessor(dataSource, templatesStub as never, service);
  }

  beforeEach(() => {
    process.env.WRITE_SWEEP_DEBOUNCE_MS = '300';
  });

  afterEach(() => {
    delete process.env.WRITE_SWEEP_DEBOUNCE_MS;
  });

  it('collapses two quick edits of the same group into a single delayed sweep job', async () => {
    build();

    await service.ingest(tpl, [{ id: 'A1', counterpartyTaxId: 'B123' }], '', 't1');
    await service.ingest(tpl, [{ id: 'A2', counterpartyTaxId: 'B123' }], '', 't2');

    expect(await delayedJobsFor(tpl.key)).toHaveLength(1);
  });

  it('re-queries submission_status=queued at execution time, so a third edit landing during the debounce window (which BullMQ silently drops from the payload) is still submitted', async () => {
    build();
    const submitGroupSpy = jest.spyOn(service, 'submitGroup');

    await service.ingest(tpl, [{ id: 'A1', counterpartyTaxId: 'B123' }], '', 't1');
    await service.ingest(tpl, [{ id: 'A2', counterpartyTaxId: 'B123' }], '', 't2');
    // Same group → same jobId → BullMQ dedups this third enqueue too, exactly
    // like it did for A2's — the job's stored payload never learns about A3.
    await service.ingest(tpl, [{ id: 'A3', counterpartyTaxId: 'B123' }], '', 't3');

    const [job] = await delayedJobsFor(tpl.key);
    expect(job).toBeDefined();

    await processor.process(job);

    expect(submitGroupSpy).toHaveBeenCalledTimes(1);
    const rowsArg = submitGroupSpy.mock.calls[0][1];
    expect(rowsArg.map((r) => (r.data as { id: string }).id).sort()).toEqual(['A1', 'A2', 'A3']);
    expect(sendMock).toHaveBeenCalledTimes(1);
  });

  it('does nothing when nothing is queued for the group anymore (e.g. already submitted by an earlier sweep)', async () => {
    build();
    const submitGroupSpy = jest.spyOn(service, 'submitGroup');

    await service.ingest(tpl, [{ id: 'A1', counterpartyTaxId: 'B123' }], '', 't1');
    const [job] = await delayedJobsFor(tpl.key);
    await dataSource.query(`UPDATE table_rows SET submission_status = 'pending' WHERE table_key = $1`, [tpl.key]);

    await processor.process(job);

    expect(submitGroupSpy).not.toHaveBeenCalled();
    expect(sendMock).not.toHaveBeenCalled();
  });

  it('skips without touching rows when the template no longer has a write config', async () => {
    build();
    const submitGroupSpy = jest.spyOn(service, 'submitGroup');

    await service.ingest(tpl, [{ id: 'A1', counterpartyTaxId: 'B123' }], '', 't1');
    const [job] = await delayedJobsFor(tpl.key);
    templatesStub.findByKey.mockResolvedValue({ ...tpl, write: null });

    await processor.process(job);

    expect(submitGroupSpy).not.toHaveBeenCalled();
  });

  it('never mixes two connections into one debounce job on a perConnection table, and submits each through its own connection', async () => {
    const perConnTpl = makeTemplate({
      perConnection: true,
      idField: 'id',
      write: {
        connectionId: 'conn-1',
        method: 'POST',
        path: '/invoices',
        trigger: 'event',
        batch: { groupBy: ['counterpartyTaxId'] },
      },
    });
    build();
    templatesStub.findByKey.mockResolvedValue(perConnTpl);
    const resolveByIdMock = jest.fn().mockResolvedValue(conn);
    service = new TableRowsService(dataSource, { resolveById: resolveByIdMock } as never, { send: sendMock } as never, queue as never, {
      record: async () => ({}),
    } as never);
    processor = new WriteSweepProcessor(dataSource, templatesStub as never, service);

    // Same groupBy value ('B123'), two different source connections.
    await service.ingest(perConnTpl, [{ id: 'A1', counterpartyTaxId: 'B123' }], 'conn-A', 't1');
    await service.ingest(perConnTpl, [{ id: 'A2', counterpartyTaxId: 'B123' }], 'conn-B', 't2');

    const jobs = await delayedJobsFor(perConnTpl.key);
    expect(jobs).toHaveLength(2); // not collapsed into one job despite identical groupValues

    for (const job of jobs) await processor.process(job);

    expect(resolveByIdMock).toHaveBeenCalledWith('conn-A');
    expect(resolveByIdMock).toHaveBeenCalledWith('conn-B');
    expect(sendMock).toHaveBeenCalledTimes(2);
    const sentIds = sendMock.mock.calls.map((call) => (call[2] as { id: string }[]).map((r) => r.id));
    expect(sentIds).toContainEqual(['A1']);
    expect(sentIds).toContainEqual(['A2']);
  });
});

describe('TableWriteBatchService — schedule-mode full sweep (table.write.batchSubmit)', () => {
  const conn: ResolvedSourceConnection = {
    id: 'conn-1',
    name: 'SII',
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    pagination: { type: 'none', recordsPath: '' },
    active: true,
  };

  const tpl = makeTemplate({
    idField: 'id',
    write: {
      connectionId: 'conn-1',
      method: 'POST',
      path: '/invoices',
      trigger: 'schedule',
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
      '',
      't1',
    );
    expect(enqueueMock).not.toHaveBeenCalled(); // trigger==='schedule' never debounce-enqueues

    await batchService.submitAllQueued(tpl);

    expect(sendMock).toHaveBeenCalledTimes(3); // 2 chunks for B123 + 1 for C456
    const idsSent = sendMock.mock.calls.map((call) => (call[2] as { id: string }[]).map((r) => r.id).sort());
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

  it('also submits queued rows for an event-mode template — the safety-net path, not gated by write.trigger', async () => {
    const eventTpl = makeTemplate({
      idField: 'id',
      write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'event', batch: { groupBy: [] } },
    });
    build();
    templatesStub.getByKey.mockResolvedValue(eventTpl);
    // Bypass the debounced enqueue entirely (as if its job silently landed
    // mid-flight per the plan's same-jobId race) — the row is simply `queued`.
    await service.ingest(eventTpl, [{ id: 'A1' }], '', 't1');

    await batchService.submitAllQueued(eventTpl);

    expect(sendMock).toHaveBeenCalledTimes(1);
    const [row] = await submissionState('orders');
    expect(row.submission_status).toBe('pending');
  });

  it('on a perConnection table, submits each connection\'s queued rows through its own connection instead of the template default', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({
      perConnection: true,
      idField: 'id',
      write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule', batch: { groupBy: [] } },
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

  it('trigger() with a connectionId on a perConnection table only counts and sweeps that connection\'s queued rows', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({
      perConnection: true,
      idField: 'id',
      write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule', batch: { groupBy: [] } },
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
    baseUrl: 'https://sii.test',
    authType: 'bearer',
    credentials: { token: 't' },
    defaultHeaders: {},
    pagination: { type: 'none', recordsPath: '' },
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
    const tpl = makeTemplate({ idField: 'id', write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule', batch: { groupBy: [] } } });
    build(tpl);
    await service.ingest(tpl, [{ id: 'A1' }, { id: 'A2' }, { id: 'A3' }], '', 't1');
    // A2 already accepted by AEAT provider (pending), A3 already terminal (CORRECTO) — must not be re-sent.
    await dataSource.query(`UPDATE table_rows SET submission_status = 'pending' WHERE table_key = 'orders' AND (data ->> 'id') = 'A2'`);
    await dataSource.query(`UPDATE table_rows SET submission_status = 'CORRECTO' WHERE table_key = 'orders' AND (data ->> 'id') = 'A3'`);

    const ids = await Promise.all(['A1', 'A2', 'A3'].map((d) => rowIdForDataId('orders', d)));
    const result = await batchService.submitByIds('orders', ids);

    expect(result).toEqual({ submitted: 1, skipped: 2 });
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect((sendMock.mock.calls[0][2] as { id: string }[]).map((r) => r.id)).toEqual(['A1']); // only A1 sent
    expect(await submissionStatusForDataId('orders', 'A1')).toBe('pending'); // freshly sent → provider ack
    expect(await submissionStatusForDataId('orders', 'A2')).toBe('pending'); // untouched
    expect(await submissionStatusForDataId('orders', 'A3')).toBe('CORRECTO'); // untouched
  });

  it('re-sends an AEAT-rejected (ERROR) selected row', async () => {
    const tpl = makeTemplate({ idField: 'id', write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule', batch: { groupBy: [] } } });
    build(tpl);
    await service.ingest(tpl, [{ id: 'A1' }], '', 't1');
    await dataSource.query(`UPDATE table_rows SET submission_status = 'ERROR' WHERE table_key = 'orders' AND (data ->> 'id') = 'A1'`);

    const result = await batchService.submitByIds('orders', [await rowIdForDataId('orders', 'A1')]);

    expect(result).toEqual({ submitted: 1, skipped: 0 });
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(await submissionStatusForDataId('orders', 'A1')).toBe('pending');
  });

  it('on a perConnection table, partitions the selection per ingestion connection (each through its own connection)', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({ perConnection: true, idField: 'id', write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule', batch: { groupBy: [] } } });
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

  it('on a perConnection table, a connectionId scopes the selection (rows from other connections are skipped)', async () => {
    const resolveByIdMock = jest.fn((id: string) => Promise.resolve({ ...conn, id }));
    const perConnTpl = makeTemplate({ perConnection: true, idField: 'id', write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule', batch: { groupBy: [] } } });
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
    const tpl = makeTemplate({ idField: 'id', write: { connectionId: 'conn-1', method: 'POST', path: '/invoices', trigger: 'schedule', batch: { groupBy: ['counterpartyTaxId'], maxBatchSize: 2 } } });
    build(tpl);
    await service.ingest(tpl, [
      { id: 'A1', counterpartyTaxId: 'B123' },
      { id: 'A2', counterpartyTaxId: 'B123' },
      { id: 'A3', counterpartyTaxId: 'B123' },
      { id: 'A4', counterpartyTaxId: 'C456' },
    ], '', 't1');

    const ids = await Promise.all(['A1', 'A2', 'A3', 'A4'].map((d) => rowIdForDataId('orders', d)));
    const result = await batchService.submitByIds('orders', ids);

    expect(result).toEqual({ submitted: 4, skipped: 0 });
    expect(sendMock).toHaveBeenCalledTimes(3); // B123 → chunks of 2+1, C456 → 1
    const idsSent = sendMock.mock.calls.map((call) => (call[2] as { id: string }[]).length).sort();
    expect(idsSent).toEqual([1, 1, 2]);
  });

  it('throws when the template has no write config', async () => {
    const tpl = makeTemplate({ idField: 'id', write: null });
    build(tpl);
    await service.ingest(makeTemplate({ idField: 'id' }), [{ id: 'A1' }], '', 't1');
    await expect(batchService.submitByIds('orders', [await rowIdForDataId('orders', 'A1')])).rejects.toThrow(/no write config/);
  });
});

describe('AeatResultProcessor — inbound AEAT-result correlation by external_ref', () => {
  async function seedRowWithExternalRef(externalRef: string, data: Record<string, unknown>): Promise<string> {
    const [{ id }]: { id: string }[] = await dataSource.query(
      `INSERT INTO table_rows (table_key, connection_id, data, external_ref, submission_status)
       VALUES ('orders', '', $1::jsonb, $2, 'pending') RETURNING id`,
      [JSON.stringify(data), externalRef],
    );
    return id;
  }

  function processJob(payload: unknown): Promise<void> {
    const processor = new AeatResultProcessor(dataSource);
    return processor.process({ data: { payload } } as never);
  }

  it('updates only submission_status/aeat_response for the matching row — never touching data or external_ref', async () => {
    const rowId = await seedRowWithExternalRef('inv-1', { status: 'draft', amount: 100 });

    await processJob({ state: 'ERROR', errorCode: '4114', invoiceId: 'inv-1', timestamp: 't', aeatResponse: 'boom' });

    const [row] = await dataSource.query(
      `SELECT data, submission_status, aeat_response, external_ref FROM table_rows WHERE id = $1`,
      [rowId],
    );
    expect(row.data).toEqual({ status: 'draft', amount: 100 });
    expect(row.submission_status).toBe('ERROR');
    expect(row.aeat_response).toMatchObject({ invoiceId: 'inv-1', state: 'ERROR' });
    expect(row.external_ref).toBe('inv-1');
  });

  it('applies a batch (array) callback to every matching row in one pass', async () => {
    await seedRowWithExternalRef('inv-a', { n: 1 });
    await seedRowWithExternalRef('inv-b', { n: 2 });

    await processJob([
      { state: 'CORRECTO', invoiceId: 'inv-a' },
      { state: 'ERROR', invoiceId: 'inv-b' },
    ]);

    const rows: { external_ref: string; submission_status: string }[] = await dataSource.query(
      `SELECT external_ref, submission_status FROM table_rows WHERE table_key = 'orders' ORDER BY external_ref`,
    );
    expect(rows).toEqual([
      { external_ref: 'inv-a', submission_status: 'CORRECTO' },
      { external_ref: 'inv-b', submission_status: 'ERROR' },
    ]);
  });

  it('ignores an unknown external_ref without failing the rest of the batch, warning only for the unmatched one', async () => {
    await seedRowWithExternalRef('inv-known', { n: 1 });
    const warnSpy = jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);

    await expect(
      processJob([
        { state: 'ERROR', invoiceId: 'inv-unknown' },
        { state: 'CORRECTO', invoiceId: 'inv-known' },
      ]),
    ).resolves.toBeUndefined();

    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('inv-unknown'));
    warnSpy.mockRestore();

    const [row] = await dataSource.query(`SELECT submission_status FROM table_rows WHERE external_ref = 'inv-known'`);
    expect(row.submission_status).toBe('CORRECTO');
  });
});
