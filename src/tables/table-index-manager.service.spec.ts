import { DataSource } from 'typeorm';
import { TableIndexManagerService } from './table-index-manager.service';
import { TableRow } from './entities/table-row.entity';
import type { TableTemplate } from './entities/table-template.entity';

/**
 * Integration spec against a real Postgres instance — `CREATE INDEX
 * CONCURRENTLY` cannot run inside a transaction and expression indexes can't
 * be faithfully simulated in memory (same rationale as
 * table-rows.service.spec.ts).
 *
 * PREREQUISITE: the dev container must be up and migrated (see that spec's
 * header for the exact commands).
 */

const DB_HOST = process.env.DB_HOST ?? 'localhost';
const DB_PORT = Number(process.env.DB_PORT ?? 5434);
const DB_USER = process.env.DB_USER ?? 'yalia';
const DB_PASSWORD = process.env.DB_PASSWORD ?? 'yalia';
const DB_NAME = process.env.DB_NAME ?? 'yalia_sii';

let dataSource: DataSource;
let service: TableIndexManagerService;

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
  await dataSource.initialize();
  await dataSource.query('SELECT 1 FROM table_rows LIMIT 0');
  service = new TableIndexManagerService(dataSource);
});

afterAll(async () => {
  if (dataSource?.isInitialized) await dataSource.destroy();
});

function makeTemplate(over: Partial<TableTemplate>): TableTemplate {
  return {
    key: 'emitidas-idx-test',
    label: 'Emitidas',
    idField: '',
    columns: [
      { key: 'counterpartyTaxId', label: 'NIF', type: 'string' },
      { key: 'invoiceType', label: 'Tipo', type: 'string' },
    ],
    write: null,
    ...over,
  } as TableTemplate;
}

async function indexNamesFor(tableKey: string): Promise<string[]> {
  // Postgres normalizes the DDL to `(table_key)::text = '<key>'::text` —
  // match loosely on the quoted key rather than the original expression text.
  const rows: { indexname: string }[] = await dataSource.query(
    `SELECT indexname FROM pg_indexes WHERE tablename = 'table_rows' AND indexdef ILIKE $1`,
    [`%'${tableKey}'%`],
  );
  return rows.map((r) => r.indexname);
}

async function dropAllFor(tpl: TableTemplate) {
  await service.dropAllIndexes(tpl);
}

describe('TableIndexManagerService — write.batch.groupBy composite index', () => {
  afterEach(async () => {
    // Best-effort cleanup even if a test throws before its own dropAllIndexes.
    // DROP INDEX CONCURRENTLY can't run inside a DO block/transaction, so each
    // drop must be its own top-level statement.
    const leftover: { indexname: string }[] = await dataSource.query(
      `SELECT indexname FROM pg_indexes WHERE tablename = 'table_rows' AND indexdef ILIKE '%emitidas-idx-test%'`,
    );
    for (const { indexname } of leftover) {
      await dataSource.query(`DROP INDEX CONCURRENTLY IF EXISTS "${indexname}"`);
    }
  });

  it('creates a composite index over submission_status + groupBy columns', async () => {
    const tpl = makeTemplate({
      write: {
        connectionId: 'conn-1',
        method: 'POST',
        path: '/invoices',
        trigger: 'event',
        batch: { groupBy: ['counterpartyTaxId', 'invoiceType'] },
      },
    });

    await service.syncIndexes(null, tpl);

    const names = await indexNamesFor(tpl.key);
    expect(names.length).toBe(1);

    const [{ indexdef }] = await dataSource.query(`SELECT indexdef FROM pg_indexes WHERE indexname = $1`, [names[0]]);
    expect(indexdef).toContain('submission_status');
    expect(indexdef).toContain("data ->> 'counterpartyTaxId'");
    expect(indexdef).toContain("data ->> 'invoiceType'");

    await dropAllFor(tpl);
  });

  it('drops the old group index and creates a new one when groupBy is renamed', async () => {
    const before = makeTemplate({
      write: {
        connectionId: 'conn-1',
        method: 'POST',
        path: '/invoices',
        trigger: 'event',
        batch: { groupBy: ['counterpartyTaxId'] },
      },
    });
    await service.syncIndexes(null, before);
    const namesBefore = await indexNamesFor(before.key);
    expect(namesBefore.length).toBe(1);

    const after = makeTemplate({
      write: {
        connectionId: 'conn-1',
        method: 'POST',
        path: '/invoices',
        trigger: 'event',
        batch: { groupBy: ['invoiceType'] },
      },
    });
    await service.syncIndexes(before, after);

    const namesAfter = await indexNamesFor(after.key);
    expect(namesAfter.length).toBe(1);
    expect(namesAfter[0]).not.toBe(namesBefore[0]);

    await dropAllFor(after);
  });

  it('emits no group index when write.batch is absent', async () => {
    const tpl = makeTemplate({});
    await service.syncIndexes(null, tpl);
    const names = await indexNamesFor(tpl.key);
    expect(names.length).toBe(0);
  });
});
