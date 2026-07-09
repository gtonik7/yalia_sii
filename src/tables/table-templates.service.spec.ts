import { TableTemplatesService } from './table-templates.service';
import type { UpsertTableTemplateDto } from './dto/upsert-table-template.dto';
import type { TableTemplate } from './entities/table-template.entity';

function makeDto(over: Partial<UpsertTableTemplateDto> = {}): UpsertTableTemplateDto {
  return {
    key: 'emitidas',
    label: 'Emitidas',
    columns: [
      { key: 'id', label: 'ID', type: 'string' },
      { key: 'counterpartyTaxId', label: 'NIF contraparte', type: 'string' },
      { key: 'invoiceType', label: 'Tipo', type: 'string' },
    ],
    idField: 'id',
    ...over,
  } as UpsertTableTemplateDto;
}

describe('TableTemplatesService — validate() write.batch.groupBy', () => {
  let service: TableTemplatesService;
  let repo: { exists: jest.Mock; create: jest.Mock; save: jest.Mock; merge: jest.Mock };
  let indexes: { syncIndexes: jest.Mock };

  beforeEach(() => {
    repo = {
      exists: jest.fn().mockResolvedValue(false),
      create: jest.fn((v) => v),
      save: jest.fn((v) => v),
      merge: jest.fn((_target, v) => v),
    };
    indexes = { syncIndexes: jest.fn().mockResolvedValue(undefined) };
    service = new TableTemplatesService(repo as never, indexes as never);
  });

  it('rejects a write.batch.groupBy key that is not one of the columns', async () => {
    const dto = makeDto({
      write: {
        connectionId: 'conn-1',
        method: 'POST',
        path: '/invoices',
        trigger: 'event',
        batch: { groupBy: ['notAColumn'] },
      },
    });

    await expect(service.create(dto)).rejects.toThrow(/write.batch.groupBy "notAColumn" is not one of the columns/);
  });

  it('accepts a write.batch.groupBy made of real columns and carries trigger/batch into the saved entity', async () => {
    const dto = makeDto({
      write: {
        connectionId: 'conn-1',
        method: 'POST',
        path: '/invoices',
        trigger: 'schedule',
        batch: { groupBy: ['counterpartyTaxId', 'invoiceType'], maxBatchSize: 50 },
      },
    });

    const saved = (await service.create(dto)) as TableTemplate;

    expect(saved.write).toEqual({
      connectionId: 'conn-1',
      method: 'POST',
      path: '/invoices',
      query: undefined,
      externalRefPath: undefined,
      trigger: 'schedule',
      batch: { groupBy: ['counterpartyTaxId', 'invoiceType'], maxBatchSize: 50 },
    });
    expect(indexes.syncIndexes).toHaveBeenCalled();
  });
});
