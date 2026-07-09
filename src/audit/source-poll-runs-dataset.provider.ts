import { Injectable, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { DatasetRegistryService } from '../datasets/dataset-registry.service';
import type { DatasetDescriptor, DatasetPage, DatasetProvider, DatasetQuery } from '../datasets/dataset.types';
import { SourcePollRun } from './entities/source-poll-run.entity';

/**
 * Exposes the audit/polling run history as a dataset so it shows up in the
 * explorer next to the audited tables themselves (no FE changes).
 */
@Injectable()
export class SourcePollRunsDatasetProvider implements DatasetProvider, OnModuleInit {
  readonly descriptor: DatasetDescriptor = {
    key: 'source-poll-runs',
    label: 'Ejecuciones de auditoría',
    description: 'Historial de ejecuciones del sondeo paginado de sistemas externos',
    perConnection: false,
    columns: [
      { key: 'createdAt', label: 'Inicio', type: 'date', sortable: true },
      { key: 'tableKey', label: 'Tabla', type: 'string', filterable: true },
      { key: 'connectionName', label: 'Conexión', type: 'string' },
      { key: 'trigger', label: 'Disparador', type: 'string', filterable: true },
      { key: 'status', label: 'Estado', type: 'string', filterable: true },
      { key: 'since', label: 'Desde (watermark)', type: 'string' },
      { key: 'pages', label: 'Páginas', type: 'number' },
      { key: 'fetched', label: 'Obtenidos', type: 'number' },
      { key: 'inserted', label: 'Insertados', type: 'number' },
      { key: 'upserted', label: 'Upserted', type: 'number' },
      { key: 'errorMessage', label: 'Error', type: 'string' },
      { key: 'completedAt', label: 'Fin', type: 'date' },
    ],
    filters: [
      {
        key: 'status',
        label: 'Estado',
        type: 'string',
        options: [
          { value: 'running', label: 'running' },
          { value: 'completed', label: 'completed' },
          { value: 'empty', label: 'empty' },
          { value: 'error', label: 'error' },
        ],
      },
      {
        key: 'trigger',
        label: 'Disparador',
        type: 'string',
        options: [
          { value: 'manual', label: 'manual' },
          { value: 'scheduled', label: 'scheduled' },
        ],
      },
    ],
    defaultSort: { key: 'createdAt', dir: 'desc' },
  };

  constructor(
    @InjectRepository(SourcePollRun) private readonly repo: Repository<SourcePollRun>,
    private readonly registry: DatasetRegistryService,
  ) {}

  onModuleInit(): void {
    this.registry.register(this);
  }

  async query(params: DatasetQuery): Promise<DatasetPage> {
    const qb = this.repo.createQueryBuilder('r');
    if (params.filters?.tableKey) qb.andWhere('r.tableKey = :tableKey', { tableKey: params.filters.tableKey });
    if (params.filters?.status) qb.andWhere('r.status = :status', { status: params.filters.status });
    if (params.filters?.trigger) qb.andWhere('r.trigger = :trigger', { trigger: params.filters.trigger });

    const [rows, total] = await qb
      .orderBy('r.createdAt', 'DESC')
      .skip((params.page - 1) * params.pageSize)
      .take(params.pageSize)
      .getManyAndCount();

    return {
      rows: rows as unknown as Record<string, unknown>[],
      total,
      page: params.page,
      pageSize: params.pageSize,
    };
  }
}
